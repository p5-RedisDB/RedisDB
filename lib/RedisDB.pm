package RedisDB;

use warnings;
use strict;
our $VERSION = "0.31";
$VERSION = eval $VERSION;

use RedisDB::Error;
use IO::Socket::INET;
use IO::Socket::UNIX;
use Socket qw(MSG_DONTWAIT SO_RCVTIMEO SO_SNDTIMEO);
use POSIX qw(:errno_h);
use Config;
use Carp;
use Try::Tiny;

=head1 NAME

RedisDB - Perl extension to access redis database

=head1 SYNOPSIS

    use RedisDB;

    my $redis = RedisDB->new(host => 'localhost', port => 6379);
    $redis->set($key, $value);
    my $value = $redis->get($key);

=head1 DESCRIPTION

This module provides interface to access redis key-value store. It
transparently handles disconnects and forks. It supports transactions,
pipelining, and subscription mode.

=head1 METHODS

=cut

=head2 $class->new(%options)

Creates the new RedisDB object. The following options are accepted:

=over 4

=item host

domain name of the host running redis server. Default: "localhost"

=item port

port to connect. Default: 6379

=item path

you can connect to redis using UNIX socket. In this case instead of
L</host> and L</port> you should specify I<path>.

=item database

DB number to use. Specified database will be selected immediately after
connecting to the server. Database changes when you sending I<select> command
to the server. You can get current database using I<selected_database> method.
Default value is 0.

=item timeout

IO timeout. With this option set, if IO operation has taken more than specified
number of seconds, module will croak. Note, that some OSes do not support
SO_RCVTIMEO, and SO_SNDTIMEO socket options, in this case timeout will not
work.

=item lazy

by default I<new> establishes connection to the server. If this parameter is
set, then connection will be established when you will send a command to the
server.

=back

=cut

sub new {
    my $class = shift;
    my $self = ref $_[0] ? $_[0] : {@_};
    bless $self, $class;
    if ( $self->{path} and ( $self->{host} or $self->{port} ) ) {
        croak "You can't specify \"path\" together with \"host\" and \"port\"";
    }
    $self->{port} ||= 6379;
    $self->{host} ||= 'localhost';
    $self->{_replies}       = [];
    $self->{_callbacks}     = [];
    $self->{_to_be_fetched} = 0;
    $self->{database} ||= 0;
    $self->_connect unless $self->{lazy};
    return $self;
}

=head2 $self->execute($command, @arguments)

send a command to the server and return the result. It will throw the exception
if the server returns an error. It may be more convenient to use instead of
this method wrapper named after the corresponding redis command. E.g.:

    $redis->execute('set', key => 'value');
    # is the same as
    $redis->set(key => 'value');

See L</"WRAPPER METHODS"> section for the full list of defined aliases.

Note, that you can't use I<execute> if you have sent some commands using
I<send_command> method without callback argument and have not yet got all
replies.

=cut

sub execute {
    my $self = shift;
    croak "You can't use RedisDB::execute while in pipelining mode."
      if $self->replies_to_fetch;
    croak "This function is not available in subscription mode." if $self->{_subscription_loop};
    my $cmd = uc shift;
    $self->send_command( $cmd, @_ );
    return $self->get_reply;
}

# establish connection to the server.

sub _connect {
    my $self = shift;

    # this is to prevent recursion
    croak "Couldn't connect to the redis-server."
      . " Connection was immediately closed by the server."
      if $self->{_in_connect}++;

    $self->{_pid} = $$;

    if ( $self->{path} ) {
        $self->{_socket} = IO::Socket::UNIX->new(
            Type => SOCK_STREAM,
            Peer => $self->{path},
        ) or croak "Can't connect to redis server socket at `$self->{path}': $!";
    }
    else {
        $self->{_socket} = IO::Socket::INET->new(
            PeerAddr => $self->{host},
            PeerPort => $self->{port},
            Proto    => 'tcp',
            ( $self->{timeout} ? ( Timeout => $self->{timeout} ) : () ),
        ) or croak "Can't connect to redis server $self->{host}:$self->{port}: $!";
    }

    if ( $self->{timeout} ) {
        my $timeout =
          $Config{longsize} == 4
          ? pack( 'LL', $self->{timeout}, 0 )
          : pack( 'QQ', $self->{timeout} );
        try {
            defined $self->{_socket}->sockopt( SO_RCVTIMEO, $timeout )
              or die "Can't set timeout: $!";
            defined $self->{_socket}->sockopt( SO_SNDTIMEO, $timeout )
              or die "Can't set send timeout: $!";
        }
        catch {
            warn "$_\n";
        };
    }

    $self->{_callbacks}         = [];
    $self->{_subscription_loop} = 0;
    if ( $self->{database} ) {
        $self->send_command( "SELECT", $self->{database}, IGNORE_REPLY() );
    }

    delete $self->{_in_connect};
    return 1;
}

my $SET_NB   = 0;
my $DONTWAIT = 0;

# Windows don't have MSG_DONTWAIT, so we need to switch socket into non-blocking mode
if ( $^O eq 'MSWin32' ) {
    $SET_NB = 1;
}
else {
    $DONTWAIT = MSG_DONTWAIT;
}

# parse data from the receive buffer without blocking
sub _recv_data_nb {
    my $self = shift;

    $self->{_socket}->blocking(0) if $SET_NB;

    while (1) {
        my $ret = recv( $self->{_socket}, my $buf, 4096, $DONTWAIT );
        unless ( defined $ret ) {

            # socket is connected, no data in recv buffer
            last if $! == EAGAIN or $! == EWOULDBLOCK;
            next if $! == EINTR;

            # die on any other error
            die "Error reading from server: $!";
        }
        elsif ( $buf ne '' ) {

            # received some data
            $self->{_buffer} .= $buf;
            1 while $self->{_buffer} and $self->_parse_reply;
        }
        else {

            # server closed connection. Check if some data was lost.
            1 while $self->{_buffer} and $self->_parse_reply;

            # if there's some replies lost
            die "Server closed connection. Some data was lost."
              if @{ $self->{_callbacks} }
                  or $self->{_in_multi};

            # clean disconnect, try to reconnect
            $self->{warnings} and warn "Disconnected, trying to reconnect";
            $self->_connect;
            last;
        }
    }

    $self->{_socket}->blocking(1) if $SET_NB;

    return;
}

sub _queue {
    my ( $self, $reply ) = @_;
    --$self->{_to_be_fetched};
    push @{ $self->{_replies} }, $reply;
}

=head2 $self->send_command($command[, @arguments][, \&callback])

send a command to the server. Returns true if the command was successfully
sent, or dies if an error occured. Note, that it does not return reply from
the server, you should retrieve it using the I<get_reply> method, or if I<callback>
has been specified, it will be invoked upon receiving the reply with two
arguments: the RedisDB object, and the reply from the server.  If the server
returns an error, the second argument to the callback will be a L<RedisDB::Error>
object, you can get description of the error using this object in string
context.  If you are not interested in reply, you can use RedisDB::IGNORE_REPLY
constant as the last argument.

Note, that RedisDB does not run any background threads, so it will not receive
the reply and invoke the callback unless you call some of its methods which
check if there are replies from the server, like I<send_command>,
I<reply_ready>, I<get_reply>, or I<get_all_replies>.

=cut

sub send_command {
    my $self = shift;

    my $callback;
    if ( ref $_[-1] eq 'CODE' ) {
        $callback = pop;
    }
    else {
        ++$self->{_to_be_fetched};
        $callback = \&_queue;
    }

    my $command = uc shift;
    if ( $self->{_subscription_loop} ) {
        croak "only (UN)(P)SUBSCRIBE and QUIT allowed in subscription loop"
          unless $command =~ /^(P?(UN)?SUBSCRIBE|QUIT)$/;
    }

    # if SELECT has been successful, we should update database
    if ( $command eq 'SELECT' ) {
        my $cb    = $callback;
        my $dbnum = $_[0];
        $callback = sub {
            $_[0]->{database} = $dbnum unless ref $_[1];
            $cb->(@_);
        };
    }

    my $request = _build_redis_request( $command, @_ );
    $self->_connect unless $self->{_socket} and $self->{_pid} == $$;

    # Here we reading received data and storing it in the _buffer,
    # but the main purpose is to check if connection is still alive
    # and reconnect if not
    $self->_recv_data_nb;

    defined $self->{_socket}->send($request) or die "Can't send request to server: $!";
    push @{ $self->{_callbacks} }, $callback;
    return 1;
}

sub _ignore {
    my ( $self, $res ) = @_;
    if ( ref $res eq 'RedisDB::Error' ) {
        warn "Ignoring error returned by redis-server: $res";
    }
}

sub IGNORE_REPLY { return \&_ignore; }

=head2 $self->send_command_cb($command[, @arguments][, \&callback])

send a command to the server, invoke specified I<callback> on reply. The
callback is invoked with two arguments: the RedisDB object, and reply from the
server.  If the server returned an error, the second argument will be a
L<RedisDB::Error> object, you can get description of the error using this
object in string context.  If the I<callback> is not specified, the reply will
be discarded.  Note, that RedisDB does not run any background threads, so it
will not receive the reply and invoke the callback unless you call some of its
methods which check if there are replies from the server, like I<send_command>,
I<send_command_cb>, I<reply_ready>, I<get_reply>, or I<get_all_replies>.

B<DEPRECATED:> this method is deprecated and may be removed in some future
version. Please use I<send_command> method instead. If you are using
I<send_command_cb> with I<&callback> argument, you can just replace the method
with I<send_command> and it will do the same. If you are using
I<send_command_cb> with the default callback, you should add the
RedisDB::IGNORE_REPLY constant as the last argument when replacing the method
with I<send_command>.  Here is the example that shows equivalents with
I<send_command>:

    $redis->send_command_cb("SET", "Key", "Value");
    # may be replaced with
    $redis->send_command("SET", "Key", "Value", RedisDB::IGNORE_REPLY);

    $redis->send_command_cb("GET", "Key", \&process_reply);
    # may be replaced with
    $redis->send_command("GET", "Key", \&process_reply);

=cut

sub send_command_cb {
    my $self = shift;
    my $callback = pop if ref $_[-1] eq 'CODE';
    $callback ||= \&_ignore;
    return $self->send_command( @_, $callback );
}

=head2 $self->reply_ready

This method may be used in the pipelining mode to check if there are some
replies already received from the server. Returns the number of replies
available for reading.

=cut

sub reply_ready {
    my $self = shift;

    $self->_recv_data_nb;
    return @{ $self->{_replies} } ? 1 : 0;
}

=head2 $self->mainloop

this method blocks till all replies from the server will be received

=cut

sub mainloop {
    my $self = shift;

    while ( @{ $self->{_callbacks} } ) {
        die "You can't call mainloop in the child process" unless $self->{_pid} == $$;
        my $ret = $self->{_socket}->recv( my $buffer, 4096 );
        unless ( defined $ret ) {
            next if $! == EINTR;
            croak "Error reading reply from server: $!";
        }
        if ( $buffer ne '' ) {

            # received some data
            $self->{_buffer} .= $buffer;
            1 while $self->_parse_reply;
        }
        else {

            # disconnected
            die "Server unexpectedly closed connection before sending full reply";
        }
    }
    return;
}

=head2 $self->get_reply

receive reply from the server. Method croaks if the server returns an error.

=cut

sub get_reply {
    my $self = shift;

    while ( not @{ $self->{_replies} } ) {
        die "We are not waiting for reply"
          unless $self->{_to_be_fetched}
              or $self->{_subscription_loop};
        die "You can't read reply in child process" unless $self->{_pid} == $$;
        while ( not $self->_parse_reply ) {
            my $ret = $self->{_socket}->recv( my $buffer, 4096 );
            unless ( defined $ret ) {
                next if $! == EINTR;
                croak "Error reading reply from server: $!";
            }
            if ( $buffer ne '' ) {

                # received some data
                $self->{_buffer} .= $buffer;
            }
            else {

                # disconnected
                die "Server unexpectedly closed connection before sending full reply";
            }
        }
    }

    my $res = shift @{ $self->{_replies} };
    croak "$res" if ref $res eq 'RedisDB::Error';
    return $res;
}

=head2 $self->get_all_replies

Wait for the replies to all the commands sent and return them as a list.

=cut

sub get_all_replies {
    my $self = shift;
    my @res;
    while ( $self->replies_to_fetch ) {
        push @res, $self->get_reply;
    }
    return @res;
}

=head2 $self->replies_to_fetch

Return the number of commands sent to the server replies to which wasn't yet
retrieved with I<get_reply> or I<get_all_replies>.

=cut

sub replies_to_fetch {
    my $self = shift;
    return $self->{_to_be_fetched} + @{ $self->{_replies} };
}

=head2 $self->selected_database

Get currently selected database.

=cut

sub selected_database {
    shift->{database};
}

=head2 $self->version

Return the version of the server the client is connected to. The version is
returned as a floating point number represented the same way as the perl
versions. E.g. for redis 2.1.12 it will return 2.001012.

=cut

sub version {
    my $self = shift;
    my $info = $self->info;
    $info->{redis_version} =~ /^([0-9]+)[.]([0-9]+)(?:[.]([0-9]+))?/
      or die "Can't parse version string: $info->{redis_version}";
    $self->{_server_version} = $1 + 0.001 * $2 + ( $3 ? 0.000001 * $3 : 0 );
    return $self->{_server_version};
}

my @commands = qw(
  append	auth	bgrewriteaof	bgsave	blpop	brpop   brpoplpush	config_get
  config_set	config_resetstat	dbsize	debug_object	debug_segfault
  decr	decrby	del	echo	exists	expire	expireat	flushall
  flushdb	get	getbit	getrange	getset	hdel	hexists	hget	hgetall
  hincrby	hkeys	hlen	hmget	hmset	hset	hsetnx	hvals	incr	incrby
  keys	lastsave	lindex	linsert	llen	lpop	lpush	lpushx
  lrange	lrem	lset	ltrim	mget	move	mset	msetnx	persist	ping
  publish	quit	randomkey	rename	renamenx	rpop	rpoplpush
  rpush	rpushx	sadd	save	scard	sdiff	sdiffstore	select	set
  setbit	setex	setnx	setrange	sinter	sinterstore
  sismember	slaveof	smembers	smove	sort	spop	srandmember
  srem	strlen	sunion	sunionstore	sync	ttl	type	unwatch watch
  zadd	zcard
  zcount	zincrby	zinterstore	zrange	zrangebyscore	zrank	zrem
  zremrangebyrank   zremrangebyscore	zrevrange	zrevrangebyscore	zrevrank
  zscore	zunionstore
);

=head1 WRAPPER METHODS

Instead of using I<execute> and I<send_command> methods directly, it may be
more convenient to use wrapper methods with names matching names of the redis
commands. These methods call I<execute> or I<send_command> depending on the
presence of the callback argument. If callback is specified, the method invokes
I<send_command> and returns as soon as the command has been sent to the server;
when the reply is received, it will be passed to the callback (see
L</"PIPELINING SUPPORT">). If there is no callback, the method invokes
I<execute>, waits for the reply from the server, and returns it. E.g.:

    $val = $redis->get($key);
    # equivalent to
    $val = $redis->execute("get", $key);

    $redis->get($key, sub { $val = $_[1] });
    # equivalent to
    $redis->send_command("get", $key, sub { $val = $_[1] });

The following wrapper methods are defined: append, auth, bgrewriteaof, bgsave,
blpop, brpop, brpoplpush, config_get, config_set, config_resetstat, dbsize,
debug_object, debug_segfault, decr, decrby, del, echo, exists, expire,
expireat, flushall, flushdb, get, getbit, getrange, getset, hdel, hexists,
hget, hgetall, hincrby, hkeys, hlen, hmget, hmset, hset, hsetnx, hvals, incr,
incrby, keys, lastsave, lindex, linsert, llen, lpop, lpush, lpushx, lrange,
lrem, lset, ltrim, mget, move, mset, msetnx, persist, ping, publish, quit,
randomkey, rename, renamenx, rpop, rpoplpush, rpush, rpushx, sadd, save, scard,
sdiff, sdiffstore, select, set, setbit, setex, setnx, setrange, sinter,
sinterstore, sismember, slaveof, smembers, smove, sort, spop, srandmember,
srem, strlen, sunion, sunionstore, sync, ttl, type, unwatch, watch, zadd,
zcard, zcount, zincrby, zinterstore, zrange, zrangebyscore, zrank, zrem,
zremrangebyrank, zremrangebyscore, zrevrange, zrevrangebyscore, zrevrank,
zscore, zunionstore

See description of all commands in redis documentation at
L<http://redis.io/commands>.

=cut

for my $command (@commands) {
    my $uccom = uc $command;
    $uccom =~ s/_/ /g;
    no strict 'refs';
    *{ __PACKAGE__ . "::$command" } = sub {
        my $self = shift;
        if ( ref $_[-1] eq 'CODE' ) {
            return $self->send_command( $uccom, @_ );
        }
        else {
            return $self->execute( $uccom, @_ );
        }
    };
}

=pod

The following commands implement some additional postprocessing of results:

=cut

=head2 $self->info

return information and statistics about the server. Redis-server returns
information in form of I<field:value>, the I<info> method parses result and
returns it as a hash reference.

=cut

sub info {
    my $self = shift;

    my $info = $self->execute('INFO');
    my %info = map { /^([^:]+):(.*)$/ } split /\r\n/, $info;
    return \%info;
}

=head2 $self->shutdown

Shuts the redis server down. Returns undef, as the server doesn't send the
answer.  Croaks in case of the error.

=cut

sub shutdown {
    my $self = shift;
    $self->send_command_cb('SHUTDOWN');
    pop @{ $self->{_callbacks} };
    return;
}

=head1 HANDLING OF SERVER DISCONNECTS

Redis server may close a connection if it was idle for some time, also the
connection may be closed in case when redis-server was restarted. RedisDB
restores a connection to the server, but only if no data was lost as a result
of the disconnect. E.g. if the client was idle for some time and the redis
server closed the connection, it will be transparently restored on sending next
command. If you sent a command and the server has closed the connection without
sending a complete reply, the connection will not be restored and the module
will throw an exception. Also the module will throw an exception if the
connection was closed in the middle of a transaction or while you're in a
subscription loop.

=cut

=head1 PIPELINING SUPPORT

You can send commands in the pipelining mode. It means you are sending multiple
commands to the server without waiting for the replies.  This is implemented by
the I<send_command> method. Recommended way of using it is to pass a reference
to the callback function as the last argument.  When module receives reply from
the server, it will call this function with two arguments: reference to the
RedisDB object, and reply from the server. It is important to understand
though, that RedisDB does not run any background threads, neither it checks for
the replies by setting some timer, so e.g. in the following example callback
will never be invoked:

    my $pong;
    $redis->send_command( "ping", sub { $pong = $_[1] } );
    sleep 1 while not $pong;    # this will never return

Therefore you need perriodically trigger check for the replies. The check is
triggered when you call the following methods: I<send_command>, I<reply_ready>,
I<get_reply>, I<get_all_replies>.  Calling wrapper method, like
C<< $redis->get('key') >>, will also trigger check as internally wrapper methods
use methods listed above.

If you invoke I<send_command> without a callback argument, you have to fetch
reply later explicitly using I<get_reply> method. This is how synchronous
I<execute> is implemented, basically it is:

    sub execute {
        my $self = shift;
        $self->send_command(@_);
        return $self->get_reply;
    }

That is why it is not allowed to call I<execute> if you have not got replies to
all commands sent previously with I<send_command> without callback.

Sometimes you are not interested in replies sent by the server, e.g. SET
command usually just return 'OK', in this case you can pass to I<send_command>
callback which ignores its arguments, or use C<RedisDB::IGNORE_REPLY> constant, it
is a no-op function:

    for (@keys) {
        # execute will not just send 'GET' command to the server,
        # but it will also receive response to the 'SET' command sent on
        # the previous loop iteration
        my $val = $redis->execute( "get", $_ );
        $redis->send_command( "set", $_, fun($val), RedisDB::IGNORE_REPLY );
    }
    # and this will wait for the last reply
    $redis->mainloop;

or using L</"WRAPPER METHODS"> you can rewrite it as:

    for (@keys) {
        my $val = $redis->get($_);
        $redis->set( $_, fun($val), RedisDB::IGNORE_REPLY );
    }
    $redis->mainloop;

=cut

=head1 SUBSCRIPTIONS SUPPORT

RedisDB supports subscriptions to redis channels. In the subscription mode you
can subscribe to some channels and receive all the messages sent to these
channels.  Every time RedisDB receives a message for the channel it invokes a
callback provided by the user. User can specify different callbacks for the
different channels.  When in the subscription mode you can subscribe to
additional channels, or unsubscribe from the channels you subscribed to, but
you can't use any other redis commands like set, get, etc. Here is the example
of running in the subscription mode:

    my $message_cb = sub {
        my ($redis, $channel, $pattern, $message) = @_;
        print "$channel: $message\n";
    };
    
    my $control_cb = sub {
        my ($redis, $channel, $pattern, $message) = @_;
        if ($channel eq 'control.quit') {
            $redis->unsubscribe;
            $redis->punsubscribe;
        }
        elsif ($channel eq 'control.subscribe') {
            $redis->subscribe($message);
        }
    };
    
    subscription_loop(
        subscribe => [ 'news',  ],
        psubscribe => [ 'control.*' => $control_cb ],
        default_callback => $message_cb,
    );

subscription_loop will subscribe you to the news channel and control.*
channels. It will call specified callbacks every time a new message received.
You can subscribe to additional channels sending their names to the
control.subscribe channel. You can unsubscribe from all the channels by sending
a message to the control.quit channel. Every callback receives four arguments:
the RedisDB object, the channel for which the message was received, the pattern
if you subscribed to this channel using I<psubscribe> method, and the message
itself.

You can publish messages into the channels using the I<publish> method. This
method should be called when you in the normal mode, and can't be used while
you're in the subscription mode.

Following methods can be used in subscribtion mode:

=cut

=head2 $self->subscription_loop(%parameters)

Enter into the subscription mode. The method subscribes you to the specified
channels, waits for the messages, and invokes the appropriate callback for
every received message. The method returns after you unsubscribed from all the
channels. It accepts the following parameters:

=over 4

=item default_callback

reference to the default callback. This callback is invoked for a message if you
didn't specify other callback for the channel this message comes from.

=item subscribe

an array reference. Contains the list of channels you want to subscribe. A
channel name may be optionally followed by the reference to a callback function
for this channel.  E.g.:

    [ 'news', 'messages', 'errors' => \&error_cb, 'other' ]

channels "news", "messages", and "other" will use default callback, but for
the "errors" channel error_cb function will be used.

=item psubscribe

same as subscribe, but you specify patterns for channels' names.

=back

All parameters are optional, but you must subscribe at least to one channel. Also
if default_callback is not specified, you have to explicitely specify a callback
for every channel you are going to subscribe.

=cut

sub subscription_loop {
    my ( $self, %args ) = @_;
    croak "Already in subscription loop" if $self->{_subscribtion_loop};
    croak "You can't start subscription loop while in pipelining mode."
      if @{ $self->{_callbacks} }
          or @{ $self->{_replies} };
    $self->{_subscribed}        = {};
    $self->{_psubscribed}       = {};
    $self->{_subscription_cb}   = $args{default_callback};
    $self->{_subscription_loop} = 1;

    if ( $args{subscribe} ) {
        while ( my $channel = shift @{ $args{subscribe} } ) {
            my $cb;
            $cb = shift @{ $args{subscribe} } if ref $args{subscribe}[0] eq 'CODE';
            $self->subscribe( $channel, $cb );
        }
    }
    if ( $args{psubscribe} ) {
        while ( my $channel = shift @{ $args{psubscribe} } ) {
            my $cb;
            $cb = shift @{ $args{psubscribe} } if ref $args{psubscribe}[0] eq 'CODE';
            $self->psubscribe( $channel, $cb );
        }
    }
    croak "You must subscribe at least to one channel"
      unless ( keys %{ $self->{_subscribed} } or keys %{ $self->{_psubscribed} } );

    while ( my $msg = $self->get_reply ) {
        die "Expected multi-bulk reply, but got $msg" unless ref $msg;
        if ( $msg->[0] eq 'message' ) {
            $self->{_subscribed}{ $msg->[1] }( $self, $msg->[1], undef, $msg->[2] );
        }
        elsif ( $msg->[0] eq 'pmessage' ) {
            $self->{_psubscribed}{ $msg->[1] }( $self, $msg->[2], $msg->[1], $msg->[3] );
        }
        elsif ( $msg->[0] eq 'subscribe' or $msg->[0] eq 'psubscribe' ) {

            # ignore
        }
        elsif ( $msg->[0] eq 'unsubscribe' ) {
            delete $self->{_subscribed}{ $msg->[1] };

            # TODO think about it, not exactly correct
            last unless $msg->[2] or %{ $self->{_psubscribed} };
        }
        elsif ( $msg->[0] eq 'punsubscribe' ) {
            delete $self->{_psubscribed}{ $msg->[1] };
            last unless $msg->[2] or %{ $self->{_subscribed} };
        }
        else {
            die "Got unknown reply $msg->[0] in subscription mode";
        }
    }
    $self->{_to_be_fetched} = 0;
    $self->_connect;
    return;
}

=head2 $self->subscribe($channel[, $callback])

Subscribe to the additional I<$channel>. If I<$callback> is not specified,
default callback will be used.

=cut

sub subscribe {
    my ( $self, $channel, $callback ) = @_;
    croak "Must be in subscription loop to subscribe" unless $self->{_subscription_loop};
    croak "Subscribe to what channel?" unless length $channel;
    $callback ||= $self->{_subscription_cb}
      or croak "Callback for $channel not specified, neither default callback defined";
    $self->{_subscribed}{$channel} = $callback;
    $self->send_command( "SUBSCRIBE", $channel );
    return;
}

=head2 $self->psubscribe($pattern[, $callback])

Subscribe to additional channels matching I<$pattern>. If I<$callback> is not specified, default
callback will be used.

=cut

sub psubscribe {
    my ( $self, $channel, $callback ) = @_;
    croak "Must be in subscription loop to subscribe" unless $self->{_subscription_loop};
    croak "Subscribe to what channel?" unless length $channel;
    $callback ||= $self->{_subscription_cb}
      or croak "Callback for $channel not specified, neither default callback defined";
    $self->{_psubscribed}{$channel} = $callback;
    $self->send_command( "PSUBSCRIBE", $channel );
    return;
}

=head2 $self->unsubscribe([@channels])

Unsubscribe from the listed I<@channels>. If no channels was specified,
unsubscribe from all the channels.

=cut

sub unsubscribe {
    my $self = shift;
    return $self->send_command( "UNSUBSCRIBE", @_ );
}

=head2 $self->punsubscribe([@patterns])

Unsubscribe from the listed I<@patterns>. If no patterns was specified,
unsubscribe from all the channels to which you subscribed using I<psubscribe>.

=cut

sub punsubscribe {
    my $self = shift;
    return $self->send_command( "PUNSUBSCRIBE", @_ );
}

=head2 $self->subscribed

Return list of channels to which you have subscribed using I<subscribe>

=cut

sub subscribed {
    return keys %{ shift->{_subscribed} };
}

=head2 $self->psubscribed

Return list of channels to which you have subscribed using I<psubscribe>

=cut

sub psubscribed {
    return keys %{ shift->{_psubscribed} };
}

=head1 TRANSACTIONS SUPPORT

Transactions allow you to execute a sequence of commands in a single step. In
order to start a transaction you should use the I<multi> method.  After you
have entered a transaction all the commands you issue are queued, but not
executed till you call the I<exec> method. Tipically these commands return
string "QUEUED" as result, but if there is an error in e.g. number of
arguments, they may croak. When you call exec, all the queued commands will be
executed and exec will return a list of results for every command in the
transaction. If any command has failed, exec will croak. If instead of I<exec>
you call I<discard>, all scheduled commands will be canceled.

You can set some keys as watched. If any whatched key has been changed by
another client before you called exec, the transaction will be discarded and
exec will return false value.

=cut

=head2 $self->multi

Enter the transaction. After this and till I<exec> or I<discard> will be called,
all the commands will be queued but not executed.

=cut

sub multi {
    my $self = shift;

    my $res = $self->execute('MULTI');
    $self->{_in_multi} = 1;
    return $res;
}

=head2 $self->exec

Execute all queued commands and finish the transaction. Returns a list of
results for every command. Will croak if some command has failed.  Also
unwatches all the keys. If some of the watched keys has been changed by other
client, the transaction will be canceled and I<exec> will return false.

=cut

sub exec {
    my $self = shift;

    my $res = $self->execute('EXEC');
    $self->{_in_multi} = undef;
    return $res;
}

=head2 $self->discard

Discard all queued commands without executing them and unwatch all keys.

=cut

sub discard {
    my $self = shift;

    my $res = $self->execute('DISCARD');
    $self->{_in_multi} = undef;
    return $res;
}

# build_redis_request($command, @arguments)
#
# Builds unified redis request from given I<$command> and I<@arguments>.
sub _build_redis_request {
    my $nargs = @_;

    use bytes;
    my $req = "*$nargs\015\012";
    while ( $nargs-- ) {
        my $arg = shift;
        $req .= '$' . length($arg) . "\015\012" . $arg . "\015\012";
    }
    return $req;
}

# $self->_parse_reply
#
# checks if buffer contains full reply. Returns 1 if it is,
# and pushes reply into @{$self->{_replies}}
my ( $READ_LINE, $READ_NUMBER, $READ_BULK_LEN, $READ_BULK, $READ_MBLK_LEN, $WAIT_BUCKS ) = 1 .. 6;

sub _parse_reply {
    my $self = shift;
    return unless $self->{_buffer};

    # if we not yet started parsing reply
    unless ( $self->{_parse_state} ) {
        my $type = substr( $self->{_buffer}, 0, 1, '' );
        $self->{_parse_reply} = [$type];
        if ( $type eq '+' or $type eq '-' ) {
            $self->{_parse_state} = $READ_LINE;
        }
        elsif ( $type eq ':' ) {
            $self->{_parse_state} = $READ_NUMBER;
        }
        elsif ( $type eq '$' ) {
            $self->{_parse_state} = $READ_BULK_LEN;
        }
        elsif ( $type eq '*' ) {
            $self->{_parse_state}      = $READ_MBLK_LEN;
            $self->{_parse_mblk_level} = 1;
        }
        else {
            die "Got invalid reply: $type$self->{_buffer}";
        }
    }

    # parse data
    my $repeat    = 1;
    my $completed = 0;
    while ($repeat) {
        $repeat = 0;
        return unless length $self->{_buffer} >= 2;
        if ( $self->{_parse_state} == $READ_LINE ) {
            if ( defined( my $line = $self->_read_line ) ) {
                if ( $self->{_parse_reply}[0] eq '+' or $self->{_parse_reply}[0] eq '-' ) {
                    $self->{_parse_reply}[1] = $line;
                    return $self->_reply_completed;
                }
                else {
                    $repeat    = $self->_mblk_item($line);
                    $completed = !$repeat;
                }
            }
        }
        elsif ( $self->{_parse_state} == $READ_NUMBER ) {
            if ( defined( my $line = $self->_read_line ) ) {
                die "Received invalid integer reply :$line" unless $line =~ /^-?[0-9]+$/;
                if ( $self->{_parse_reply}[0] eq ':' ) {
                    $self->{_parse_reply}[1] = $line;
                    return $self->_reply_completed;
                }
                else {
                    $repeat    = $self->_mblk_item($line);
                    $completed = !$repeat;
                }
            }
        }
        elsif ( $self->{_parse_state} == $READ_BULK_LEN ) {
            if ( defined( my $len = $self->_read_line ) ) {
                if ( $len >= 0 ) {
                    $self->{_parse_state}    = $READ_BULK;
                    $self->{_parse_bulk_len} = $len;
                    $repeat                  = 1;
                }
                elsif ( $len == -1 ) {
                    if ( $self->{_parse_reply}[0] eq '$' ) {
                        $self->{_parse_reply}[1] = undef;
                        return $self->_reply_completed;
                    }
                    else {
                        $repeat    = $self->_mblk_item(undef);
                        $completed = !$repeat;
                    }
                }
            }
        }
        elsif ( $self->{_parse_state} == $READ_BULK ) {
            return unless length $self->{_buffer} >= 2 + $self->{_parse_bulk_len};
            my $bulk = substr( $self->{_buffer}, 0, $self->{_parse_bulk_len}, '' );
            substr $self->{_buffer}, 0, 2, '';
            if ( $self->{_parse_reply}[0] eq '$' ) {
                $self->{_parse_reply}[1] = $bulk;
                return $self->_reply_completed;
            }
            else {
                $repeat    = $self->_mblk_item($bulk);
                $completed = !$repeat;
            }
        }
        elsif ( $self->{_parse_state} == $READ_MBLK_LEN ) {
            if ( defined( my $len = $self->_read_line ) ) {
                if ( $len > 0 ) {
                    $self->{_parse_mblk_len} = $len;
                    $self->{_parse_state}    = $WAIT_BUCKS;
                    $self->{_parse_reply}[1] = [];
                    $repeat                  = 1;
                }
                elsif ( $len == 0 || $len == -1 ) {
                    if ( $self->{_parse_mblk_level}-- == 1 ) {
                        $self->{_parse_reply}[1] = $len ? undef : [];
                        return $self->_reply_completed;
                    }
                    else {
                        ( $self->{_parse_mblk_len}, $self->{_parse_reply} ) =
                          @{ $self->{_parse_mblk_store} };
                        push @{ $self->{_parse_reply}[1] }, $len ? undef : [];
                        if ( --$self->{_parse_mblk_len} ) {
                            $self->{_parse_state} = $WAIT_BUCKS;
                            $repeat = 1;
                        }
                        else {
                            $repeat = 0;
                        }
                        $completed = !$repeat;
                    }
                }
                else {
                    die "Invalid multi-bulk reply: *$len\015\012$self->{_buffer}";
                }
            }
        }
        elsif ( $self->{_parse_state} == $WAIT_BUCKS ) {
            my $char = substr( $self->{_buffer}, 0, 1, '' );
            if ( $char eq '$' ) {
                $self->{_parse_state} = $READ_BULK_LEN;
            }
            elsif ( $char eq ':' ) {
                $self->{_parse_state} = $READ_NUMBER;
            }
            elsif ( $char eq '+' ) {
                $self->{_parse_state} = $READ_LINE;
            }
            elsif ( $char eq '*' ) {
                $self->{_parse_state} = $READ_MBLK_LEN;
                $self->{_parse_mblk_level}++;
                $self->{_parse_mblk_store} = [ $self->{_parse_mblk_len}, $self->{_parse_reply} ];
                $self->{_parse_reply} = ['*'];
            }
            else {
                die "Invalid multi-bulk reply. Expected '\$' or ':' but got $char"
                  ;    # $self->{_buffer}";
            }
            $repeat = 1;
        }
    }
    return $completed ? $self->_reply_completed : undef;
}

sub _read_line {
    my $self = shift;
    my $pos = index $self->{_buffer}, "\015\012";
    my $line;
    if ( $pos >= 0 ) {

        # Got end of the line, add all stuff before \r\n
        # to the reply string. Strip \r\n from the buffer
        $line = substr( $self->{_buffer}, 0, $pos, '' );
        substr $self->{_buffer}, 0, 2, '';
    }
    return $line;
}

sub _mblk_item {
    my ( $self, $value ) = @_;

    push @{ $self->{_parse_reply}[1] }, $value;
    my $repeat;
    if ( --$self->{_parse_mblk_len} ) {
        $self->{_parse_state} = $WAIT_BUCKS;
        $repeat = 1;
    }
    elsif ( --$self->{_parse_mblk_level} ) {
        $self->{_parse_mblk_len} = shift @{ $self->{_parse_mblk_store} };
        $self->{_parse_mblk_len}--;
        my $reply = shift @{ $self->{_parse_mblk_store} };
        push @{ $reply->[1] }, $self->{_parse_reply}[1];
        $self->{_parse_reply} = $reply;
        $self->{_parse_state} = $WAIT_BUCKS;
        $repeat               = $self->{_parse_mblk_len} > 0;
    }
    else {
        $repeat = 0;
    }

    return $repeat;
}

sub _reply_completed {
    my $self = shift;
    $self->{_parse_state} = undef;
    my $cb = shift @{ $self->{_callbacks} };
    $cb ||= \&_queue;
    my $rep;
    if ( $self->{_parse_reply}[0] eq '-' ) {
        $rep = RedisDB::Error->new( $self->{_parse_reply}[1] );
    }
    else {
        $rep = $self->{_parse_reply}[1];
    }
    $cb->( $self, $rep );
    $self->{_parse_reply} = undef;
    return 1;
}

1;

__END__

=head1 SEE ALSO

L<Redis>, L<Redis::hiredis>, L<AnyEvent::Redis>

=head1 WHY ANOTHER ONE

I was in need of the client for redis database. L<AnyEvent::Redis> didn't suite
me as it requires an event loop, and it didn't fit into the existing code. The
problem with L<Redis> is that it didn't (at the time I started this) reconnect
to the server if connection was closed after timeout or as result of the server
restart, and it does not support pipelining. After analizing what I need to
change in L<Redis> in order to get all I want, I decided that it will be
simplier to write the new module from scratch. This also solves the problem of
backward compatibility.

=head1 BUGS

Please report any bugs or feature requests via GitHub bug tracker at
L<http://github.com/trinitum/RedisDB/issues>.

Known bugs are:

Timeout support is OS dependent. If OS doesn't support SO_SNDTIMEO and SO_RCVTIMEO
options timeouts will not work.

=head1 ACKNOWLEDGEMENTS

Thanks to Sanko Robinson and FunkyMonk for help with porting this module on Windows.

=head1 AUTHOR

Pavel Shaydo, C<< <zwon at cpan.org> >>

=head1 LICENSE AND COPYRIGHT

Copyright 2011 Pavel Shaydo.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut
