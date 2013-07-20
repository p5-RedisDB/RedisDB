package RedisDB;

use strict;
use warnings;
our $VERSION = "2.15";
$VERSION = eval $VERSION;

use RedisDB::Error;
use RedisDB::Parse::Redis;
use IO::Socket::INET;
use IO::Socket::UNIX;
use Socket qw(MSG_DONTWAIT MSG_NOSIGNAL SO_RCVTIMEO SO_SNDTIMEO);
use POSIX qw(:errno_h);
use Config;
use Carp;
use Try::Tiny;
use Encode qw();

=head1 NAME

RedisDB - Perl extension to access redis database

=head1 SYNOPSIS

    use RedisDB;

    my $redis = RedisDB->new(host => 'localhost', port => 6379);
    $redis->set($key, $value);
    my $value = $redis->get($key);

=head1 DESCRIPTION

This module provides interface to access redis key-value store, it
transparently handles disconnects and forks, supports transactions,
pipelining, and subscription mode. Module includes XS and pure Perl
versions of the parser.

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

=item password

Password, if redis server requires authentication. Alternatively you can use
I<auth> method after connection.

=item database

DB number to use. Specified database will be selected immediately after
connecting to the server. Database changes when you sending I<select> command
to the server. You can get current database using I<selected_database> method.
Default value is 0.

=item raise_error

By default if redis-server returned error reply, or there was a connection
error I<get_reply> method throws an exception of L<RedisDB::Error> type, if
you set this parameter to false it will return an error object instead. Note,
that if you have set this to false you must always check if the result you've
got from RedisDB is a L<RedisDB::Error> object.

=item timeout

IO timeout. With this option set, if IO operation has taken more than specified
number of seconds, module will croak or return L<RedisDB::Error::EAGAIN> error
object depending on L</raise_error> setting. Note, that some OSes do not
support SO_RCVTIMEO, and SO_SNDTIMEO socket options, in this case timeout will
not work.

=item utf8

Assume that all data on the server encoded in UTF-8. As result all strings will
be converted to UTF-8 before sending to server, and all results will be decoded
from UTF-8. See L</"UTF-8 SUPPORT">.

=item connection_name

After establishing connection set its name. See "CLIENT SETNAME" command
description in the redis documentation.

=item lazy

by default I<new> establishes connection to the server. If this parameter is
set, then connection will be established when you will send a command to the
server.

=item reconnect_attempts

this parameter allows you to specify how many attempts to (re)connect to the
server should be made before returning error. Default value is 1, set to -1 if
module should try to reconnect indefinitely.

=item reconnect_delay_max

module makes a delay before each new attempt to connect. Delay increases with
each new attempt. This parameter allows you to specify maximum delay between
attempts to reconnect. Default value is 10.

=item on_connect_error

this allows you to specify callback that will be invoked in the case of failed
connection attempt. First argument to callback is a reference to the RedisDB
object, and second is the error description. You must not invoke any methods on
the object, but you can change I<port> and I<host>, or I<path> attributes.
After callback returned, module tries to establish connection again. Default
callback confesses. This may be useful to switch to reserve server if primary
is down.

=begin comment

=item on_disconnect

callback to invoke when server closed connection

=end comment

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
    $self->{raise_error}    = 1 unless exists $self->{raise_error};
    $self->{_replies}       = [];
    $self->{_to_be_fetched} = 0;
    $self->{database}            ||= 0;
    $self->{reconnect_attempts}  ||= 1;
    $self->{reconnect_delay_max} ||= 10;
    $self->{on_connect_error}    ||= \&_on_connect_error;
    $self->_init_parser;
    $self->_connect unless $self->{lazy};
    return $self;
}

sub _init_parser {
    my $self = shift;
    $self->{_parser} = RedisDB::Parse::Redis->new( utf8 => $self->{utf8}, redisdb => $self );
}

=head2 $self->execute($command, @arguments)

send a command to the server and return the result. It will throw an exception
if the server returns an error or return L<RedisDB::Error> depending on
L</raise_error> parameter. It may be more convenient to use instead of this
method wrapper named after the corresponding redis command. E.g.:

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
    croak "You can't use RedisDB::execute when you have replies to fetch."
      if $self->replies_to_fetch;
    croak "This function is not available in subscription mode." if $self->{_subscription_loop};
    my $cmd = uc shift;
    $self->send_command( $cmd, @_ );
    return $self->get_reply;
}

sub _on_connect_error {
    my ( $self, $err ) = @_;
    my $server = $self->{path} || ("$self->{host}:$self->{port}");
    confess "Couldn't connect to the redis server at $server: $!";
}

sub _on_disconnect {
    my ( $self, $err, $error_obj ) = @_;

    if ($err) {
        $error_obj ||= RedisDB::Error::DISCONNECTED->new(
            "Server unexpectedly closed connection. Some data might have been lost.");
        if ( $self->{raise_error} || $self->{_in_multi} ) {
            $self->reset_connection;
            confess $error_obj;
        }
        elsif ( my $loop_type = $self->{_subscription_loop} ) {
            my $subscribed  = delete $self->{_subscribed};
            my $psubscribed = delete $self->{_psubscribed};
            my $callback    = delete $self->{_subscription_cb};
            $self->reset_connection;
            $self->_connect;
            $self->{_subscription_loop} = $loop_type;
            $self->{_subscription_cb}   = $callback;
            $self->{_parser}->set_default_callback($callback);
            $self->{_subscribed}  = $subscribed;
            $self->{_psubscribed} = $psubscribed;

            for ( keys %$subscribed ) {
                $self->send_command( 'subscribe', $_ );
            }
            for ( keys %$psubscribed ) {
                $self->send_command( 'psubscribe', $_ );
            }
        }
        else {

            # parser may be in inconsistent state, so we just replace it with a new one
            my $parser = delete $self->{_parser};
            delete $self->{_socket};
            $parser->propagate_reply($error_obj);
        }
    }
    else {
        $self->{warnings} and warn( $error_obj || "Server closed connection, reconnecting..." );
    }
}

# establish connection to the server.

sub _connect {
    my $self = shift;

    # this is to prevent recursion
    confess "Couldn't connect to the redis-server."
      . " Connection was immediately closed by the server."
      if $self->{_in_connect};

    $self->{_pid} = $$;

    delete $self->{_socket};
    my $error;
    while ( not $self->{_socket} ) {
        if ( $self->{path} ) {
            $self->{_socket} = IO::Socket::UNIX->new(
                Type => SOCK_STREAM,
                Peer => $self->{path},
            ) or $error = $!;
        }
        else {
            my $attempts = $self->{reconnect_attempts};
            my $delay;
            while ( not $self->{_socket} and $attempts ) {
                sleep $delay if $delay;
                $self->{_socket} = IO::Socket::INET->new(
                    PeerAddr => $self->{host},
                    PeerPort => $self->{port},
                    Proto    => 'tcp',
                    ( $self->{timeout} ? ( Timeout => $self->{timeout} ) : () ),
                ) or $error = $!;
                $delay = $delay ? ( 1 + rand ) * $delay : 1;
                $delay = $self->{reconnect_delay_max} if $delay > $self->{reconnect_delay_max};
                $attempts--;
            }
        }
    }
    continue {
        $self->{on_connect_error}->( $self, $error ) unless $self->{_socket};
    }

    if ( $self->{timeout} ) {
        my $timeout;

        # NetBSD 6 uses 64-bit time_t on all architectures
        my $netbsd6;
        if ( $Config{osname} eq 'netbsd' ) {
            $Config{osvers} =~ /^([0-9]+)/;
            if ( $1 and $1 >= 6 ) {
                $netbsd6 = 1;
            }
        }
        if ( $netbsd6 and $Config{longsize} == 4 ) {
            if ( defined $Config{use64bitint} ) {
                $timeout = pack( 'QL', $self->{timeout}, 0 );
            }
            else {
                $timeout = pack(
                    'LLL',
                    (
                        $Config{byteorder} eq '1234'
                        ? ( $self->{timeout}, 0, 0 )
                        : ( 0, $self->{timeout}, 0 )
                    )
                );
            }
        }
        else {
            $timeout = pack( 'L!L!', $self->{timeout}, 0 );
        }
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

    $self->{_in_connect}++;
    $self->_init_parser;
    $self->{_subscription_loop} = 0;
    delete $self->{_server_version};

    # authenticate
    if ( $self->{password} ) {
        $self->send_command(
            "AUTH",
            $self->{password},
            sub {
                my ( $self, $res ) = @_;
                croak "$res" if ref $res eq 'RedisDB::Error';
            }
        );
    }

    # connection name
    if ( $self->{connection_name} ) {
        $self->send_command( qw(CLIENT SETNAME), $self->{connection_name}, IGNORE_REPLY() );
    }

    # select database
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
        my $ret = recv( $self->{_socket}, my $buf, 131072, $DONTWAIT );
        unless ( defined $ret ) {

            # socket is connected, no data in recv buffer
            last if $! == EAGAIN or $! == EWOULDBLOCK;
            next if $! == EINTR;

            # on any other error close connection
            $self->_on_disconnect( 1,
                RedisDB::Error::DISCONNECTED->new("Error reading from server: $!") );
        }
        elsif ( $buf ne '' ) {

            # received some data
            $self->{_parser}->add($buf);
        }
        else {
            delete $self->{_socket};

            if ( $self->{_parser}->callbacks or $self->{_in_multi} ) {

                # there are some replies lost
                $self->_on_disconnect(1);
            }
            else {
                # clean disconnect, try to reconnect
                $self->_on_disconnect(0);
            }

            $self->_connect unless $self->{_socket};
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
sent, or dies if an error occurred. Note, that it does not return reply from
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

my $NOSIGNAL = try { MSG_NOSIGNAL } || 0;

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

    # remember password
    if ( $command eq 'AUTH' ) {
        $self->{password} = $_[0];
    }

    # if SELECT has been successful, we should update database
    elsif ( $command eq 'SELECT' ) {
        my $cb    = $callback;
        my $dbnum = $_[0];
        $callback = sub {
            $_[0]->{database} = $dbnum unless ref $_[1];
            $cb->(@_);
        };
    }

    # if CLIENT SETNAME we should remember the name
    elsif ( $command eq 'CLIENT' && uc $_[0] eq 'SETNAME' ) {
        $self->{connection_name} = $_[1];
    }

    $self->_connect unless $self->{_socket} and $self->{_pid} == $$;

    # Here we are reading received data and parsing it,
    # and at the same time checking if the connection is still alive
    $self->_recv_data_nb;

    my $request = $self->{_parser}->build_request( $command, @_ );
    $self->{_parser}->add_callback($callback);
    {
        local $SIG{PIPE} = 'IGNORE' unless $NOSIGNAL;
        defined $self->{_socket}->send( $request, $NOSIGNAL )
          or $self->_on_disconnect( 1,
            RedisDB::Error::DISCONNECTED->new("Can't send request to server: $!") );
    }
    return 1;
}

sub _ignore {
    my ( $self, $res ) = @_;
    if ( ref $res eq 'RedisDB::Error' ) {
        warn "Ignoring error returned by redis-server: $res";
    }
}

sub IGNORE_REPLY { return \&_ignore; }

=begin comment

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

=end comment

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

this method blocks till all replies from the server will be received. Note,
that callbacks for some replies may send new requests to the server and so this
method may block for indefinite time.

=cut

sub mainloop {
    my $self = shift;

    while ( $self->{_parser}->callbacks ) {
        croak "You can't call mainloop in the child process" unless $self->{_pid} == $$;
        my $ret = $self->{_socket}->recv( my $buffer, 131072 );
        unless ( defined $ret ) {
            next if $! == EINTR;
            if ( $! == EAGAIN ) {
                confess "Timed out waiting reply from the server";
            }
            else {
                $self->_on_disconnect( 1,
                    RedisDB::Error::DISCONNECTED->new("Error reading reply from the server: $!") );
                next;
            }
        }
        if ( $buffer ne '' ) {

            # received some data
            $self->{_parser}->add($buffer);
        }
        else {

            # disconnected
            $self->_on_disconnect(
                1,
                RedisDB::Error::DISCONNECTED->new(
                    "Server unexpectedly closed connection before sending full reply")
            );
        }
    }
    return;
}

=head2 $self->get_reply

Receive and return reply from the server. If the server returned an error,
method throws L<RedisDB::Error> exception or returns L<RedisDB::Error> object,
depending on I<raise_error> parameter, see I<new>.

=cut

sub get_reply {
    my $self = shift;

    croak "We are not waiting for reply"
      unless @{ $self->{_replies} }
      or $self->{_to_be_fetched}
      or $self->{_subscription_loop};
    croak "You can't read reply in child process" unless $self->{_pid} == $$;
    while ( not @{ $self->{_replies} } ) {
        my $ret = $self->{_socket}->recv( my $buffer, 131072 );
        if ( not defined $ret ) {
            next if $! == EINTR or $! == 0;
            my $err;
            if ( $! == EAGAIN or $! == EWOULDBLOCK ) {
                $err = RedisDB::Error::EAGAIN->new("$!");
            }
            else {
                $err = RedisDB::Error::DISCONNECTED->new("Connection error: $!");
            }
            $self->_on_disconnect( 1, $err );
        }
        elsif ( $buffer ne '' ) {

            # received some data
            $self->{_parser}->add($buffer);
        }
        else {

            # disconnected, should die unless raise_error is unset
            $self->_on_disconnect(1);
        }
    }

    my $res = shift @{ $self->{_replies} };
    if ( ref $res eq 'RedisDB::Error'
        and ( $self->{raise_error} or $self->{_in_multi} ) )
    {
        croak $res;
    }

    return $res;
}

=head2 $self->get_all_replies

Wait till replies to all the commands without callback set will be received.
Return a list of replies to these commands. For commands with callback set
replies are processed as usual. Unlike I<mainloop> this method block only till
replies to all commands for which callback was NOT set will be received.

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
retrieved with I<get_reply> or I<get_all_replies>. This number only includes
commands for which callback was not set.

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

=head2 $self->reset_connection

Resets connection. This closes existing connection and drops all previously
sent requests. After invoking this method the object returns to the same state
as it was returned by the constructor.

=cut

sub reset_connection {
    my $self = shift;
    delete $self->{$_} for grep /^_/, keys %$self;
    $self->{_replies} = [];
    $self->_init_parser;
    $self->{_to_be_fetched} = 0;
    return;
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
      or croak "Can't parse version string: $info->{redis_version}";
    $self->{_server_version} = $1 + 0.001 * $2 + ( $3 ? 0.000001 * $3 : 0 );
    return $self->{_server_version};
}

# don't forget to update POD
my @commands = qw(
  append	auth	bgrewriteaof	bgsave	bitcount	bitop
  blpop	brpop   brpoplpush	client	client_kill	client_getname	client_setname
  config	config_get	config_set	config_resetstat
  dbsize	debug_object	debug_segfault
  decr	decrby	del	dump	echo	eval    evalsha exists	expire	expireat	flushall
  flushdb	get	getbit	getrange	getset	hdel	hexists	hget	hgetall
  hincrby	hincrbyfloat	hkeys	hlen	hmget	hmset	hset	hsetnx	hvals	incr	incrby
  incrbyfloat	keys	lastsave	lindex	linsert	llen	lpop	lpush	lpushx
  lrange	lrem	lset	ltrim	mget	migrate	move	mset	msetnx	object	object_refcount
  object_encoding	object_idletime	persist	pexpire	pexpireat	ping	psetex	pttl
  publish	quit	randomkey	rename	renamenx	restore	rpop	rpoplpush
  rpush	rpushx	sadd	save	scard	script	script_exists   script_flush    script_kill
  script_load   sdiff	sdiffstore	select	set
  setbit	setex	setnx	setrange	sinter	sinterstore
  sismember	slaveof	slowlog smembers	smove	sort	spop	srandmember
  srem	strlen	sunion	sunionstore	sync	time    ttl	type	unwatch watch
  zadd	zcard
  zcount	zincrby	zinterstore	zrange	zrangebyscore	zrank	zrem
  zremrangebyrank   zremrangebyscore	zrevrange	zrevrangebyscore	zrevrank
  zscore	zunionstore
);

sub _simple_commands {
    return @commands;
}

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
bitcount, bitop, blpop, brpop, brpoplpush, client, client_kill, client_getname,
client_setname, config, config_get, config_set, config_resetstat, dbsize,
debug_object, debug_segfault, decr, decrby, del, dump, echo, eval, evalsha,
exists, expire, expireat, flushall, flushdb, get, getbit, getrange, getset,
hdel, hexists, hget, hgetall, hincrby, hincrbyfloat, hkeys, hlen, hmget, hmset,
hset, hsetnx, hvals, incr, incrby, incrbyfloat, keys, lastsave, lindex,
linsert, llen, lpop, lpush, lpushx, lrange, lrem, lset, ltrim, mget, migrate,
move, mset, msetnx, object, object_refcount, object_encoding, object_idletime,
persist, pexpire, pexpireat, ping, psetex, pttl, publish, quit, randomkey,
rename, renamenx, restore, rpop, rpoplpush, rpush, rpushx, sadd, save, scard,
script, script_exists, script_flush, script_kill, script_load, sdiff,
sdiffstore, select, set, setbit, setex, setnx, setrange, sinter, sinterstore,
sismember, slaveof, slowlog, smembers, smove, sort, spop, srandmember, srem,
strlen, sunion, sunionstore, sync, time, ttl, type, unwatch, watch, zadd,
zcard, zcount, zincrby, zinterstore, zrange, zrangebyscore, zrank, zrem,
zremrangebyrank, zremrangebyscore, zrevrange, zrevrangebyscore, zrevrank,
zscore, zunionstore.

See description of all commands in redis documentation at
L<http://redis.io/commands>.

=cut

for my $command (@commands) {
    my @uccom = split /_/, uc $command;
    no strict 'refs';
    *{ __PACKAGE__ . "::$command" } = sub {
        my $self = shift;
        if ( ref $_[-1] eq 'CODE' ) {
            return $self->send_command( @uccom, @_ );
        }
        else {
            return $self->execute( @uccom, @_ );
        }
    };
}

=pod

The following commands implement some additional postprocessing of the results:

=cut

=head2 $self->info([$callback])

return information and statistics about the server. Redis-server returns
information in form of I<field:value>, the I<info> method parses result and
returns it as a hash reference.

=cut

sub info {
    my $self = shift;
    my $orig = $_[-1];
    if ( $orig && ref $orig eq 'CODE' ) {
        my $cb = sub {
            my ( $redis, $info ) = @_;
            $orig->( $redis, _parse_info($info) );
        };
        return $self->send_command( 'INFO', $cb );
    }
    else {
        my $info = $self->execute('INFO');
        return _parse_info($info);
    }
}

sub _parse_info {
    my $info = shift;
    return $info if !$info || ref $info;
    my %info = map { /^([^:]+):(.*)$/ } split /\r\n/, $info;
    return \%info;
}

=head2 $self->client_list([$callback])

return list of clients connected to the server. This method parses server
output and returns result as reference to array of hashes.

=cut

sub client_list {
    my $self = shift;
    my $orig = $_[-1];
    if ( $orig && ref $orig eq 'CODE' ) {
        my $cb = sub {
            my ( $redis, $list ) = @_;
            $orig->( $redis, _parse_client_list($list) );
        };
        return $self->send_command( qw(CLIENT LIST), $cb );
    }
    else {
        my $list = $self->execute(qw(CLIENT LIST));
        return _parse_client_list($list);
    }
}

sub _parse_client_list {
    my $list = shift;
    return $list if !$list || ref $list;
    my @clients = split /\015?\012/, $list;
    my $res = [];
    for (@clients) {
        my %cli = map { /^([^=]+)=(.*)$/ ? ( $1, $2 ) : () } split / /;
        push @$res, \%cli;
    }
    return $res;
}

=head2 $self->shutdown

Shuts the redis server down. Returns undef, as the server doesn't send the
answer.  Croaks in case of the error.

=cut

sub shutdown {
    my $self = shift;
    $self->send_command_cb( 'SHUTDOWN', @_ );
    return;
}

=head1 UTF-8 SUPPORT

The redis protocol is designed to work with the binary data, both keys and
values are encoded in the same way as sequences of octets.  By default this
module expects all data to be just strings of bytes. There is an option to
treat all data as UTF-8 strings. If you pass I<utf8> parameter to the
constructor, module will encode all strings to UTF-8 before sending them to
server, and will decode all strings received from server from UTF-8. This has
following repercussions you should be aware off: first, you can't store binary
data on server with this option on, it would be treated as a sequence of latin1
characters, and would be converted into a corresponding sequence of UTF-8
encoded characters; second, if data returned by the server is not a valid UTF-8
encoded string, the module will croak, and you will have to reinitialize the
connection. The parser only checks for invalid UTF-8 byte sequences, it doesn't
check if input contains invalid code points. Generally, using this option is
not recommended.

=cut

=head1 ERROR HANDLING

If L</raise_error> parameter was set to true in the constructor (which is
default setting), then module will throw an exception in case network IO
function returned an error, or if redis-server returned an error reply. Network
exceptions belong to L<RedisDB::Error::EAGAIN> or
L<RedisDB::Error::DISCONNECTED> class, if redis-server returned an error
exception will be of L<RedisDB::Error> class. If the object was in subscription
mode, you will have to restore all the subscriptions. If the object was in the
middle of transaction, when after network error you will have to start the
transaction again.

If L</raise_error> parameter was disabled, then instead of throwing an
exception, module will return exception object and also pass this exception
object to every callback waiting for the reply from the server. If the object
is in subscription mode, then module will automatically restore all
subscriptions after reconnect. Note, that during transaction L</raise_error> is
always enabled, so any error will throw an exception.

=cut

=head1 HANDLING OF SERVER DISCONNECTS

Redis server may close a connection if it was idle for some time, also the
connection may be closed in case when redis-server was restarted, or just
because of the network problem. RedisDB always tries to restore connection to
the server if no data has been lost as a result of disconnect, and if
L</raise_error> parameter disabled it will try to reconnect even if disconnect
happened during data transmission.  E.g. if the client was idle for some time
and the redis server closed the connection, it will be transparently restored
when you send a command next time no matter if L</raise_error> enabled or not.
If you sent a command and the server has closed the connection without sending
a complete reply, then module will act differently depending on L</raise_error>
value.  If L</raise_error> enabled, the module will cancel all current
callbacks, reset the object to the initial state, and throw an exception of
L<RedisDB::Error::DISCONNECTED> class, next time you use the object it will
establish a new connection. If L</raise_error> disabled, the module will pass
L<RedisDB::Error::DISCONNECTED> object to all outstanding callbacks and will
try to reconnect to the server; it will also automatically restore
subscriptions if object was in subscription mode.

Module makes several attempts to reconnect each time increasing interval before
the next attempt, depending on the values of L</reconnect_attempts> and
L</reconnect_delay_max>. After each failed attempt to connect module will
invoke L</on_connect_error> callback which for example may change redis-server
hostname, so on next attempt module will try to connect to different server.

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

Therefore you need periodically trigger check for the replies. The check is
triggered when you call the following methods: I<send_command>, I<reply_ready>,
I<get_reply>, I<get_all_replies>.  Calling wrapper method, like
C<< $redis->get('key') >>, will also trigger check as internally wrapper methods
use methods listed above.

Also you can omit callback argument when invoke I<send_command>. In this case
you have to fetch reply later explicitly using I<get_reply> method. This is how
synchronous I<execute> is implemented, basically it is:

    sub execute {
        my $self = shift;
        $self->send_command(@_);
        return $self->get_reply;
    }

That is why it is not allowed to call I<execute> unless you have got replies to
all commands sent previously with I<send_command> without callback. Using
I<send_command> without callback is not recommended.

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

Following methods can be used in subscription mode:

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
if default_callback is not specified, you have to explicitly specify a callback
for every channel you are going to subscribe.

=cut

sub subscription_loop {
    my ( $self, %args ) = @_;
    croak "Already in subscription loop" if $self->{_subscription_loop};
    croak "You can't start subscription loop while in pipelining mode."
      if $self->{_parser}->callbacks
      or @{ $self->{_replies} };
    $self->{_subscribed}        = {};
    $self->{_psubscribed}       = {};
    $self->{_subscription_cb}   = $args{default_callback};
    $self->{_subscription_loop} = 1;
    $self->{_parser}->set_default_callback( \&_queue );

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

    while ( $self->{_subscription_loop} ) {
        my $msg = $self->get_reply;
        confess "Expected multi-bulk reply, but got $msg" unless ref $msg;
        if ( $msg->[0] eq 'message' ) {
            $self->{_subscribed}{ $msg->[1] }( $self, $msg->[1], undef, $msg->[2] )
              if $self->{_subscribed}{ $msg->[1] };
        }
        elsif ( $msg->[0] eq 'pmessage' ) {
            $self->{_psubscribed}{ $msg->[1] }( $self, $msg->[2], $msg->[1], $msg->[3] )
              if $self->{_psubscribed}{ $msg->[1] };
        }
        elsif ( $msg->[0] =~ /^p?(un)?subscribe/ ) {

            # ignore
        }
        else {
            confess "Got unknown reply $msg->[0] in subscription mode";
        }
    }
    return;
}

=head2 $self->subscribe($channel[, $callback])

Subscribe to the I<$channel>. If I<$callback> is not specified, default
callback will be used. If you are invoking I<subscribe> outside of subscription
loop, I<$callback> is ignored.

=cut

sub subscribe {
    my ( $self, $channel, $callback ) = @_;
    unless ( $self->{_subscription_loop} ) {
        $self->{_subscription_loop} = -1;
        $self->{_subscription_cb}   = \&_queue;
        $self->{_parser}->set_default_callback( \&_queue );
    }
    croak "Subscribe to what channel?" unless length $channel;
    if ( $self->{_subscription_loop} > 0 ) {
        $callback ||= $self->{_subscription_cb}
          or croak "Callback for $channel not specified, neither default callback defined";
    }
    else {
        $callback = 1;
    }
    $self->{_subscribed}{$channel} = $callback;
    $self->send_command( "SUBSCRIBE", $channel );
    return;
}

=head2 $self->psubscribe($pattern[, $callback])

Subscribe to channels matching I<$pattern>. If I<$callback> is not specified,
default callback will be used. If you are invoking I<psubscribe> outside of
subscription loop, I<$callback> is ignored.

=cut

sub psubscribe {
    my ( $self, $channel, $callback ) = @_;
    unless ( $self->{_subscription_loop} ) {
        $self->{_subscription_loop} = -1;
        $self->{_subscription_cb}   = \&_queue;
        $self->{_parser}->set_default_callback( \&_queue );
    }
    croak "Subscribe to what channel?" unless length $channel;
    if ( $self->{_subscription_loop} > 0 ) {
        $callback ||= $self->{_subscription_cb}
          or croak "Callback for $channel not specified, neither default callback defined";
    }
    else {
        $callback = 1;
    }
    $self->{_psubscribed}{$channel} = $callback;
    $self->send_command( "PSUBSCRIBE", $channel );
    return;
}

=head2 $self->unsubscribe([@channels])

Unsubscribe from the listed I<@channels>. If no channels was specified,
unsubscribe from all the channels to which you have subscribed using
I<subscribe>.

=cut

sub unsubscribe {
    my $self = shift;
    if (@_) {
        delete $self->{_subscribed}{$_} for @_;
    }
    else {
        $self->{_subscribed} = {};
    }
    if ( %{ $self->{_subscribed} } or %{ $self->{_psubscribed} } ) {
        return $self->send_command( "UNSUBSCRIBE", @_ );
    }
    else {
        delete $self->{_subscription_loop};
        $self->{_to_be_fetched} = 0;
        return $self->_connect;
    }
}

=head2 $self->punsubscribe([@patterns])

Unsubscribe from the listed I<@patterns>. If no patterns was specified,
unsubscribe from all the channels to which you have subscribed using
I<psubscribe>.

=cut

sub punsubscribe {
    my $self = shift;
    if (@_) {
        delete $self->{_psubscribed}{$_} for @_;
    }
    else {
        $self->{_psubscribed} = {};
    }
    if (   %{ $self->{_subscribed} }
        or %{ $self->{_psubscribed} } )
    {
        return $self->send_command( "PUNSUBSCRIBE", @_ );
    }
    else {
        delete $self->{_subscription_loop};
        $self->{_to_be_fetched} = 0;
        return $self->_connect;
    }
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
executed till you call the I<exec> method. Typically these commands return
string "QUEUED" as a result, but if there is an error in e.g. number of
arguments, they may croak. When you call exec, all the queued commands will be
executed and exec will return a list of results for every command in the
transaction. If instead of I<exec> you call I<discard>, all scheduled commands
will be canceled.

You can set some keys as watched. If any watched key has been changed by
another client before you called exec, the transaction will be discarded and
exec will return false value.

=cut

=head2 $self->multi

Enter the transaction. After this and till I<exec> or I<discard> will be called,
all the commands will be queued but not executed.

=cut

sub multi {
    my $self = shift;

    die "Multi calls can not be nested!" if $self->{_in_multi};
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

1;

__END__

=head1 SEE ALSO

L<Redis>, L<Redis::hiredis>, L<Redis::Client>, L<AnyEvent::Redis>, L<AnyEvent::Redis::RipeRedis>

=head1 WHY ANOTHER ONE

I was in need of the client for redis database. L<AnyEvent::Redis> didn't suite
me as it requires an event loop, and it didn't fit into the existing code. The
problem with L<Redis> is that it didn't (at the time I started this) reconnect
to the server if connection was closed after timeout or as result of the server
restart, and it does not support pipelining. After analyzing what I need to
change in L<Redis> in order to get all I want, I decided that it will be
simpler to write the new module from scratch. This also solves the problem of
backward compatibility.

=head1 BUGS

Please report any bugs or feature requests via GitHub bug tracker at
L<http://github.com/trinitum/RedisDB/issues>.

Known bugs are:

Timeout support is OS dependent. If OS doesn't support SO_SNDTIMEO and SO_RCVTIMEO
options timeouts will not work.

QUIT command doesn't work with redis-server before version 2.0

=head1 ACKNOWLEDGEMENTS

Sanko Robinson and FunkyMonk helped me with porting this module to Windows.

HIROSE Masaake fixed handling of commands containing space (like "CONFIG GET")

=head1 AUTHOR

Pavel Shaydo, C<< <zwon at cpan.org> >>

=head1 LICENSE AND COPYRIGHT

Copyright 2011-2013 Pavel Shaydo.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut
