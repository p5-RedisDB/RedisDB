package RedisDB;

use warnings;
use strict;
our $VERSION = 0.02;

use IO::Socket::INET;
use Socket qw(MSG_DONTWAIT);
use POSIX qw(:errno_h);
use Carp;

=head1 NAME

RedisDB - Module to access redis database

=head1 SYNOPSIS

    use RedisDB;

    my $redis = RedisDB->new(host => 'localhost', port => 6379);
    $redis->set($key, $value);
    my $value = $redis->get($key);

=head1 DESCRIPTION

B<This is alfa version, use on your own risk, interface is subject to change>

This module provides interface to access redis database. It transparently
handles disconnects and forks.

=head1 METHODS

=head2 $class->new(%options)

Creates new RedisDB object. The following options are allowed:

=over 4

=item host

domain name of the host running redis server. Default: "localhost"

=item port

port to connect. Default: 6379

=item lazy

by default I<new> establishes connection to the server. If this parameter is
set, then connection will be established when you will send command to the
server.

=back

=cut

sub new {
    my $class = shift;
    my $self = ref $_[0] ? $_[0] : {@_};
    bless $self, $class;
    $self->{port} ||= 6379;
    $self->{host} ||= 'localhost';
    $self->connect unless $self->{lazy};
    return $self;
}

=head2 $self->execute($command, @arguments)

send command to the server and return server reply. It throws exception if
server returns error.

=cut

sub execute {
    my $self = shift;
    $_[0] = uc $_[0];
    $self->send_command(@_);
    my ( $type, $value ) = $self->recv_reply;
    croak $value if $type eq '-';
    return $value;
}

=head2 $self->connect

establish connection to the server. There's usually no need to use this
method, as connection is established by new or when you send a command.

=cut

sub connect {
    my $self = shift;
    $self->{_pid}    = $$;
    $self->{_socket} = IO::Socket::INET->new(
        PeerAddr => $self->{host},
        PeerPort => $self->{port},
        Proto    => 'tcp'
    ) or die "Can't connect to redis server $self->{host}:$self->{port}: $!";
    $self->{_commands_in_flight} = 0;
    return 1;
}

=head2 $self->send_command($command, @arguments)

send command to the server. Returns true if command was successfully sent, or
dies if error occured. Note, that it doesn't return server reply, you should
retrieve reply using I<recv_reply> method.

=cut

sub send_command {
    my $self    = shift;
    my $request = _build_redis_request(@_);
    $self->connect unless $self->{_socket} and $self->{_pid} == $$;
    local $SIG{PIPE} = 'IGNORE';

    # Here we reading received data and storing it in the _buffer,
    # but the main purpose is to check if connection is still alive
    # and reconnect if not
    while (1) {
        my $ret = $self->{_socket}->recv( my $buf, 4096, MSG_DONTWAIT );
        unless ( defined $ret ) {

            # socket is connected, no data in recv buffer
            last if $! == EAGAIN or $! == EWOULDBLOCK;
            next if $! == EINTR;

            # not handling this error
            die "Error reading from server: $!";
        }
        elsif ( $buf ne '' ) {

            # received some data
            $self->{_buffer} .= $buf;
        }
        else {

            # disconnected, try to reconnect
            $self->{warnings} and warn "Disconnected, trying to reconnect";
            $self->connect;
            last;
        }
    }

    $self->{debug} and warn "Sending request";
    defined $self->{_socket}->send($request) or die "Can't send request to server: $!";
    $self->{_commands_in_flight}++;
    return 1;
}

=head2 $self->recv_reply

receive reply from the server. Method returns two elements array. First element
is the character depending on type of reply: "+" one line reply, "-" error, ":"
integer reply, "$" bulk reply, "*" multi-bulk reply. Second element is reply
itself, which is scalar for all replies, except that it is array reference for
multi-bulk.

=cut

sub recv_reply {
    my $self = shift;
    die "We are not waiting for reply" unless $self->{_commands_in_flight};
    die "You can't read reply in child process" unless $self->{_pid} == $$;
    my @reply;
    while ( not @reply = $self->_parse_reply ) {
        my $ret = $self->{_socket}->recv( my $buffer, 4096 );
        $self->{debug} and warn "Received: $buffer";
        unless ( defined $ret ) {
            next if $! == EINTR;
            die "Error reading reply from server: $!";
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
    $self->{debug} and warn "Reply: $reply[0], $reply[1]";
    $self->{_commands_in_flight}--;
    return @reply;
}

my @commands = qw(
  append	auth	bgrewriteaof	bgsave	blpop	brpoplpush	config_get
  config_set	config_resetstat	dbsize	debug_object	debug_segfault
  decr	decrby	del	echo	exists	expire	expireat	flushall
  flushdb	get	getbit	getrange	getset	hdel	hexists	hget	hgetall
  hincrby	hkeys	hlen	hmget	hmset	hset	hsetnx	hvals	incr	incrby
  info	keys	lastsave	lindex	linsert	llen	lpop	lpush	lpushx
  lrange	lrem	lset	ltrim	mget	move	mset	msetnx	persist	ping
  publish	quit	randomkey	rename	renamenx	rpop	rpoplpush
  rpush	rpushx	sadd	save	scard	sdiff	sdiffstore	select	set
  setbit	setex	setnx	setrange	shutdown	sinter	sinterstore
  sismember	slaveof	smembers	smove	sort	spop	srandmember
  srem	strlen	sunion	sunionstore	sync	ttl	type	zadd	zcard
  zcount	zincrby	zinterstore	zrange	zrangebyscore	zrank	zremrangebyrank
  zremrangebyscore	zrevrange	zrevrangebyscore	zrevrank
  zscore	zunionstore
);

=head1 SUPPORTED REDIS COMMANDS

Usually, instead of using I<send_command> and I<recv_reply> methods, you can just use functions corresponding to redis commands:
append	auth	bgrewriteaof	bgsave	blpop	brpoplpush	config_get
config_set	config_resetstat	dbsize	debug_object	debug_segfault
decr	decrby	del	echo	exists	expire	expireat	flushall
flushdb	get	getbit	getrange	getset	hdel	hexists	hget	hgetall
hincrby	hkeys	hlen	hmget	hmset	hset	hsetnx	hvals	incr	incrby
info	keys	lastsave	lindex	linsert	llen	lpop	lpush	lpushx
lrange	lrem	lset	ltrim	mget	move	mset	msetnx	persist	ping
publish	quit	randomkey	rename	renamenx	rpop	rpoplpush
rpush	rpushx	sadd	save	scard	sdiff	sdiffstore	select	set
setbit	setex	setnx	setrange	shutdown	sinter	sinterstore
sismember	slaveof	smembers	smove	sort	spop	srandmember
srem	strlen	sunion	sunionstore	sync	ttl	type	zadd	zcard
zcount	zincrby	zinterstore	zrange	zrangebyscore	zrank	zremrangebyrank
zremrangebyscore	zrevrange	zrevrangebyscore	zrevrank
zscore	zunionstore

See description of these commands in redis documentation at L<http://redis.io/commands>.

=cut

for my $command (@commands) {
    my $uccom = uc $command;
    $uccom =~ s/_/ /g;
    no strict 'refs';
    *{ __PACKAGE__ . "::$command" } = sub {
        my $self = shift;
        return $self->execute( $uccom, @_ );
    };
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
# This method expect some data in the $self->{_buffer}. If buffer contains
# full request, then method returns array with reply type as first element
# and the data as the rest of array
my ( $READ_LINE, $READ_NUMBER, $READ_BULK_LEN, $READ_BULK, $READ_MBLK_LEN, $WAIT_BUCKS ) = 1 .. 6;

sub _parse_reply {
    my $self = shift;
    return unless $self->{_buffer};

    # if we not yet started parsing reply
    unless ( $self->{_parse_state} ) {
        my $type = substr( $self->{_buffer}, 0, 1, '' );
        $self->{_parse_reply} = [$type];
        if ( $type =~ /[+-]/ ) {
            $self->{_parse_state} = $READ_LINE;
        }
        elsif ( $type eq ':' ) {
            $self->{_parse_state} = $READ_NUMBER;
        }
        elsif ( $type eq '$' ) {
            $self->{_parse_state} = $READ_BULK_LEN;
        }
        elsif ( $type eq '*' ) {
            $self->{_parse_state} = $READ_MBLK_LEN;
        }
        else {
            die "Got invalid reply: $type$self->{_buffer}";
        }
    }

    # parse data
    my $repeat = 1;
    while ($repeat) {
        $repeat = 0;
        return unless length $self->{_buffer} >= 2;
        if ( $self->{_parse_state} == $READ_LINE ) {
            if ( defined( my $line = $self->_read_line ) ) {
                $self->{_parse_reply}[1] = $line;
                return $self->_reply_completed;
            }
        }
        elsif ( $self->{_parse_state} == $READ_NUMBER ) {
            if ( defined( my $line = $self->_read_line ) ) {
                die "Received invalid integer reply :$line" unless $line =~ /^-?[0-9]+$/;
                $self->{_parse_reply}[1] = $line;
                return $self->_reply_completed;
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
                        push @{ $self->{_parse_reply}[1] }, undef;
                        if ( --$self->{_parse_mblk_len} ) {
                            $self->{_parse_state} = $WAIT_BUCKS;
                            $repeat = 1;
                        }
                        else {
                            return $self->_reply_completed;
                        }
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
                push @{ $self->{_parse_reply}[1] }, $bulk;
                if ( --$self->{_parse_mblk_len} ) {
                    $self->{_parse_state} = $WAIT_BUCKS;
                    $repeat = 1;
                }
                else {
                    return $self->_reply_completed;
                }
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
                elsif ( $len == 0 ) {
                    $self->{_parse_reply}[1] = [];
                    return $self->_reply_completed;
                }
                elsif ( $len == -1 ) {
                    $self->{_parse_reply}[1] = undef;
                    return $self->_reply_completed;
                }
                else {
                    die "Invalid multi-bulk reply: *$len\015\012$self->{_buffer}";
                }
            }
        }
        elsif ( $self->{_parse_state} == $WAIT_BUCKS ) {
            die "Invalid multi-bulk reply. Expected '\$'"
              unless substr( $self->{_buffer}, 0, 1, '' ) eq '$';
            $self->{_parse_state} = $READ_BULK_LEN;
            $repeat = 1;
        }
    }
    return;
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

sub _reply_completed {
    my $self = shift;
    $self->{_parse_state} = undef;
    my $rep = $self->{_parse_reply};
    $self->{_parse_reply} = undef;
    return @$rep;
}

1;

__END__

=head1 SEE ALSO

L<Redis>, L<Redis::hiredis>, L<AnyEvent::Redis>

=head1 TODO

=over 4

=item *

Test all commands

=item *

Better pipelining support

=item *

User defined error callback

=item *

Handle cases than client not interested in replies

=item *

Transactions support (MULTI, EXEC, DISCARD, WATCH, UNWATCH)

=item *

Subscriptions support (PSUBSCRIBE, PUNSUBSCRIBE, SUBSCRIBE, UNSUBSCRIBE)

=item *

MONITOR support

=item *

Non-blocking check if reply available

=back

=head1 BUGS

Please report any bugs or feature requests to C<bug-redisdb at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=RedisDB>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.

=head1 AUTHOR

Pavel Shaydo, C<< <zwon at cpan.org> >>

=head1 LICENSE AND COPYRIGHT

Copyright 2011 Pavel Shaydo.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut
