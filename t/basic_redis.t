use Test::Most;
use lib 't';
use RedisServer;
use RedisDB;

my $server = RedisServer->start;
plan( skip_all => "Can't start redis-server" ) unless $server;
plan('no_plan');
my $redis = RedisDB->new( host => 'localhost', port => $server->{port} );
$redis->send_command('PING');
my @res = $redis->recv_reply;
eq_or_diff \@res, [ '+', 'PONG' ], "Got PONG";

$redis->send_command( 'SET', 'test string', 'test value' );
@res = $redis->recv_reply;
eq_or_diff \@res, [ '+', 'OK' ], "Set test string";
$redis->send_command( 'GET', 'test string' );
@res = $redis->recv_reply;
eq_or_diff \@res, [ '$', 'test value' ], "Got test string";
$redis->send_command( 'GET', 'test non-existing string' );
@res = $redis->recv_reply;
is_deeply \@res, [ '$', undef ], "Got undef for non-existing string";

$redis->send_command( 'INCR', 'counter' );
@res = $redis->recv_reply;
eq_or_diff \@res, [ ':', 1 ], "counter value is 1";
$redis->send_command( 'INCRBY', 'counter', 10 );
@res = $redis->recv_reply;
eq_or_diff \@res, [ ':', 11 ], "counter value now is 11";

# Send multiple commands without reading reply
for (qw(this is a list to test LRANGE)) {
    $redis->send_command( 'RPUSH', 'test list', $_ );
}

# Read replies
for ( 1 .. 7 ) {
    @res = $redis->recv_reply;
    eq_or_diff \@res, [ ':', $_ ], "List length is $_";
}

$redis->send_command( 'LRANGE', 'test list', 1, 3 );
@res = $redis->recv_reply;
eq_or_diff \@res, [ '*', [qw(is a list)] ], 'LRANGE returned correct result';

$server->stop;
