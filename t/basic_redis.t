use Test::Most 0.22;
use lib 't';
use RedisServer;
use RedisDB;

my $server = RedisServer->start;
plan( skip_all => "Can't start redis-server" ) unless $server;
plan('no_plan');
my $redis = RedisDB->new( host => 'localhost', port => $server->{port} );
diag("Testing against redis-server " . $redis->info->{redis_version});
$redis->send_command('PING');
my $res = $redis->get_reply;
eq_or_diff $res, 'PONG', "Got PONG";

$redis->send_command( 'SET', 'test string', 'test value' );
$res = $redis->get_reply;
eq_or_diff $res, 'OK', "Set test string";
$redis->send_command( 'GET', 'test string' );
$res = $redis->get_reply;
eq_or_diff $res, 'test value', "Got test string";
$redis->send_command( 'GET', 'test non-existing string' );
$res = $redis->get_reply;
eq_or_diff $res, undef, "Got undef for non-existing string";

$redis->send_command( 'INCR', 'counter' );
$res = $redis->get_reply;
eq_or_diff $res, 1, "counter value is 1";
$redis->send_command( 'INCRBY', 'counter', 10 );
$res = $redis->get_reply;
eq_or_diff $res, 11, "counter value now is 11";

# Send multiple commands without reading reply
for (qw(this is a list to test LRANGE)) {
    $redis->send_command( 'RPUSH', 'test list', $_ );
}

# Read replies
for ( 1 .. 7 ) {
    $res = $redis->get_reply;
    eq_or_diff $res, $_, "List length is $_";
}

$redis->send_command( 'LRANGE', 'test list', 1, 3 );
$res = $redis->get_reply;
eq_or_diff $res, [qw(is a list)], 'LRANGE returned correct result';

is $redis->quit, "OK", "QUIT";

$server->stop;
