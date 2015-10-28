use Test::Most 0.22;
use Test::RedisDB;
use RedisDB;

# This test is derived from basic_redis to preserve
# tests for deprecated send_command_cb method

my $server = Test::RedisDB->new;
plan( skip_all => "Can't start redis-server" ) unless $server;
my $redis = $server->redisdb_client;

# Test callbacks
my @replies;
sub cb {
    shift;
    push @replies, [ @_ ];
}

$redis->send_command_cb('SET', 'CB A', 'AAA');
$redis->send_command_cb('GET', 'CB A', \&cb);
$redis->send_command_cb('SET', 'CB B', 'BBB');
$redis->send_command_cb('GET', 'CB B', \&cb);
is $redis->ping, "PONG", "can execute while sent some commands with callbacks";
$redis->send_command_cb('RPUSH', 'CB LIST', 'CCC');
$redis->send_command_cb('RPUSH', 'CB LIST', 'DDD');
$redis->send_command('RPUSH', 'CB LIST', 'EEE');
$redis->send_command_cb('LRANGE', 'CB LIST', 0, 3, sub { my $redis = shift; cb($redis, 'LIST', @_) } );
is $redis->replies_to_fetch, 1, "One reply to fetch";
is $redis->get_reply, 3, "Added 3 elements to the list";
$redis->send_command('PING');
is $redis->get_reply, 'PONG', "Pinged server";
eq_or_diff \@replies,
  [ [qw(AAA)], [qw(BBB)], [ 'LIST', [qw(CCC DDD EEE)] ], ],
  "Callback was called with correct arguments";
is $redis->replies_to_fetch, 0, "No replies to fetch";

done_testing;
