use Test::Most 0.22;
use lib 't';
use RedisServer;
use RedisDB;

my $server = RedisServer->start;
plan( skip_all => "Can't start redis-server" ) unless $server;
plan('no_plan');

my $redis = RedisDB->new( host => 'localhost', port => $server->{port} );

is $redis->multi, 'OK', "Entered transaction";
is $redis->set("key", "value"), 'QUEUED', "Queued set";
for (qw(this is a list)) {
    is $redis->rpush("list", $_), 'QUEUED', "Queued push '$_'";
}
is $redis->lrange("list", 0, 3), 'QUEUED', "Queued lrange";
my $redis2 = RedisDB->new( host => 'localhost', port => $server->{port} );
is $redis2->set("key", "wrong value"), "OK", "Set key to wrong value";
my $res = $redis->exec;
eq_or_diff $res, [qw(OK 1 2 3 4), [qw(this is a list)]], "Transaction was successfull";
is $redis->get("key"), "value", "key set to correct value";

is $redis->watch("watch"), "OK", "watch for watch";
is $redis->multi, 'OK', "Entered transaction";
dies_ok { $redis->multi } "multi can't be nested";
is $redis->set("key", "another value"), "QUEUED", "QUEUED set";
is $redis2->set("watch", "changed"), "OK", "Set watched key";
is $redis->exec, undef, "Transaction failed";
is $redis->get("key"), "value", "key wasn't changed";

is $redis->multi, "OK", "Entered transaction";
is $redis->set("key", "another value"), "QUEUED", "QUEUED set";
is $redis->discard, "OK", "Discarded transaction";
is $redis->get("key"), "value", "key wasn't changed";
