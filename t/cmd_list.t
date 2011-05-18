use Test::Most 0.22;
use lib 't';
use RedisServer;
use RedisDB;

my $server = RedisServer->start;
plan( skip_all => "Can't start redis-server" ) unless $server;
plan('no_plan');

my $redis = RedisDB->new( host => 'localhost', port => $server->{port} );

is $redis->llen("list1"), 0, "LLEN of empty list is 0";
is $redis->rpush("list1", "V1"), 1, "RPUSH";
is $redis->lpush("list1", "V2"), 2, "LPUSH";
is $redis->rpop("list1"), "V1", "RPOP";
for (qw(V3 V4 V5 V6)) {
    $redis->rpush("list1", $_);
}
is $redis->llen("list1"), 5, "LLEN";
is $redis->lindex("list1", 33), undef, "LINDEX for out of range value";
is $redis->lindex("list1", 1), "V3", "LINDEX";
is $redis->lpop("list1"), "V2", "LPOP";
eq_or_diff $redis->lrange("list1", 1, -2), [qw(V4 V5)], "LRANGE";
is $redis->lrem("list1", 0, "V5"), 1, "LREM";
is $redis->lset("list1", 2, "VVI"), "OK", "LSET";
eq_or_diff $redis->lrange("list1", 0, -1), [qw(V3 V4 VVI)], "LRANGE";
for (qw(V7 V8 V9 V0)) {
    $redis->rpush("list1", $_);
}
is $redis->ltrim("list1", 2, -3), "OK", "LTRIM";
eq_or_diff $redis->lrange("list1", 0, -1), [qw(VVI V7 V8)], "LTREAM result is correct";

if($redis->version >= 1.001) {
    is $redis->rpoplpush("list1", "list2"), "V8", "RPOPLPUSH";
    is $redis->llen("list1"), 2, "list1 len is 2";

    if($redis->version >= 1.003001) {
        eq_or_diff $redis->blpop("list1", "list2", 0), [qw(list1 VVI)], "BLPOP";
        eq_or_diff $redis->brpop("list2", "list1", 0), [qw(list2 V8)], "BRPOP";

        if($redis->version >= 2.001001) {
            is $redis->linsert("list1", "BEFORE", "V7", "V1"), 2, "LINSERT BEFORE";
            is $redis->linsert("list1", "AFTER", "V1", "V3"), 3, "LINSERT AFTER";
            is $redis->rpushx("list1", "V8"), 4, "RPUSHX";
            is $redis->lpushx("list3", "V0"), 0, "LPUSHX";
            eq_or_diff $redis->lrange("list1", 0, -1), [qw(V1 V3 V7 V8)], "list1 contains expected values";

            if($redis->version >= 2.001007) {
                is $redis->brpoplpush("list1", "list3", 0), "V8", "BRPOPLPUSH";
            }
        }
    }
}
