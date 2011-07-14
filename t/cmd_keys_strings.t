use Test::Most 0.22;
use lib 't';
use RedisServer;
use RedisDB;

my $server = RedisServer->start;
plan( skip_all => "Can't start redis-server" ) unless $server;
plan('no_plan');

my $redis = RedisDB->new( host => 'localhost', port => $server->{port} );

$redis->flushdb;
is $redis->set( 'mykey1', 'myvalue1' ), "OK", "SET mykey1";
is $redis->getset( 'mykey1', 'my new value' ), "myvalue1", "GETSET";
is $redis->exists("mykey1"),  1, "EXISTS";
is $redis->exists("mykey 1"), 0, "not EXISTS";
is $redis->setnx( "mykey1", "new value" ), 0, "SETNX (key exists)";
is $redis->rename( "mykey1", "my first key" ), "OK", "RENAME";
is $redis->get("my first key"), "my new value", "GET my new value";
if ( $redis->version >= 1.003003 ) {
    my $expected = "my new value with appendix";
    is $redis->append( "my first key", " with appendix" ), length($expected), "APPEND";
    is $redis->get("my first key"), $expected, "GOT value with appendix";
    if ( $redis->version >= 2.001002 ) {
        is $redis->strlen("my first key"), length($expected), "STRLEN";
    }
}

is $redis->set( "delme", 123 ), "OK", "SET delme";
is $redis->exists("delme"), 1, "delme exists";
is $redis->del("delme"),    1, "DEL delme";
is $redis->exists("delme"), 0, "delme doesn't exist";

is $redis->incr("counter"), 1, "INCR";
is $redis->incrby( "counter", 77 ), 78, "INCRBY 77";
is $redis->decr("counter"), 77, "DECR";
is $redis->decrby( "counter", 35 ), 42, "DECRBY 35";

eq_or_diff [ sort @{ $redis->keys('*') } ], [ sort "my first key", "counter" ], "KEYS";

if ( $redis->version >= 2.001008 ) {
    is $redis->set( "bits", chr(0x55) ), "OK", "set key to 0x55";
    is $redis->getbit( "bits", 0 ), 0, "GETBIT 0";
    is $redis->getbit( "bits", 1 ), 1, "GETBIT 1";
    is $redis->setbit( "bits", 2, 1 ), 0, "SETBIT 2";
    is $redis->getbit( "bits", 2 ), 1, "GETBIT 2";
    $redis->set( "range_test", "test getrange" );
    is $redis->getrange( "range_test", 5, -1 ), "getrange", "GETRANGE";
    is $redis->setrange( "range_test", 5, "set" ), 13, "SETRANGE";
    is $redis->get("range_test"), "test setrange", "SETRANGE result is correct";
}

if ( $redis->version >= 1.001 ) {
    is $redis->mset( aaa => 1, bbb => 2, ccc => 3 ), "OK", "MSET";
    is $redis->msetnx( ddd => 4, eee => 5, fff => 6 ), 1, "MSETNX 1";
    is $redis->msetnx( fff => 7, ggg => 8, hhh => 9 ), 0, "MSETNX 0";
    eq_or_diff $redis->mget(qw(aaa bbb eee fff hhh)), [ qw(1 2 5 6), undef ], "MGET";
    is $redis->renamenx( eee => 'iii' ), 1, "RENAMENX 1";
    is $redis->renamenx( ddd => 'fff' ), 0, "RENAMENX 0";
    eq_or_diff $redis->mget(qw(eee iii ddd fff)), [ undef, qw(5 4 6) ], "RENAMENX works correctly";
}

if ( $redis->version >= 2.001002 ) {
    is $redis->setex( "expires", 2, "in two seconds" ), "OK", "SETEX";
    ok $redis->ttl("expires") > 0, "TTL >0";
    $redis->set( "persistent", "value" );
    is $redis->ttl("persistent"), -1, "TTL -1";
    is $redis->expire( "persistent", 100 ), 1, "EXPIRE";
    ok $redis->ttl("persistent") > 98, "TTL >98";
    is $redis->expireat( "persistent", time + 10 ), 1, "EXPIREAT";
    my $ttl = $redis->ttl("persistent");
    ok( $ttl <= 10 && $ttl > 8, "Correct TTL" );
    is $redis->persist("persistent"), 1,  "PERSIST";
    is $redis->ttl("persistent"),     -1, "key will persist";
    sleep 3;
    is $redis->exists("expires"), 0, "expired key was deleted";
}

$redis->shutdown;
$server->stop;
