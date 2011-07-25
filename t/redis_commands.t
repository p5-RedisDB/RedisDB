use Test::Most 0.22;
use lib 't';
use RedisServer;
use RedisDB;

my $server = RedisServer->start;
plan( skip_all => "Can't start redis-server" ) unless $server;
plan('no_plan');

my $redis = RedisDB->new( host => 'localhost', port => $server->{port} );

subtest "Keys and strings commands" => \&cmd_keys_strings;
subtest "Lists commands"            => \&cmd_lists;
subtest "Hashes commands"           => \&cmd_hashes;
subtest "Server info commands"      => \&cmd_server;

sub cmd_keys_strings {
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
        eq_or_diff $redis->mget(qw(eee iii ddd fff)), [ undef, qw(5 4 6) ],
          "RENAMENX works correctly";
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
}

sub cmd_lists {
    $redis->flushdb;
    is $redis->llen("list1"), 0, "LLEN of empty list is 0";
    is $redis->rpush( "list1", "V1" ), 1, "RPUSH";
    is $redis->lpush( "list1", "V2" ), 2, "LPUSH";
    is $redis->rpop("list1"), "V1", "RPOP";
    for (qw(V3 V4 V5 V6)) {
        $redis->rpush( "list1", $_ );
    }
    is $redis->llen("list1"), 5, "LLEN";
    is $redis->lindex( "list1", 33 ), undef, "LINDEX for out of range value";
    is $redis->lindex( "list1", 1 ),  "V3",  "LINDEX";
    is $redis->lpop("list1"), "V2", "LPOP";
    eq_or_diff $redis->lrange( "list1", 1, -2 ), [qw(V4 V5)], "LRANGE";
    is $redis->lrem( "list1", 0, "V5" ), 1, "LREM";
    is $redis->lset( "list1", 2, "VVI" ), "OK", "LSET";
    eq_or_diff $redis->lrange( "list1", 0, -1 ), [qw(V3 V4 VVI)], "LRANGE";

    for (qw(V7 V8 V9 V0)) {
        $redis->rpush( "list1", $_ );
    }
    is $redis->ltrim( "list1", 2, -3 ), "OK", "LTRIM";
    eq_or_diff $redis->lrange( "list1", 0, -1 ), [qw(VVI V7 V8)], "LTREAM result is correct";

    if ( $redis->version >= 1.001 ) {
        is $redis->rpoplpush( "list1", "list2" ), "V8", "RPOPLPUSH";
        is $redis->llen("list1"), 2, "list1 len is 2";

        if ( $redis->version >= 1.003001 ) {
            eq_or_diff $redis->blpop( "list1", "list2", 0 ), [qw(list1 VVI)], "BLPOP";
            eq_or_diff $redis->brpop( "list2", "list1", 0 ), [qw(list2 V8)], "BRPOP";

            if ( $redis->version >= 2.001001 ) {
                is $redis->linsert( "list1", "BEFORE", "V7", "V1" ), 2, "LINSERT BEFORE";
                is $redis->linsert( "list1", "AFTER",  "V1", "V3" ), 3, "LINSERT AFTER";
                is $redis->rpushx( "list1", "V8" ), 4, "RPUSHX";
                is $redis->lpushx( "list3", "V0" ), 0, "LPUSHX";
                eq_or_diff $redis->lrange( "list1", 0, -1 ), [qw(V1 V3 V7 V8)],
                  "list1 contains expected values";

                if ( $redis->version >= 2.001007 ) {
                    is $redis->brpoplpush( "list1", "list3", 0 ), "V8", "BRPOPLPUSH";
                }
            }
        }
    }
}

sub cmd_hashes {
    $redis->flushdb;
    is $redis->hset( 'thash', keyb => 'value b' ), 1, "HSET new key";
    is $redis->hset( 'thash', keyb => 'valueb' ),  0, "HSET updated key";
    is $redis->hsetnx( 'thash', keya => 'valuea' ), 1, "HSETNX new key";
    is $redis->hsetnx( 'thash', keyb => 'valueb' ), 0, "HSETNX existing key";
    is $redis->hmset( 'thash', keyc => 'valuec', keyd => 'valued' ), 'OK', "HMSET";

    is $redis->hexists( 'thash', 'counter' ), 0, "HEXISTS == 0";
    is $redis->hincrby( 'thash', counter => 3 ), 3, "HINCRBY new key";
    is $redis->hincrby( 'thash', counter => 4 ), 7, "HINCRBY existing key";
    is $redis->hexists( 'thash', 'counter' ), 1, "HEXISTS == 1";
    is $redis->hget( 'thash', 'counter' ), 7, "HGET";
    is $redis->hdel( 'thash', 'counter' ), 1, "HDEL";

    my %thash = @{ $redis->hgetall('thash') };
    my @thash = map { $_ => $thash{$_} } sort keys %thash;
    eq_or_diff \@thash, [qw(keya valuea keyb valueb keyc valuec keyd valued)], "HGETALL";
    eq_or_diff [ sort @{ $redis->hkeys('thash') } ], [qw(keya keyb keyc keyd)],         "HKEYS";
    eq_or_diff [ sort @{ $redis->hvals('thash') } ], [qw(valuea valueb valuec valued)], "HVALS";
    is $redis->hlen('thash'), 4, "HLEN";
    eq_or_diff $redis->hmget( 'thash', qw(keyb counter keya) ), [ 'valueb', undef, 'valuea' ],
      'HMGET';
}

sub cmd_server {
    my $info = $redis->info;
    is ref($info), "HASH", "Got hashref from info";
    ok exists $info->{redis_version}, "There's redis_version in the hash";
    $info->{redis_version} =~ /^([0-9]+)[.]([0-9]+)(?:[.]([0-9]+))?/;
    ok( ( $1 and $2 ), "Looks like a version" );
    my $version = 0 + $1 + 0.001 * $2 + ( $3 ? 0.000001 * $3 : 0 );
    is '' . $redis->version, "$version", "Correct server version: $version";
}

$redis->shutdown;
