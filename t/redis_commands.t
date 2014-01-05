use Test::Most 0.22;
use lib 't';
use RedisServer;
use RedisDB;
use Digest::SHA qw(sha1_hex);
use Time::HiRes qw(usleep);
use Scalar::Util qw(blessed);

my $server = RedisServer->start;
plan( skip_all => "Can't start redis-server" ) unless $server;

my $redis = RedisDB->new( host => 'localhost', port => $server->{port} );
plan( skip_all => "Test requires redis-server at least 1.2" ) unless $redis->version ge 1.003015;

subtest "Keys and strings commands" => \&cmd_keys_strings;
subtest "Scan commands"             => \&cmd_scan;
subtest "Lists commands"            => \&cmd_lists;
subtest "Hashes commands"           => \&cmd_hashes;
subtest "Server info commands"      => \&cmd_server;
subtest "Sets commands"             => \&cmd_sets;
subtest "Ordered sets commands"     => \&cmd_zsets;
subtest "Scripts"                   => \&cmd_scripts;

sub group_pairs {
    my @res;
    while (@_) {
        push @res, [ shift, shift ];
    }
    return @res;
}

sub cmd_keys_strings {
    $redis->flushdb;
    is $redis->set( 'mykey1', 'myvalue1' ), "OK", "SET mykey1";
    is $redis->getset( 'mykey1', 'my new value' ), "myvalue1", "GETSET";
    is $redis->exists("mykey1"),  1, "EXISTS";
    is $redis->exists("mykey 1"), 0, "not EXISTS";
    is $redis->setnx( "mykey1", "new value" ), 0, "SETNX (key exists)";
    is $redis->rename( "mykey1", "my first key" ), "OK", "RENAME";
    is $redis->get("my first key"), "my new value", "GET my new value";

    my $expected = "my new value with appendix";
    is $redis->append( "my first key", " with appendix" ), length($expected), "APPEND";
    is $redis->get("my first key"), $expected, "GOT value with appendix";
    if ( $redis->version >= 2.001002 ) {
        is $redis->strlen("my first key"), length($expected), "STRLEN";
    }

    is $redis->set( "delme", 123 ), "OK", "SET delme";
    is $redis->exists("delme"), 1, "delme exists";
    is $redis->del("delme"),    1, "DEL delme";
    is $redis->exists("delme"), 0, "delme doesn't exist";

    is $redis->incr("counter"), 1, "INCR";
    is $redis->incrby( "counter", 77 ), 78, "INCRBY 77";
    is $redis->decr("counter"), 77, "DECR";
    is $redis->decrby( "counter", 35 ), 42, "DECRBY 35";
    if ( $redis->version > 2.005 ) {
        ok abs( $redis->incrbyfloat( "counter", -2.5 ) - 39.5 ) < 1e-7, "INCRBYFLOAT";
    }

    eq_or_diff [ sort @{ $redis->keys('*') } ], [ sort "my first key", "counter" ], "KEYS";

    if ( $redis->version >= 2.001008 ) {
        is $redis->set( "bits", chr(0x55) ), "OK", "set key to 0x55";
        is $redis->getbit( "bits", 0 ), 0, "GETBIT 0";
        is $redis->getbit( "bits", 1 ), 1, "GETBIT 1";
        is $redis->setbit( "bits", 2, 1 ), 0, "SETBIT 2";
        is $redis->getbit( "bits", 2 ), 1, "GETBIT 2";
        if ( $redis->version >= 2.006 ) {
            is $redis->bitcount("bits"), 5, "BITCOUNT";
            is $redis->bitop( "NOT", "bits", "bits" ), 1, "BITOP NOT";
            is $redis->bitcount("bits"), 3, "BITCOUNT";
            is $redis->set( "bits1", "\x75" ),     "OK", "set bits1 to \\x75";
            is $redis->set( "bits2", "\000\x55" ), "OK", "set bits2 to \\000\\x55";
            is $redis->bitop( "OR", "bits3", "bits1", "bits2" ), 2, "BITOP OR";
            is $redis->get("bits3"), "\x75\x55", "bits3 == bits1 | bits2";
            is $redis->set( "bits4", "\xf0\xf0" ), "OK", "set bits4 to \\xf0\\xf0";
            is $redis->bitop( "AND", "bits5", "bits3", "bits4" ), 2, "BITOP AND";
            is $redis->get("bits5"), "\x70\x50", "bits5 == bits3 & bits4";
            is $redis->bitop( "XOR", "bits6", "bits3", "bits4" ), 2, "BITOP XOR";
            is $redis->get("bits6"), "\x85\xa5", "bits6 == bits3 ^ bits4";
        }
        else {
            diag "Skipped tests for redis >= 2.6";
        }
        $redis->set( "range_test", "test getrange" );
        is $redis->getrange( "range_test", 5, -1 ), "getrange", "GETRANGE";
        is $redis->setrange( "range_test", 5, "set" ), 13, "SETRANGE";
        is $redis->get("range_test"), "test setrange", "SETRANGE result is correct";
    }
    else {
        diag "Skipped tests for redis >= 2.1.8";
    }

    is $redis->mset( aaa => 1, bbb => 2, ccc => 3 ), "OK", "MSET";
    is $redis->msetnx( ddd => 4, eee => 5, fff => 6 ), 1, "MSETNX 1";
    is $redis->msetnx( fff => 7, ggg => 8, hhh => 9 ), 0, "MSETNX 0";
    eq_or_diff $redis->mget(qw(aaa bbb eee fff hhh)), [ qw(1 2 5 6), undef ], "MGET";
    is $redis->renamenx( eee => 'iii' ), 1, "RENAMENX 1";
    is $redis->renamenx( ddd => 'fff' ), 0, "RENAMENX 0";
    eq_or_diff $redis->mget(qw(eee iii ddd fff)), [ undef, qw(5 4 6) ], "RENAMENX works correctly";

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

    if ( $redis->version >= 2.005 ) {
        is $redis->setex( "pexpires", 10, "in two seconds" ), "OK", "SETEX";
        ok $redis->pttl("pexpires") > 1000, "PTTL > 1000";
        is $redis->pexpire( "pexpires", 9000 ), 1, "PEXPIRE";
        ok $redis->ttl("pexpires") < 10, "TTL < 10";
        is $redis->psetex( "pexpires", 20_000, "20 seconds" ), "OK", "PSETEX";
        ok $redis->ttl("pexpires") > 10, "TTL > 10";
        is $redis->pexpireat( "pexpires", time * 1000 + 1100 ), 1, "PEXPIREAT";
        usleep 1_200_000;
        is $redis->get("pexpires"), undef, "expired";
    }

    if ( $redis->version >= 2.002003 ) {
        is $redis->set(qw(object test)), "OK", "Set object";
        is $redis->object_refcount("object"), 1,     "OBJECT REFCOUNT";
        is $redis->object_encoding("object"), "raw", "OBJECT ENCODING";
        my $idle = $redis->object_idletime("object");
        ok $idle >= 0 && $idle < 11, "OBJECT IDLETIME";
    }

    if ( $redis->version >= 2.005011 ) {
        is $redis->set(qw(dump test)), "OK", "Set dump";
        my $dump = $redis->dump("dump");
        ok $dump, "DUMP";
        $redis->del("dump");
        is $redis->restore( "dump", 0, $dump ), "OK", "RESTORE";
        is $redis->get("dump"), "test", "Restored";
    }
}

sub cmd_scan {
    plan skip_all => "testing SCAN requires redis 2.8.0" if $redis->version < 2.008;
    $redis->flushdb;
    my @all_keys;
    for ( 1 .. 40 ) {
        $redis->set( "key$_", $_, RedisDB::IGNORE_REPLY );
        push @all_keys, "key$_";
    }
    eq_or_diff [ sort @{ $redis->keys('*') } ], [ sort @all_keys ],
      "KEYS returned expected list of keys";
    my ( $cnt, $cursor ) = ( 0, 0 );
    my @keys;
    while ( $cnt++ < 5 ) {
        my $res = $redis->scan( $cursor, 'COUNT', 20 );
        fail "SCAN returned an error: $res" if blessed $res;
        $cursor = $res->[0];
        push @keys, @{ $res->[1] };
        last unless $cursor;
    }
    fail "Haven't scanned all the keys after $cnt iterations" if $cnt > 5;
    eq_or_diff [ sort @keys ], [ sort @all_keys ], "SCAN returned all expected keys";

    is $redis->hmset( "test_hash", map { ( $_, "${_}value" ) } @all_keys ), "OK",
      "initialized hash with HMSET";
    my $hscan = $redis->hscan( "test_hash", 0, "MATCH", "*4", "COUNT", 100 );
    is $hscan->[0], 0, "Got all matching keys in a single HSCAN call";
    eq_or_diff [ sort { $a->[0] cmp $b->[0] } group_pairs @{ $hscan->[1] } ],
      [ map { [ $_, "${_}value" ] } sort grep { /4$/ } @all_keys ],
      "Correct list of keys from HSCAN";

    is $redis->sadd( "test_set", @all_keys ), 40, "initialized a set";
    my $sscan = $redis->sscan( "test_set", 0, "MATCH", "*3", "COUNT", 100 );
    is $sscan->[0], 0, "Got all matching elements in a single SSCAN call";
    eq_or_diff [ sort @{ $sscan->[1] } ], [ sort grep { /3$/ } @all_keys ],
      "Correct list of elements from SSCAN";

    is $redis->zadd( "test_zset", map { ( $_, "key$_" ) } 1 .. 40 ), 40, "initialized a sorted set";
    my $zscan = $redis->zscan( "test_zset", 0, "MATCH", "*2", "COUNT", 100 );
    is $zscan->[0], 0, "Got all matching elements in a single ZSCAN call";
    eq_or_diff [ sort { $a->[0] cmp $b->[0] } group_pairs @{ $zscan->[1] } ],
      [ sort { $a->[0] cmp $b->[0] } grep { $_->[0] =~ /2$/ } map { [ "key$_", $_ ] } 1 .. 40 ],
      "Correct list of elements from ZSCAN";
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

    is $redis->rpoplpush( "list1", "list2" ), "V8", "RPOPLPUSH";
    is $redis->llen("list1"), 2, "list1 len is 2";

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
    if ( $redis->version > 2.005 ) {
        ok abs( $redis->hincrbyfloat( 'thash', 'counter', '-2.5' ) - 4.5 ) < 1e-7, "HINCRBYFLOAT";
    }
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
    my $info2;
    $redis->info( sub { $info2 = $_[1] } );
    $redis->mainloop;
    is ref($info2), "HASH", "Got hashref in info callback";
    is $info2->{redis_version}, $info->{redis_version}, "Same info as from synchronous call";

    if ( $redis->version >= 2.0 ) {
        eq_or_diff $redis->config_get("dbfilename"), [qw(dbfilename dump_test.rdb)], "CONFIG GET";
    }
    if ( $redis->version >= 2.005 ) {
        my ( $sec, $ms ) = @{ $redis->time };
        ok time - $sec < 2, "Server time is correct";
    }
    if ( $redis->version ge 2.006009 ) {
        my $redis2 =
          RedisDB->new( host => 'localhost', port => $server->{port}, connection_name => 'bar', );
        is $redis->client_getname, undef, "Name for connection is not set";
        is $redis->client_setname("foo"), "OK", "Set it to 'foo'";
        is $redis->client_getname, "foo", "Now connection name is 'foo'";
        my $clients = $redis->client_list;
        is 0 + @$clients, 2, "Two clients connected to the server";
        unless ( $clients->[0]{name} eq 'foo' ) {
            @$clients = reverse @$clients;
        }
        is $clients->[0]{name}, "foo", "First client's name 'foo'";
        is $clients->[1]{name}, "bar", "Another's is 'bar'";
        is $redis->client_kill( $clients->[1]{addr} ), "OK",
          "Killed 'bar' connection ($clients->[1]{addr})";
        $redis->client_list( sub { $clients = $_[1] } );
        $redis->mainloop;
        is 0 + @$clients, 1, "Only one client is connected";
        is $redis2->client_getname, "bar", "Second connection is restored with name 'bar'";
    }
    else {
        diag "Skipped tests for redis >= 2.6.9";
    }
}

sub cmd_sets {
    $redis->flushdb;
    is $redis->sadd( "set1", "A" ), 1, "SADD set1 A";
    is $redis->sadd( "set1", "B" ), 1, "SADD set1 B";
    is $redis->sadd( "set1", "C" ), 1, "SADD set1 C";
    is $redis->sadd( "set1", "A" ), 0, "SADD set1 A";
    is $redis->sadd( "set1", "D" ), 1, "SADD set1 D";
    is $redis->scard("set1"), 4, "4 elements in set1";

    is $redis->sadd( "set2", "B" ), 1, "SADD set2 B";
    is $redis->sadd( "set2", "D" ), 1, "SADD set2 D";
    is $redis->sadd( "set2", "F" ), 1, "SADD set2 F";

    eq_or_diff [ sort @{ $redis->sdiff( "set1", "set2" ) } ], [qw(A C)], "SDIFF";
    is $redis->sdiffstore( "set3", "set2", "set1" ), 1, "SDIFFSTORE set3 set2 set1";
    is $redis->sdiffstore( "set4", "set1", "set2" ), 2, "SDIFFSTORE set4 set1 set2";

    eq_or_diff $redis->sinter( "set3", "set4" ), [], "SINTER set3 set4 is empty";
    is $redis->sinterstore( "set5", "set1", "set2" ), 2, "SINTERSTORE set5 set1 set2";
    is $redis->sismember( "set3", "F" ), 1, "SISMEMBER";
    is $redis->sismember( "set3", "B" ), 0, "not SISMEMBER";
    eq_or_diff [ sort @{ $redis->smembers("set5") } ], [qw(B D)], "SMEMBERS set5";
    is $redis->smove( "set3", "set5", "B" ), 0, "SMOVE";
    is $redis->smove( "set3", "set5", "F" ), 1, "SMOVE";
    eq_or_diff [ sort @{ $redis->sunion( "set4", "set5" ) } ], [qw(A B C D F)], "SUNION";
    is $redis->sunionstore( "big_set", "set4", "set5" ), 5, "SUNIONSTORE";
    is $redis->sunionstore( "large_set", "big_set" ), 5, "copy set";
    is $redis->sismember( "big_set", $redis->srandmember("big_set") ), 1, "SRANDMEMBER";
    eq_or_diff $redis->sdiff( "large_set", "big_set" ), [], "SDIFF empty";
    my $elem = $redis->spop("big_set");
    eq_or_diff $redis->sdiff( "large_set", "big_set" ), [$elem], "SPOP removed element";
    is $redis->srem( "large_set", $elem ), 1, "SREM";
    eq_or_diff $redis->sdiff( "large_set", "big_set" ), [], "SREM removed element";
}

sub cut_precision {
    @_ = @{ +shift };
    my @res;
    while (@_) {
        push @res, shift;
        push @res, 0 + sprintf "%.3f", shift;
    }
    return \@res;
}

sub cmd_zsets {
    $redis->flushdb;
    is $redis->zadd( "zset1", 1.24, "one" ), 1, "ZADD add";
    is $redis->zadd( "zset1", 1,    "one" ), 0, "ZADD update";
    my @zset = ( 3.2 => "three", 2.1 => "two", 7 => "four", 5 => "five", 4.1 => "four" );
    $redis->zadd( "zset1", splice @zset, 0, 2 ) while @zset;
    is $redis->zcard("zset1"), 5, "ZCARD";
    is $redis->zcount( "zset1", 1.1, 4 ), 2, "ZCOUNT";
    is sprintf( "%.2f", $redis->zincrby( "zset1", 0.1, "two" ) ), "2.20", "ZINCRBY";
    is sprintf( "%.2f", $redis->zincrby( "zset1", 0,   "two" ) ), "2.20", "ZINCRBY 0";

    my @zset2 = ( 1 => "A", 2 => "B", 3 => "C", 4 => "D", 5 => "E", 6 => "F" );
    $redis->zadd( "zset2", splice @zset2, 0, 2 ) while @zset2;
    my @zset3 = ( 0.5 => "A", 0.4 => "B", 0.3 => "C", 0.2 => "D", 0.1 => "E" );
    $redis->zadd( "zset3", splice @zset3, 0, 2 ) while @zset3;

    is $redis->zinterstore( "zsum", 2, "zset2", "zset3" ), 5, "ZINTERSTORE";
    eq_or_diff cut_precision( $redis->zrange( "zsum", 0, -1, "WITHSCORES" ) ),
      [qw(A 1.5 B 2.4 C 3.3 D 4.2 E 5.1)], "ZRANGE";
    is $redis->zinterstore( "zmax", 2, "zset2", "zset3", "weights", 1, 6, "aggregate", "max" ),
      5, "ZINTERSTORE max";
    eq_or_diff cut_precision( $redis->zrange( "zmax", 0, -1, "WITHSCORES" ) ),
      [qw(B 2.4 A 3 C 3 D 4 E 5)], "ZRANGE";
    eq_or_diff $redis->zrangebyscore( "zmax", '(3', '5' ), [qw(D E)], "ZRANGEBYSCORE";
    is $redis->zrank( "zmax", "D" ), 3, "ZRANK";
    is $redis->zrem( "zmax", 'C' ), 1, "ZREM";
    is $redis->zrem( "zmax", 'E' ), 1, "ZREM";
    is $redis->zrem( "zmax", 'F' ), 0, "ZREM";
    eq_or_diff $redis->zrange( "zmax", 0, -1 ), [qw(B A D)], "check result of ZREM";
    is $redis->zremrangebyrank( "zmax", 1, 1 ), 1, "ZREMRANGEBYRANK";
    eq_or_diff $redis->zrange( "zmax", 0, -1 ), [qw(B D)], "check result of ZREMANGEBYRANK";

    if ( $redis->version >= 2.001006 ) {
        is $redis->zremrangebyscore( "zmax", 3, "+inf" ), 1, "ZREMRANGEBYSCORE";
        eq_or_diff $redis->zrange( "zmax", 0, -1 ), [qw(B)], "check result of ZREMRANGEBYSCORE";
        eq_or_diff $redis->zrevrange( "zset2", 0, -1 ),
          [ reverse @{ $redis->zrange( "zset2", 0, -1 ) } ], "ZREVRANGE";
        eq_or_diff $redis->zrevrangebyscore( "zset2", 6, 1 ),
          [ reverse @{ $redis->zrangebyscore( "zset2", 1, 6 ) } ],
          "ZREVRANGEBYSCORE";
        is $redis->zrevrank( "zset2", "D" ), 2, "ZREVRANK";
        is $redis->zscore( "zset2", "D" ), 4, "ZSCORE";
        is $redis->zunionstore( "zunion", 2, "zset2", "zset3", "aggregate", "sum" ), 6,
          "ZUNIONSTORE";
        eq_or_diff cut_precision( $redis->zrange( "zunion", 0, -1, "WITHSCORES" ) ),
          [qw(A 1.5 B 2.4 C 3.3 D 4.2 E 5.1 F 6)], "ZUNIONSTORE result is correct";
    }
}

sub cmd_scripts {
    plan skip_all => "This test requires redis-server >= 2.6" unless $redis->version >= 2.005;
    $redis->flushdb;
    is $redis->script_flush, 'OK', "SCRIPT FLUSH";

    my $script1 = "return {1,2,{3,'test',ARGV[1]}}";
    my $sha1    = sha1_hex($script1);
    eq_or_diff $redis->eval( $script1, 0, 'passed' ), [ 1, 2, [ 3, 'test', 'passed' ] ], "EVAL";

    my $script2 = "return redis.call('set',KEYS[1],ARGV[1])";
    my $sha2    = sha1_hex($script2);
    is $redis->eval( $script2, 1, 'eval', 'passed' ), "OK", "eval set";

    my $script3 = "return redis.call('get',KEYS[1])";
    my $sha3    = sha1_hex($script3);
    eq_or_diff $redis->script_exists( $sha1, $sha3, $sha2 ), [ 1, 0, 1 ], "SCRIPT EXISTS";
    is $redis->script_load($script3), $sha3, "SCRIPT LOAD";
    eq_or_diff $redis->evalsha( $sha3, 1, 'eval' ), "passed", "EVALSHA";
}

done_testing;
