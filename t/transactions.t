use Test::Most 0.22;
use lib 't';
use RedisServer;
use RedisDB;

my $server = RedisServer->start;
plan( skip_all => "Can't start redis-server" ) unless $server;

my $redis = RedisDB->new( host => 'localhost', port => $server->{port} );

is $redis->multi, 'OK', "Entered transaction";
is $redis->set( "key", "value" ), 'QUEUED', "Queued set";
for (qw(this is a list)) {
    is $redis->rpush( "list", $_ ), 'QUEUED', "Queued push '$_'";
}
is $redis->lrange( "list", 0, 3 ), 'QUEUED', "Queued lrange";

# You can pass hashref to constructor too
my $redis2 = RedisDB->new( { host => 'localhost', port => $server->{port} } );
is $redis2->set( "key", "wrong value" ), "OK", "Set key to wrong value";
my $res = $redis->exec;
eq_or_diff $res, [ qw(OK 1 2 3 4), [qw(this is a list)] ], "Transaction was successfull";
is $redis->get("key"), "value", "key set to correct value";

if ( $redis->version >= 2.001 ) {
    is $redis->watch("watch"), "OK", "watch for watch";
    is $redis->multi, 'OK', "Entered transaction";
    dies_ok { $redis->multi } "multi can't be nested";
    is $redis->set( "key", "another value" ), "QUEUED", "QUEUED set";
    is $redis2->set( "watch", "changed" ), "OK", "Set watched key";
    is $redis->exec, undef, "Transaction failed";
    is $redis->get("key"), "value", "key wasn't changed";

    is $redis->multi, "OK", "Entered transaction";
    is $redis->set("key", "another value"), "QUEUED", "QUEUED set";
    is $redis->discard, "OK", "Discarded transaction";
    is $redis->get("key"), "value", "key wasn't changed";
}

# must not reconnect while in multi
is $redis->multi, "OK", "Entered transaction";
is $redis->set( "key", "42" ), "QUEUED", "QUEUED set";
is $redis->quit, "OK", "QUIT";
dies_ok { $redis->set( "key2", "43" ) } "Not reconnecting when in transaction";
$redis = undef;

done_testing;

END {
    if ($redis) {
        $redis->shutdown;
    }
    elsif ($redis2) {
        $redis2->shutdown;
    }
}
