#!/usr/bin/perl 

use 5.010;
use warnings;
use Redis;
use RedisDB;
use Redis::hiredis;
use AnyEvent::Redis;
use AnyEvent::Redis::RipeRedis;
use lib qw(t ../t);
use RedisServer;
use Redis::Client;

use Benchmark qw( cmpthese );

say <<EOT;
******************************************************************
* Check source code to understand what this script benchmarking. *
* Depending on your environment you may get different results,   *
* e.g. hyper-threading may significantly decrease performance    *
* of some modules.                                               *
******************************************************************
EOT

say "Testing against";
say "RedisDB:         ",            RedisDB->VERSION;
say "Redis:           ",            Redis->VERSION;
say "Redis::hiredis:  ",            Redis::hiredis->VERSION;
say "Redis::Client:   ",            Redis::Client->VERSION;
say "AnyEvent::Redis: ",            AnyEvent::Redis->VERSION;
say "AnyEvent::Redis::RipeRedis: ", AnyEvent::Redis::RipeRedis->VERSION;

my $srv;

if ( $ENV{TEST_REDIS} ) {
    my ( $host, $port ) = $ENV{TEST_REDIS} =~ /([^:]+):([0-9]+)/;
    $srv = { host => $host, port => $port };
}
else {
    $srv = RedisServer->start;
    $srv->{host} = '127.0.0.1';
}

my $redis = Redis->new(
    server   => "$srv->{host}:$srv->{port}",
    encoding => undef,

    #    reconnect => 1,
);
my $redisdb = RedisDB->new( host => "$srv->{host}", port => $srv->{port} );
say "redis-server: ", $redisdb->version;
my $rediscl = Redis::Client->new( host => "$srv->{host}", port => $srv->{port} );
my $hiredis = Redis::hiredis->new();
$hiredis->connect( $srv->{host}, $srv->{port} );

my $ae_redis = AnyEvent::Redis->new( host => "$srv->{host}", port => $srv->{port} );
my $cv = $ae_redis->set( "RDB_AE_TEST", 1 );
$cv->recv;

$cv = AE::cv;
my $ripe = AnyEvent::Redis::RipeRedis->new( host => "$srv->{host}", port => $srv->{port} );
$ripe->set(
    "RDB_RIPE_TEST",
    1,
    {
        on_done => sub { $cv->send }
    }
);
$cv->recv;

sub sender {
    my ( $cli, $num, $data ) = @_;
    for ( 1 .. $num ) {
        $cli->set( "key$_", $data );
        $cli->get("key$_");
    }
}

$redisdb->set( "RDB$_", "0123456789abcdef", RedisDB::IGNORE_REPLY ) for 1 .. 1000;

say '';
say "Testing setting/getting 16 bytes values";

cmpthese 50, {
    Redis => sub {
        sender( $redis, 1000, "0123456789abcdef" );
    },
    hiredis => sub {
        sender( $hiredis, 1000, "0123456789abcdef" );
    },
    RedisDB => sub {
        sender( $redisdb, 1000, "0123456789abcdef" );
    },
    "Redis::Client" => sub {
        sender( $rediscl, 1000, "0123456789abcdef" );
    },
    "RedisDB Pipelining" => sub {
        for ( 1 .. 1000 ) {
            $redisdb->set( "RDB$_", "0123456789abcdef", RedisDB::IGNORE_REPLY );
            $redisdb->get( "RDB$_", RedisDB::IGNORE_REPLY );
        }
        $redisdb->mainloop;
    },
    "Redis Pipelining" => sub {
        for ( 1 .. 1000 ) {
            $redis->set( "RDB$_", "0123456789abcdef", sub { } );
            $redis->get( "RDB$_", sub { } );
        }
        $redis->wait_all_responses;
    },
    "hiredis pipelining" => sub {
        for ( 1 .. 1000 ) {
            $hiredis->append_command("SET RDB$_ 0123456789abcdef");
            $hiredis->append_command("GET RDB$_");
        }
        my %res;
        for ( 1 .. 2000 ) {
            $res{ $hiredis->get_reply }++;
        }
    },
    "AE::Redis" => sub {
        my $done = AE::cv;
        my $cnt;
        AE::postpone {
            for ( 1 .. 1000 ) {
                $ae_redis->set( "RDB$_", "0123456789abcdef", sub { } );
                $ae_redis->get( "RDB$_", sub { $done->send if ++$cnt == 1000 } );
            }
        };
        $done->recv;
    },
    "AE::R::RipeRedis" => sub {
        my $done = AE::cv;
        my $cnt;
        AE::postpone {
            for ( 1 .. 1000 ) {
                $ripe->set( "RDB$_", "0123456789abcdef" );
                $ripe->get(
                    "RDB$_",
                    {
                        on_done => sub { $done->send if ++$cnt == 1000 }
                    }
                );
            }
        };
        $done->recv;
    },
};

say '';
say "Testing setting/getting 2K values";

cmpthese 150, {
    Redis => sub {
        sender( $redis, 300, "0123456789abcdef" x 128 );
    },
    hiredis => sub {
        sender( $hiredis, 300, "0123456789abcdef" x 128 );
    },
    RedisDB => sub {
        sender( $redisdb, 300, "0123456789abcdef" x 128 );
    },
    "Redis::Client" => sub {
        sender( $rediscl, 300, "0123456789abcdef" x 128 );
    },
};

say '';
say "Testing setting/getting 16K values";

cmpthese 150, {
    Redis => sub {
        sender( $redis, 100, "0123456789abcdef" x 1024 );
    },
    hiredis => sub {
        sender( $hiredis, 100, "0123456789abcdef" x 1024 );
    },
    RedisDB => sub {
        sender( $redisdb, 100, "0123456789abcdef" x 1024 );
    },
    "Redis::Client" => sub {
        sender( $rediscl, 100, "0123456789abcdef" x 1024 );
    },
};
