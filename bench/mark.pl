#!/usr/bin/perl 

use 5.010;
use warnings;
use Redis;
use RedisDB;
use Redis::hiredis;
use lib qw(t ../t);
use RedisServer;
use Redis::Client;

use Benchmark qw( cmpthese );

# benchmark some redis clients.  Note, that I'm getting quite a different
# results in different environments, need to review this thing.

say "Testing against";
say "RedisDB:        ", RedisDB->VERSION;
say "Redis:          ", Redis->VERSION;
say "Redis::hiredis: ", Redis::hiredis->VERSION;
say "Redis::Client:  ", Redis::Client->VERSION;

my $srv   = RedisServer->start;
my $redis = Redis->new(
    server   => "localhost:$srv->{port}",
    encoding => undef,
#    reconnect => 1,
);
my $redisdb = RedisDB->new( host => "localhost", port => $srv->{port} );
my $rediscl = Redis::Client->new( host => "localhost", port => $srv->{port} );
my $hiredis = Redis::hiredis->new();
$hiredis->connect('localhost');

sub sender {
    my ( $cli, $num, $data ) = @_;
    for ( 1 .. $num ) {
        $cli->set( "key$_", $data );
        $cli->get("key$_");
    }
}

$redisdb->set( "RDB$_", "0123456789abcdef", RedisDB::IGNORE_REPLY ) for 1 .. 1000;

say '';

cmpthese 150, {
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
};

say '';

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
