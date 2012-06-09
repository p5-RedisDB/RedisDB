use strict;
use warnings;

use Test::More;

BEGIN {
    eval "use Test::LeakTrace";
    plan skip_all => 'This test requires Test::LeakTrace' if $@;
}

use RedisDB;
use RedisDB::Parse::Redis;

my $srv = IO::Socket::INET->new( LocalAddr => '127.0.0.1', Proto => 'tcp', Listen => 1 );
plan skip_all => "Can't start server" unless $srv;
my $pid = fork;

if ( defined($pid) && $pid == 0 ) {
    alarm 10;
    my $cli = $srv->accept;
    1 while <$cli>;
    exit 0;
}

no_leaks_ok {
    my $redis = RedisDB->new( host => '127.0.0.1', port => $srv->sockport, timeout => 5 );
}
"create/destroy an object";

no_leaks_ok {
    my $redisdb = {};
    my $parser = RedisDB::Parse::Redis->new( redisdb => $redisdb );
    $redisdb->{parser} = $parser;
    $parser->add_callback( sub { 1 } );
    my $lf = "\015\012";
    $parser->add( "*3${lf}"
          . "*4${lf}:5${lf}:1336734898${lf}:43${lf}"
          . "*2${lf}\$3${lf}get${lf}\$4${lf}test${lf}"
          . "*4${lf}:4${lf}:1336734895${lf}:175${lf}"
          . "*3${lf}\$3${lf}set${lf}\$4${lf}test${lf}\$2${lf}43${lf}"
          . "*4${lf}:3${lf}:1336734889${lf}:20${lf}"
          . "*2${lf}\$7${lf}slowlog${lf}\$3${lf}len${lf}" );
}
"parse a complex structure";

no_leaks_ok {
    my $redisdb = {};
    my $parser = RedisDB::Parse::Redis->new( redisdb => $redisdb );
    $redisdb->{parser} = $parser;
    $parser->add_callback( sub { 1 } );
    my $lf = "\015\012";
    eval {
        $parser->add( "*3${lf}"
              . "*4${lf}:5${lf}:1336734898${lf}:43${lf}"
              . "*2${lf}\$3${lf}gets${lf}\$4${lf}tests${lf}" );
    };
}
"parser throws an exception";

no_leaks_ok {
    my $redisdb = {};
    my $parser = RedisDB::Parse::Redis->new( redisdb => $redisdb );
    $redisdb->{parser} = $parser;
    $parser->add_callback( sub { die "Oops" } );
    my $lf = "\015\012";
    eval { $parser->add("\$4${lf}test${lf}"); };
}
"callback throws an exception";

done_testing;
