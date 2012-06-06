use Test::Most 0.22;
use lib 't';
use RedisServer;
use RedisDB;

my $server = RedisServer->start;
plan( skip_all => "Can't start redis-server" ) unless $server;

my $binary    = "\x{01}\x{00}\x{c0}\x{d1}\x{ff}\x{fe}";
my $utf8      = "\x{4f60}\x{597d}";
my $nonutf    = "\x{e4}\x{bd}\x{a0}\x{e5}\x{a5}\x{bd}";
my $moose     = "\x{e4}lg";
my $moose_bin = "\x{c3}\x{a4}lg";

my $redis = RedisDB->new( host => 'localhost', port => $server->{port} );
subtest "utf8 disabled" => sub {
    $redis->set( "binary", $binary );
    is $redis->get("binary"), $binary, "Binary value stored/retrieved correctly";
    $redis->set( $binary, "binary" );
    is $redis->get($binary), "binary", "Binary key stored/retrieved correctly";
    $redis->set( "utf8", $utf8 );
    is $redis->get("utf8"), $nonutf, "utf8 value retrived as non-utf8";
    $redis->set( $utf8, "utf8" );
    is $redis->get($utf8), "utf8", "utf8 key stored/retrieved (not exactly) correctly";
    $redis->set( $moose_bin, $moose );
    is $redis->get($moose_bin), $moose, "Caught a binary moose";
    is $redis->get($moose),     undef, "But not a unicode";
};

subtest "utf8 enabled" => sub {
    plan skip_all => "utf8 option is not implemented";
    my $redis8 =
      RedisDB->new( host => 'localhost', port => $server->{port}, utf8 => 1, );
    dies_ok { $redis8->get("binary") } "Couldn't get binary value";
    $redis8->reset_connection;
    is $redis8->set( "moose", $moose ), "OK", "set latin1 value";
    is $redis->get("moose"), $moose_bin, "latin1 value is utf8 encoded";
    is $redis8->get("moose"), $moose, "latin1 value stored/retrieved correctly";
    $redis8->set( "utf8", $utf8 );
    is $redis8->get("utf8"), $utf8, "utf8 value stored/retrieved correctly";
    is $redis->get("utf8"), $nonutf, "utf8 value correctly encoded";
    $redis8->set( $utf8, "utf8" );
    is $redis8->get($utf8),  "utf8", "utf8 key stored/retrieved correctly";
    $redis8->set($moose, "passed");
    is $redis->get($moose_bin), "passed", "latin1 key stored as utf8";
};

done_testing;

END {
    $redis->shutdown if $redis;
}
