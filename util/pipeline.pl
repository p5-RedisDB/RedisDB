#!/usr/bin/perl 

use 5.010;
use strict;
use warnings;
use RedisDB;
use lib qw(t ../t);
use RedisServer;
use Time::HiRes qw(time);
use Getopt::Long;

my ( $pipeline, $size, $count ) = ( 1, 16, 50_000 );
GetOptions(
    "pipeline!" => \$pipeline,
    "size=i"    => \$size,
    "count=i"   => \$count,
) or die;

say "RedisDB: ", RedisDB->VERSION;
say "Testing ", ( $pipeline ? "in pipeling mode" : "in synchronous mode" );
say "Data chunk size ",  $size;
say "Number of chunks ", $count;

my $srv     = RedisServer->start;
my $redisdb = RedisDB->new( host => "localhost", port => $srv->{port} );
my $chunk   = 'x' x $size;
my @cb      = $pipeline ? RedisDB::IGNORE_REPLY : ();

my $start = time;

for ( 1 .. $count ) {
    $redisdb->set( "RDB$_", $chunk, @cb );
    $redisdb->get( "RDB$_", @cb );
}
$redisdb->mainloop;

printf "Time: %.3fs\n", time - $start;
