#!/usr/bin/perl 

use strict;
use warnings;
use RedisDB;
use lib qw(t ../t);
use RedisServer;

my $srv = RedisServer->start;
my $redisdb = RedisDB->new(host => "localhost", port => $srv->{port});

for ( 1 .. 10 ) {
    for ( 1 .. 10000 ) {
        $redisdb->send_command( 'SET', "RDB$_", "0123456789abcdef" );
        $redisdb->send_command( 'GET', "RDB$_" );
    }
    my %res;
    for ( 1 .. 20000 ) {
        $res{ ( $redisdb->get_reply )[1] }++;
    }
    die "wrong result" unless $res{'0123456789abcdef'} == 10000;
}
