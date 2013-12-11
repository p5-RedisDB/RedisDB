use 5.010;
use strict;
use warnings;

use lib qw(t ../t);
use RedisServer;
use RedisDB;
use Scalar::Util qw(blessed);

=head1 NAME

no_raise_error_1.pl

=head1 DESCRIPTION

This example shows how object with raise_error disabled returns error as
blessed reference when connection to server has failed.

=cut

my @servers;
my @queues;
for ( 1 .. 3 ) {
    my $srv = RedisServer->start;
    push @servers, $srv;
    my $redis = RedisDB->new(
        host        => "127.0.0.1",
        port        => $srv->{port},
        raise_error => undef,
    );
    push @queues, $redis;
}

while (1) {
    my $n = 0;
    foreach my $redis (@queues) {
        my $len;
        $len = $redis->incr("error_test");
        if ( blessed $len) {
            say "Got an error: $len";
            $servers[$n] = RedisServer->start(port => $redis->{port}) if rand > 0.7;
        }
        else {
            say "Server $n: $len";

            # randomly stop server
            $servers[$n]->stop if rand > 0.8;
        }
        $n++;
    }
    sleep(1);
}
