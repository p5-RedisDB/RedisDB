use 5.010;
use strict;
use warnings;

use Test::RedisDB;
use RedisDB;

my @servers;
for ( 1 .. 3 ) {
    my $srv = Test::RedisDB->new;
    push @servers, $srv;
}

=head1 NAME

server_failover.pl

=head1 DESCRIPTION

This example demonstrates how you can switch between several servers. If one
of the servers goes down, RedisDB will connect to another one. All you need to
do is to set new host and port attributes in I<on_connect_error> callback.
After callback returns, module tries to establish connection again using new
parameters.

=cut

my $redis = RedisDB->new(
    port             => $servers[0]->port,
    raise_error      => 0,
    on_connect_error => sub {
        shift @servers;

        # or it could be:
        # push @servers, shift @servers;
        unless(@servers) {
            say "No more servers";
            exit 0;
        }
        say "Disconnected from $_[0]{port}, connecting to $servers[0]{port}";
        $_[0]{port} = $servers[0]{port};
    },
);

while (1) {
    say "Foo: ", $redis->incr("foo");
    $redis->shutdown if rand > 0.9;
    sleep 1;
}
