use strict;
use warnings;

use Test::More;

BEGIN {
    eval "use Test::LeakTrace";
    plan skip_all => 'This test requires Test::LeakTrace' if $@;
}

use RedisDB;

my $srv = IO::Socket::IP->new( LocalAddr => '127.0.0.1', Proto => 'tcp', Listen => 1 );
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

done_testing;
