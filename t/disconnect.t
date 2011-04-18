use Test::Most 0.22;
use RedisDB;
use IO::Socket::INET;

my $srv = IO::Socket::INET->new( LocalAddr => '127.0.0.1', Proto => 'tcp', Listen => 1 );

if ( fork == 0 ) {
    my $conn = 0;
    $SIG{ALRM} = sub { die "Died on timeout. $conn connections accepted" };
    alarm 10;
    my @replies = ( "+PONG\015\012", "+OK\015\012", "+OK" );
    while (@replies) {
        my $cli = $srv->accept;
        $conn++;
        $cli->recv( my $buf, 1024 );
        $cli->send( shift(@replies), 0 );
        close $cli;
    }
    exit 0;
}

plan( skip_all => "Can't start server" ) unless $srv;
plan("no_plan");
my $redis = RedisDB->new( host => '127.0.0.1', port => $srv->sockport );
my $ret;
lives_ok { $ret = $redis->ping } "Ping";
is $ret, 'PONG', "pong";
undef $ret;
lives_ok { $ret = $redis->set( 'key', 'value' ) } "Connection restored";
is $ret, 'OK', "key is set";
dies_ok { $redis->get('key') } "Died on unclean disconnect";
