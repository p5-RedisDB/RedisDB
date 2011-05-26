use Test::Most 0.22;
use RedisDB;
use IO::Socket::INET;
use IO::Socket::UNIX;
use File::Temp qw(tempdir);
use File::Spec;
use Try::Tiny;

# Check that module is able to restore connection

my $srv = IO::Socket::INET->new( LocalAddr => '127.0.0.1', Proto => 'tcp', Listen => 1 );

if ( fork == 0 ) {
    my $conn = 0;
    $SIG{ALRM} = sub { die "Died on timeout. $conn connections accepted" };
    alarm 10;
    my @replies = ( "1", "2", "+PONG\015\012", "+OK\015\012", "+OK" );
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

# check that we able to connect to server several times
# workaround for some cpan testers

sub get_str {
    my $resp;
    my $cli =
      IO::Socket::INET->new( Proto => 'tcp', PeerAddr => '127.0.0.1', PeerPort => $srv->sockport );
    if ($cli) {
        $cli->send("Hey!");
        $cli->recv( $resp, 128 );
    }
    return $resp;
}

# first connect
get_str;

# skip tests if second connect fails
plan( skip_all => "Server doesn't allow to connect second time" ) unless get_str;

plan("no_plan");
my $redis = RedisDB->new( host => '127.0.0.1', port => $srv->sockport );
my $ret;
lives_ok { $ret = $redis->ping } "Ping";
is $ret, 'PONG', "pong";
undef $ret;
lives_ok { $ret = $redis->set( 'key', 'value' ) } "Connection restored";
is $ret, 'OK', "key is set";
dies_ok { $redis->get('key') } "Died on unclean disconnect";

# Check that IO timeout is working

SKIP: {
    skip "OS $^O doesn't support timeout on sockets", 2 if $^O =~ /solaris/;

    $srv = IO::Socket::INET->new( LocalAddr => '127.0.0.1', Proto => 'tcp', Listen => 1 );

    if ( fork == 0 ) {
        $SIG{ALRM} = sub { exit 0 };
        alarm 10;
        my $cli = $srv->accept;
        1 while defined $cli->recv( my $buf, 1024 );
        exit 0;
    }

    $redis = RedisDB->new( host => '127.0.0.1', port => $srv->sockport, timeout => 3 );
    lives_ok { $redis->send_command('PING') } "Sent command without problems";
    dies_ok { $redis->get_reply } "Dies on timeout while receiving reply";
}

# Check that we can connect to UNIX socket

SKIP: {
    my $sock_path = File::Spec->catfile( tempdir( CLEANUP => 1 ), "test_redis" );
    my $srv =
      try { IO::Socket::UNIX->new( Type => SOCK_STREAM, Local => $sock_path, Listen => 1 ) };
    skip "Can't create UNIX socket", 2 unless $srv;
    if ( fork == 0 ) {
        $SIG{ALRM} = sub { exit 0 };
        alarm 10;
        my $cli = $srv->accept;
        $cli->recv( my $buf, 1024 );
        $cli->send("+PONG\r\n");
        $cli->close;
        exit 0;
    }
    lives_ok { $redis = RedisDB->new( path => $sock_path ) } "Connected to UNIX socket";
    is $redis->get("ping"), "PONG", "Got PONG via UNIX socket";
}

