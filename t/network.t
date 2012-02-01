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
    my @replies = ( "+PONG\015\012", "+OK\015\012", "+OK" );
    while (@replies) {
        my $cli = $srv->accept;
        $conn++;
        { local $/ = "\r\n"; $cli->getline; }
        $cli->send( shift(@replies), 0 );
        close $cli;
    }
    exit 0;
}

plan( skip_all => "Can't start server" ) unless $srv;

my $port = $srv->sockport;
close $srv;
my $redis = RedisDB->new( host => '127.0.0.1', port => $port, lazy => 1 );
my $ret;
lives_ok { $ret = $redis->ping } "Ping";
is $ret, 'PONG', "pong";
undef $ret;
sleep 1;
lives_ok { $ret = $redis->set( 'key', 'value' ) } "Connection restored";
is $ret, 'OK', "key is set";
dies_ok { $redis->get('key') } "Died on unclean disconnect";
wait;
dies_ok { RedisDB->new( host => '127.0.0.1', port => $port ) } "Dies on conection failure";

# Check what will happen if server immediately closes connection

$srv = IO::Socket::INET->new( LocalAddr => '127.0.0.1', Proto => 'tcp', Listen => 1 );
if ( fork == 0 ) {
    $SIG{ALRM} = sub { exit 0 };
    alarm 5;
    while () {
        my $cli = $srv->accept;
        close $cli;
    }
}

$port = $srv->sockport;
close $srv;
$redis = RedisDB->new( host => '127.0.0.1', port => $port, lazy => 1 );
$redis->{_db_number} = 1;
dies_ok { $redis->set( 'key', 'value' ); } "dies if redis-server immediately closes connection";

# Check that IO timeout is working

SKIP: {
    skip "OS $^O doesn't support timeout on sockets", 2 if $^O =~ /solaris|MSWin32/;

    $srv = IO::Socket::INET->new( LocalAddr => '127.0.0.1', Proto => 'tcp', Listen => 1 );

    if ( fork == 0 ) {
        $SIG{ALRM} = sub { exit 0 };
        alarm 10;
        my $cli = $srv->accept;
        my $buf;
        1 while defined $cli->recv( $buf, 1024 ) && $buf;
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
        defined $cli->recv( my $buf, 1024 ) or die "recv filed: $!";
        defined $cli->send("+PONG\r\n") or die "send filed: $!";
        $cli->close;
        exit 0;
    }
    dies_ok {
        RedisDB->new( path => $sock_path, host => 'localhost' );
    }
    "path and host can't be specified together";
    dies_ok {
        RedisDB->new( path => $sock_path, port => 6379 );
    }
    "path and port can't be specified together";
    dies_ok {
        RedisDB->new( path => "$sock_path.does_not_exist" );
    }
    "croaks if can't connect to socket";
    lives_ok { $redis = RedisDB->new( path => $sock_path ) } "Connected to UNIX socket";
    is $redis->get("ping"), "PONG", "Got PONG via UNIX socket";
}

done_testing;
