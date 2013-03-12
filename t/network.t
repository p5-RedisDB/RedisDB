use Test::Most 0.22;
use RedisDB;
use IO::Socket::INET;
use IO::Socket::UNIX;
use File::Temp qw(tempdir);
use File::Spec;
use Try::Tiny;
use Time::HiRes qw(usleep);
use Test::FailWarnings;

# Check that module is able to restore connection
subtest "Restore connection" => sub {
    my $srv = IO::Socket::INET->new(
        LocalAddr => '127.0.0.1',
        Proto     => 'tcp',
        Listen    => 1,
        ReuseAddr => 1,
    );
    plan skip_all => "Can't start server" unless $srv;
    my $empty_port = IO::Socket::INET->new(
        LocalAddr => '127.0.0.1',
        Proto     => 'tcp',
        Listen    => 1,
    )->sockport;
    die "Couldn't get empty port" unless $empty_port;

    my $port = $srv->sockport;
    my $pid  = fork;
    if ( $pid == 0 ) {
        $SIG{ALRM} = sub { die "Died on timeout." };
        alarm 10;
        my $cli = $srv->accept;
        $cli->recv( my $buf, 1024 );
        $cli->send( "+PONG\015\012", 0 );
        $cli->close;

        # simulate restart of the redis-server
        $srv->close;
        usleep 1_000_000;
        $srv = IO::Socket::INET->new(
            LocalAddr => '127.0.0.1',
            LocalPort => $port,
            Proto     => 'tcp',
            Listen    => 1,
            ReuseAddr => 1,
        ) or die $!;
        my @replies = ( "+OK\015\012", "+OK", "+PONG\015\012", ":42\015\012" );
        while (@replies) {
            my $cli = $srv->accept;
            $cli->recv( my $buf, 1024 );
            $cli->send( shift(@replies), 0 );
            close $cli;
        }
        exit 0;
    }

    close $srv;
    my $redis = RedisDB->new(
        host                => '127.0.0.1',
        port                => $port,
        lazy                => 1,
        reconnect_attempts  => 3,
        reconnect_delay_max => 2,
    );
    my $ret;
    lives_ok { $ret = $redis->ping } "Ping";
    is $ret, 'PONG', "pong";
    undef $ret;
    usleep 200_000;
    lives_ok { $ret = $redis->set( 'key', 'value' ) } "Connection restored";
    is $ret, 'OK', "key is set";
    usleep 200_000;
    dies_ok { $redis->get('key') } "Died on unclean disconnect";
    is $redis->ping, 'PONG', "Restored connection after exception";
    my $invoked_callback;
    my $fourty_two = RedisDB->new(
        host             => '127.0.0.1',
        port             => $empty_port,
        on_connect_error => sub { shift->{port} = $port; $invoked_callback++; },
    )->get('forty_two');
    ok $invoked_callback, "Invoked 'on_connect_error' callback";
    is $fourty_two, 42, "Restored connection after invoking 'on_connect_error'";
    wait;
    throws_ok {
        RedisDB->new(
            host                => '127.0.0.1',
            port                => $port,
            reconnect_attempts  => 3,
            reconnect_delay_max => 2,
          )
    }
    qr/on_connect_error/, "Dies on conection failure";
};

# Check functionality if raise_error is disabled
subtest "Restore connection without raise_error" => sub {
    my $srv = IO::Socket::INET->new(
        LocalAddr => '127.0.0.1',
        Proto     => 'tcp',
        Listen    => 1,
        ReuseAddr => 1,
    );
    plan skip_all => "Can't start server" unless $srv;

    my $port = $srv->sockport;
    my $pid  = fork;
    if ( $pid == 0 ) {
        $SIG{ALRM} = sub { die "Died on timeout." };
        alarm 10;
        my $cli = $srv->accept;
        my $buf = '';
        while ( $buf !~ /foo/ ) {
            $cli->recv( $buf, 1024 );
        }
        $cli->send( "+PONG", 0 );
        $cli->close;

        $srv->close;
        usleep 1_000_000;
        $srv = IO::Socket::INET->new(
            LocalAddr => '127.0.0.1',
            LocalPort => $port,
            Proto     => 'tcp',
            Listen    => 1,
            ReuseAddr => 1,
        ) or die $!;
        my @replies = ("+OK\015\012");
        while (@replies) {
            my $cli = $srv->accept;
            $cli->recv( my $buf, 1024 );
            $cli->send( shift(@replies), 0 );
            close $cli;
        }
        exit 0;
    }

    close $srv;
    my $redis = RedisDB->new(
        host                => '127.0.0.1',
        port                => $port,
        raise_error         => undef,
        reconnect_attempts  => 3,
        reconnect_delay_max => 2,
    );
    my $cb_res;
    $redis->set( "baz", "bar", sub { $cb_res = $_[1] } );
    my $res = $redis->get("foo");
    isa_ok $res,    "RedisDB::Error::DISCONNECTED", "get returned an error";
    ok $cb_res,     "Callback has been invoked";
    isa_ok $cb_res, "RedisDB::Error::DISCONNECTED", "  with an error object";

    is $redis->set( "key", "value" ), "OK", "reconnected and set the key";
};

# Check what will happen if server immediately closes connection
subtest "No _connect recursion" => sub {
    my $srv = IO::Socket::INET->new( LocalAddr => '127.0.0.1', Proto => 'tcp', Listen => 1 );
    plan skip_all => "Can't start server" unless $srv;

    my $pid = fork;
    if ( $pid == 0 ) {
        $SIG{ALRM} = sub { exit 0 };
        alarm 5;
        my $cli = $srv->accept;
        close $cli;
        exit 0;
    }

    my $port = $srv->sockport;
    close $srv;
    my $redis = RedisDB->new( host => '127.0.0.1', port => $port, lazy => 1, database => 1 );
    dies_ok { $redis->set( 'key', 'value' ); } "dies on recursive _connect";
};

# Check that IO timeout is working
subtest "socket timeout" => sub {
    plan skip_all => "OS $^O doesn't support timeout on sockets" if $^O =~ /solaris|MSWin32|cygwin/;

    my $srv = IO::Socket::INET->new( LocalAddr => '127.0.0.1', Proto => 'tcp', Listen => 1 );

    my $pid = fork;
    if ( $pid == 0 ) {
        $SIG{ALRM} = sub { exit 0 };
        alarm 10;
        my $cli = $srv->accept;
        my $buf;
        1 while defined $cli->recv( $buf, 1024 ) && $buf;
        exit 0;
    }

    my $redis = RedisDB->new( host => '127.0.0.1', port => $srv->sockport, timeout => 3 );
    lives_ok { $redis->send_command('PING') } "Sent command without problems";
    throws_ok { $redis->get_reply } 'RedisDB::Error::EAGAIN',
      "Dies on timeout while receiving reply";
};

# Check that we can connect to UNIX socket
subtest "UNIX socket" => sub {
    plan skip_all => "OS $^O doesn't support UNIX sockets" if $^O =~ /MSWin32/;
    my $sock_path = File::Spec->catfile( tempdir( CLEANUP => 1 ), "test_redis" );
    my $srv =
      try { IO::Socket::UNIX->new( Type => SOCK_STREAM, Local => $sock_path, Listen => 1 ) };
    plan skip_all => "Can't create UNIX socket" unless $srv;
    my $pid = fork;
    if ( $pid == 0 ) {
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
    my $redis;
    lives_ok { $redis = RedisDB->new( path => $sock_path ) } "Connected to UNIX socket";
    is $redis->get("ping"), "PONG", "Got PONG via UNIX socket";
};

done_testing;
