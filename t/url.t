use Test::Most 0.22;
use RedisDB;
use Test::RedisDB;

subtest 'incompatible combination of options' => sub {
    dies_ok {
        RedisDB->new( lazy => 1, url => 'redis://localhost',
            host => 'localhost' );
    } "Dies on url + host";

    dies_ok {
        RedisDB->new( lazy => 1, url => 'redis://localhost',
            port => 6379 );
    } "Dies on url + port";

    dies_ok {
        RedisDB->new( lazy => 1, url => 'redis://localhost',
            path => '/tmp/redis.sock' );
    } "Dies on url + path";
};

subtest 'TCP URLs - using the userinfo and path' => sub {
    my $redis = RedisDB->new( lazy => 1,
        url => 'redis://:testpassword@redis.example.com:1234/5' );

    is $redis->{host}, "redis.example.com", "host is correct";
    is $redis->{port}, 1234, "port is correct";
    is $redis->{path}, undef, "path is correct";
    is $redis->{database}, 5, "database is correct";
    is $redis->{password}, "testpassword", "password is correct";
};

subtest 'TCP URLs - using query params' => sub {
    my $redis = RedisDB->new( lazy => 1,
        url => 'redis://redis.example.com:1234?db=5&password=testpassword' );

    is $redis->{host}, "redis.example.com", "host is correct";
    is $redis->{port}, 1234, "port is correct";
    is $redis->{path}, undef, "path is correct";
    is $redis->{database}, 5, "database is correct";
    is $redis->{password}, "testpassword", "password is correct";
};

subtest 'TCP URLs - defaults' => sub {
    my $redis = RedisDB->new( lazy => 1,
        url => 'redis://' );

    is $redis->{host}, "localhost", "host is correct";
    is $redis->{port}, 6379, "port is correct";
    is $redis->{path}, undef, "path is correct";
    is $redis->{database}, 0, "database is correct";
    is $redis->{password}, undef, "password is correct";
};


subtest 'Unix domain socket URLs - using the userinfo and path' => sub {
    my $redis = RedisDB->new( lazy => 1,
        url => 'redis+unix://:testpassword@/tmp/test.sock' );

    is $redis->{path}, '/tmp/test.sock', "path is correct";
    is $redis->{database}, 0, "database is correct";
    is $redis->{password}, "testpassword", "password is correct";
};

subtest 'Unix domain socket URLs - using query params' => sub {
    my $redis = RedisDB->new( lazy => 1,
        url => 'redis+unix:///tmp/test.sock?db=5&password=testpassword' );

    is $redis->{path}, '/tmp/test.sock', "path is correct";
    is $redis->{database}, 5, "database is correct";
    is $redis->{password}, "testpassword", "password is correct";
};


subtest 'Test::RedisDB' => sub {
    my $server = Test::RedisDB->new;
    plan( skip_all => "Can't start redis-server" ) unless $server;
    my $redis  = $server->redisdb_client;
    my $id     = time . ".$$";
    $redis->set( "foo", $id );
    my $url = $server->url;
    ok $url, "got url";
    my $redis2 = RedisDB->new( url => $url );
    is $redis2->get("foo"), $id, "Connected to correct redis using the url";
};


done_testing;
