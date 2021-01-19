use Test::Most 0.22;
use Test::RedisDB;
use RedisDB;

my $server = Test::RedisDB->new( password => 'test' );
plan( skip_all => "Can't start redis-server" ) unless $server;
my $redis = RedisDB->new( host => 'localhost', port => $server->{port} );
dies_ok { $redis->ping } "Couldn't ping before auth";
dies_ok { $redis->auth("wrong pass") } "Didn't accept wrong password";
is $redis->auth('test'), 'OK', "Authenticated";
is $redis->ping, 'PONG', "Can ping now";
$redis->select(1);
$redis->set( "Database", 1 );

if ( $redis->version >= 2 ) {
    delete $redis->{_socket};
    is $redis->ping, 'PONG', "Still can ping server after reconnecting";
    is $redis->get("Database"), 1, "Selected database 1";
    $redis->{password} = 'wrong';
    delete $redis->{_socket};
    throws_ok { $redis->ping } qr/invalid (?:username-)?password/i, "dies on reconnect if password is wrong";
}
else {
    diag "Can't finish test, requires redis-server version 2.0.0 and above";
}

done_testing;
