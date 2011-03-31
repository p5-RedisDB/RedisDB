use Test::Most;
use lib 't';
use RedisServer;
use RedisDB;

my $srv = RedisServer->start;

plan( skip_all => "Can't start redis-server" ) unless $srv;
plan("no_plan");
my $redis = RedisDB->new( host => $srv->{host}, port => $srv->{port} );
lives_ok { $redis->set( 'key', 'value' ) } "Set key value";
$srv->restart;
sleep 2;
my $val = $redis->get('key');
is $val, 'value', 'yeah! got the value';
