use Test::Most 0.22;
use lib 't';
use RedisServer;
use RedisDB;

my $server = RedisServer->start;
plan( skip_all => "Can't start redis-server" ) unless $server;
plan('no_plan');

my $redis = RedisDB->new( host => 'localhost', port => $server->{port} );

my $info = $redis->info;
is ref($info), "HASH", "Got hashref from info";
ok exists $info->{redis_version}, "There's redis_version in the hash";
$info->{redis_version} =~ /^([0-9]+)[.]([0-9]+)(?:[.]([0-9]+))?/;
ok(($1 and $2), "Looks like a version");
my $version = 0 + $1 + 0.001 * $2 + ($3 ? 0.000001 * $3 : 0);
is '' . $redis->version, "$version", "Correct server version: $version";
