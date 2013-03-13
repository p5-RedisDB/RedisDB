use Test::Most 0.22;
use Test::FailWarnings;
use lib 't';
use RedisServer;
use RedisDB;

my $server = RedisServer->start;
plan( skip_all => "Can't start redis-server" ) unless $server;

my $pub = RedisDB->new(
    host => 'localhost',
    port => $server->{port},
);
plan( skip_all => "This test requires redis-server 2.6.9 or later" ) if $pub->version lt 2.006009;

my $sub = RedisDB->new(
    host            => 'localhost',
    port            => $server->{port},
    raise_error     => undef,
    connection_name => "test_subscriber",
);

$sub->subscribe("sub");
$sub->psubscribe("psub*");
is $pub->publish( "sub",      "foo" ), 1, "published to sub";
is $pub->publish( "psub-boo", "bar" ), 1, "published to psub-boo";

alarm 10;
my $res = $sub->get_reply;
eq_or_diff $res, [ 'subscribe', 'sub', 1 ], "got subscribe message";
$res = $sub->get_reply;
eq_or_diff $res, [ 'psubscribe', 'psub*', 2 ], "got psubscribe message";
$res = $sub->get_reply;
eq_or_diff $res, [ 'message', 'sub', 'foo' ], "got message from sub";
$res = $sub->get_reply;
eq_or_diff $res, [ 'pmessage', 'psub*', 'psub-boo', 'bar' ], "got message from psub-boo";
alarm 0;

# disconnect subscriber
my $clients = $pub->client_list;
my ($sub_rec) = grep { $_->{name} eq 'test_subscriber' } @$clients;
$pub->client_kill( $sub_rec->{addr} );

alarm 10;
$res = $sub->get_reply;
eq_or_diff $res, [ 'subscribe', 'sub', 1 ], "got subscribe message";
$res = $sub->get_reply;
eq_or_diff $res, [ 'psubscribe', 'psub*', 2 ], "got psubscribe message";

$pub->publish( "sub", "subscriptions restored" );

$res = $sub->get_reply;
eq_or_diff $res, [ "message", "sub", "subscriptions restored" ],
  "got a new message from sub channel";
alarm 0;

done_testing;
