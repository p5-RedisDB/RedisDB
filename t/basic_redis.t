use Test::Most 0.22;
use lib 't';
use RedisServer;
use RedisDB;

my $server = RedisServer->start;
plan( skip_all => "Can't start redis-server" ) unless $server;
my $redis = RedisDB->new( host => 'localhost', port => $server->{port} );
diag("Testing against redis-server " . $redis->info->{redis_version});
$redis->send_command('PING');
my $res = $redis->get_reply;
is $res, 'PONG', "Got PONG";

$redis->send_command( 'SET', 'test string', 'test value' );
$res = $redis->get_reply;
is $res, 'OK', "Set test string";
$redis->send_command( 'GET', 'test string' );
$res = $redis->get_reply;
is $res, 'test value', "Got test string";
$redis->send_command( 'GET', 'test non-existing string' );
$res = $redis->get_reply;
is $res, undef, "Got undef for non-existing string";

$redis->send_command( 'INCR', 'counter' );
$res = $redis->get_reply;
is $res, 1, "counter value is 1";
$redis->send_command( 'INCRBY', 'counter', 10 );
$res = $redis->get_reply;
is $res, 11, "counter value now is 11";

# Send multiple commands without reading reply
for (qw(this is a list to test LRANGE)) {
    $redis->send_command( 'RPUSH', 'test list', $_ );
}

# Read replies
for ( 1 .. 7 ) {
    $res = $redis->get_reply;
    is $res, $_, "List length is $_";
}

$redis->send_command( 'LRANGE', 'test list', 1, 3 );
$res = $redis->get_reply;
eq_or_diff $res, [qw(is a list)], 'LRANGE returned correct result';

is $redis->quit, "OK", "QUIT";

ok ! $redis->reply_ready, "No any replies";
$redis->send_command('SET', 'key A', 'value A');
$redis->send_command('RPUSH', 'list B', 'B1');
$redis->send_command('RPUSH', 'list B', 'B2');
$redis->send_command('LRANGE', 'list B', 0, 2);
$redis->send_command('GET', 'key A');
sleep 1;
ok $redis->reply_ready, "Got some replies";
is $redis->replies_to_fetch, 5, "5 commands in flight";
eq_or_diff [ $redis->get_all_replies ], [ 'OK', '1', '2', [ qw(B1 B2) ], 'value A' ], "Got all replies";

# Test callbacks
my @replies;
sub cb {
    shift;
    push @replies, [ @_ ];
}

$redis->send_command( 'SET', 'CB A', 'AAA', RedisDB::IGNORE_REPLY );
$redis->send_command( 'GET', 'CB A', \&cb );
$redis->set( 'CB B', 'BBB', RedisDB::IGNORE_REPLY );
$redis->get( 'CB B', \&cb );
is $redis->ping, "PONG", "can execute while sent some commands with callbacks";
$redis->send_command( 'RPUSH', 'CB LIST', 'CCC', RedisDB::IGNORE_REPLY );
$redis->rpush( 'CB LIST', 'DDD', RedisDB::IGNORE_REPLY );
$redis->send_command( 'RPUSH', 'CB LIST', 'EEE' );
$redis->send_command( 'LRANGE', 'CB LIST', 0, 3,
    sub { my $redis = shift; cb( $redis, 'LIST', @_ ) } );
is $redis->replies_to_fetch, 1, "One reply to fetch";
is $redis->get_reply,        3, "Added 3 elements to the list";
$redis->send_command('PING');
is $redis->get_reply, 'PONG', "Pinged server";
eq_or_diff \@replies,
  [ [qw(AAA)], [qw(BBB)], [ 'LIST', [qw(CCC DDD EEE)] ], ],
  "Callback was called with correct arguments";
is $redis->replies_to_fetch, 0, "No replies to fetch";

my $incr;
my $icnt;
$incr = sub {
    my ( $redis, $res ) = @_;
    $icnt = $res;
    $redis->rpush( 'mainlist', $res, RedisDB::IGNORE_REPLY );
    $redis->incr( 'mainloop', $incr ) if $res < 5;
};
$redis->incr( 'mainloop', $incr );
$redis->send_command('PING');
$redis->mainloop;
is $icnt, 5, "last result of increment is 5";
is $redis->get_reply, 'PONG', "got PONG after mainloop";
eq_or_diff $redis->lrange( 'mainlist', 0, -1 ), [ 1 .. 5 ], "Correct mainlist value";

$redis->set( "DB_number", 0 );
is $redis->select(1),        'OK',  "selected DB 1";
is $redis->selected_database, 1, "selected_database 1";
is $redis->get("DB_number"), undef, "DB_number is not defined";
$redis->set( "DB_number", 1 );
is $redis->get("DB_number"), 1, "DB number 1";
is $redis->selected_database, 1, "selected_database 1";
$redis->quit;
is $redis->get("DB_number"), 1,    "after reconnecting DB number is still 1";
is $redis->select(0),        'OK', "selected DB 0";
is $redis->selected_database, 0, "selected_database 0";
is $redis->get("DB_number"), 0,    "DB number 0";

done_testing;
END { $redis->shutdown if $redis; }
