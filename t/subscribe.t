use Test::Most 0.22;
use lib 't';
use RedisServer;
use RedisDB;

my $server = RedisServer->start;
plan( skip_all => "Can't start redis-server" ) unless $server;
my $redis = RedisDB->new( host => 'localhost', port => $server->{port} );

unless ( my $pid = fork ) {
    die "Couldn't fork: $!" unless defined $pid;

    # give it some time to subscribe
    sleep 1;

    # this nobody should receive
    $redis->publish( "no_channel", "message" );

    # parent should unsubscribe from abc, foo, and news.* after this
    $redis->publish( "abc", "abc_pass" );
    $redis->publish( "bar", "bar message" );

    # give it some time to unsubscribe
    sleep 1;
    $redis->publish( "abc",        "It sholdn't receive this message" );
    $redis->publish( "news.test",  "Test unfortunately failed" );
    $redis->publish( "bar",        "bar message" );
    $redis->publish( "boo",        "boo message" );
    $redis->publish( "other.quit", "quit" );

    # wait for first subscription loop to exit
    sleep 1;
    $redis->publish( "quit", "quit" );

    $redis = 0;
    exit 0;
}

my %counts;

is $redis->set( "running test", "subscribe.t" ), "OK", "Successfully set value in DB";

$SIG{ALRM} = sub { die "subscription loop didn't exit" };
alarm 7;
$redis->subscription_loop(
    subscribe        => [ abc      => \&abc_cb,  'foo', 'bar' ],
    psubscribe       => [ 'news.*' => \&news_cb, 'other.*' ],
    default_callback => \&def_cb,
);
pass "Left first subscription loop";
$redis->subscription_loop(
    subscribe => [ quit => sub { $_[0]->unsubscribe('quit') } ],
);
pass "Left second subscription loop";
alarm 0;

eq_or_diff \%counts, { bar => 2, other => 1, boo => 1 }, "Correct numbers of messages";

is $redis->get("running test"), "subscribe.t", "execute available again";

sub abc_cb {
    my ( $redis, $chan, $ptrn, $message ) = @_;

    is $chan,    'abc',      "received message for abc";
    is $ptrn,    undef,      "no pattern defined";
    is $message, "abc_pass", "message is abc_pass";
    dies_ok { $redis->get("running test") } "execute is not available in subscription mode";
    dies_ok { $redis->send_command( "get", "running test" ) }
    "send_command is not available in subscription mode";
    eq_or_diff [ sort $redis->subscribed ],  [qw(abc bar foo)],    "Subscribed to abc foo bar";
    eq_or_diff [ sort $redis->psubscribed ], [qw(news.* other.*)], "Psubscribed to news.* other.*";
    $redis->unsubscribe( 'abc', 'foo' );
    $redis->punsubscribe('news.*');
    $redis->subscribe( 'boo', \&boo_cb );
    return;
}

sub boo_cb {
    my ( $redis, $chan, $ptrn, $message ) = @_;

    $counts{boo}++;
    is $chan,    'boo',         "received message for boo";
    is $ptrn,    undef,         "no pattern defined";
    is $message, "boo message", "correct message for boo";
    return;
}

sub news_cb {
    fail "news_cb callback sould not be invoked";
    return;
}

sub def_cb {
    my ( $redis, $chan, $ptrn, $message ) = @_;

    if ( $chan eq 'bar' ) {
        $counts{bar}++;
        pass "Correct channel";
        is $message, "bar message", "message for bar";
        is $ptrn,    undef,         "bar is not pattern match";
    }
    elsif ( $ptrn && $ptrn eq 'other.*' ) {
        $counts{other}++;
        is $chan,    'other.quit', "Received message for other.quit";
        is $message, 'quit',       "Correct message for other.quit";
        $redis->unsubscribe;
        $redis->punsubscribe;
    }
    else {
        fail "No messages for $chan should be received";
    }

    return;
}

my $pub = RedisDB->new( host => 'localhost', port => $server->{port} );
my $sub = RedisDB->new( host => 'localhost', port => $server->{port} );
$sub->subscribe('baz', sub { $_[0]->unsubscribe('baz') });
$sub->psubscribe('un*', sub { $_[0]->punsubscribe });
my $rep =$sub->get_reply;
is $rep->[0], 'subscribe', "got subscribe reply";
$rep =$sub->get_reply;
is $rep->[0], 'psubscribe', "got psubscribe reply";
dies_ok { $sub->get('key') } "get is not allowed in subscription mode";

$pub->publish('unexpected', 'msg 1');
$pub->publish('baz', 'msg 2');

$rep = $sub->get_reply;
eq_or_diff $rep, ['pmessage', 'un*', 'unexpected', 'msg 1'], "got msg 1 on unexpected channel";
$rep = $sub->get_reply;
eq_or_diff $rep, ['message', 'baz', 'msg 2'], "got msg 2 on baz channel";

$sub->unsubscribe;
$sub->punsubscribe;

done_testing;
END { $redis->shutdown if $redis; }
