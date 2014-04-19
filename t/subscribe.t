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
$redis->set("__test__", "__test__", RedisDB::IGNORE_REPLY);

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

subtest "subscriptions outside of subscription_loop" => sub {
    my $pub = RedisDB->new( host => 'localhost', port => $server->{port} );
    my $sub = RedisDB->new( host => 'localhost', port => $server->{port} );
    my $received;
    my $cb = sub { $received->{ $_[1] } = $_[3] };
    $sub->subscribe( 'baz', $cb );
    $sub->psubscribe( 'un*', $cb );
    my $rep = $sub->get_reply;
    is $rep->[0], 'subscribe', "got subscribe reply";
    $rep = $sub->get_reply;
    is $rep->[0], 'psubscribe', "got psubscribe reply";
    ok !$received, "p?subscribe messages didn't invoke callback";
    dies_ok { $sub->get('key') } "get is not allowed in subscription mode";

    if ( $pub->version >= 2.008 ) {
        subtest "PUBSUB" => sub {
            eq_or_diff $pub->pubsub('CHANNELS'), ['baz'], "Only baz channel is active";
            eq_or_diff $pub->pubsub_channels,    ['baz'], "Only baz channel is active";
            eq_or_diff $pub->pubsub_numsub( 'baz', 'boo' ), [ 'baz', 1, 'boo', 0 ],
              "baz has one subscriber, boo has none";
            eq_or_diff $pub->pubsub_numpat, 1, "one pattern subscriber";
        };
    }
    else {
        diag "Not testing PUBSUB command. Requires redis-server >= 2.8.0";
    }

    $pub->publish( 'unexpected', 'msg 1' );
    $pub->publish( 'baz',        'msg 2' );

    $rep = $sub->get_reply;
    eq_or_diff $rep, [ 'pmessage', 'un*', 'unexpected', 'msg 1' ],
      "got msg 1 from the unexpected channel";
    is $received->{unexpected}, 'msg 1', "callback for unexpected channel was invoked";
    $rep = $sub->get_reply;
    eq_or_diff $rep, [ 'message', 'baz', 'msg 2' ], "got msg 2 from the baz channel";
    is $received->{baz}, 'msg 2', "callback for baz channel was invoked";

    {
        # this also checks how recv in get_reply deals with interrupts
        local $SIG{ALRM} = sub { $pub->publish( 'baz', 'msg 3' ); delete $SIG{ALRM}; alarm 3; };
        alarm 1;
        $rep = $sub->get_reply;
        alarm 0;
        eq_or_diff $rep, [ 'message', 'baz', 'msg 3' ], "got msg 3 from the baz channel";
        is $received->{baz}, 'msg 3', "callback for baz channel was invoked";
    }

    $sub->unsubscribe;
    $sub->punsubscribe;
};

subtest "unsubscribe without psubscriptions (issue #18)" => sub {
    my $sub = RedisDB->new( host => 'localhost', port => $server->{port} );
    $sub->subscribe('foo');
    $sub->unsubscribe;
    pass "Unsubscribed";
    $sub->reset_connection;
    $sub->psubscribe('foo*');
    $sub->punsubscribe;
    pass "Punsubscribed";
};

subtest "subscribe before starting subscription loop" => sub {
    unless ( my $pid = fork ) {
        die "Couldn't fork: $!" unless defined $pid;
        sleep 1;
        $redis->publish( bar          => 'bar message' );
        $redis->publish( 'other.quit' => 'quit' );
        exit 0;
    }
    my $sub = RedisDB->new( host => 'localhost', port => $server->{port} );
    $sub->subscribe( 'bar' => \&def_cb );
    $counts{bar}   = 0;
    $counts{other} = 0;
    $sub->subscription_loop(
        psubscribe       => ['other.*'],
        default_callback => \&def_cb,
    );
    is $counts{bar},   1, "got one message for bar";
    is $counts{other}, 1, "got one message for other";
};

done_testing;
