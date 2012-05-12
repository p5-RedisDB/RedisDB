use Test::Most 0.22;

use RedisDB::Parse::Redis;
use utf8;

my $parser = RedisDB::Parse::Redis->new();
my $lf     = "\015\012";

subtest "Request encoding" => \&request_encoding;

subtest "One line reply" => \&one_line_reply;

subtest "Integer reply" => \&integer_reply;

subtest "Bulk reply" => \&bulk_reply;

subtest "Multi-bulk reply" => \&multi_bulk_reply;

subtest "Deep nested multi-bulk reply" => \&nested_mb_reply;

subtest "Transaction" => \&transaction;

done_testing;

sub request_encoding {
    my $command = 'test';
    my $int     = 12;
    my $string  = "Short string for testing";
    my $ustring = "上得山多终遇虎";

    use bytes;
    my $binary =
      "Some strings may contain\n linebreaks\0 \r\n or zero terminated strings\0 or some latin1 chars \110";

    eq_or_diff(
        $parser->build_request('test'),
        join( $lf, '*1', '$4', 'test', '' ),
        "Single command is ok"
    );
    eq_or_diff( $parser->build_request( $command, $int ),
        join( $lf, '*2', '$4', 'test', '$2', '12', '' ), "Integer" );
    eq_or_diff(
        $parser->build_request( $command, $string ),
        join( $lf, '*2', '$4', 'test', '$24', $string, '' ),
        "ASCII string"
    );
    my $ulen = length $ustring;
    ok $ulen > 7, "Length is in bytes";
    eq_or_diff(
        $parser->build_request( $command, $ustring, $string ),
        join( $lf, '*3', '$4', 'test', "\$$ulen", $ustring, '$24', $string, '' ),
        "unicode string"
    );
    my $blen = length $binary;
    eq_or_diff(
        $parser->build_request( $command, $binary, $ustring ),
        join( $lf, '*3', '$4', 'test', "\$$blen", $binary, "\$$ulen", $ustring, '' ),
        "binary string"
    );
}

my @replies;

sub cb {
    shift;
    push @replies, shift;
}

sub one_line_reply {
    @replies = ();
    $parser->add_callback( \&cb ) for 1 .. 3;
    is $parser->callbacks, 3, "Three callbacks were added";
    $parser->add("+");
    is @replies, 0, "+";
    $parser->add("OK");
    is @replies, 0, "+OK";
    $parser->add("\015");
    is @replies, 0, "+OK\\r";
    $parser->add("\012+And here we have something long$lf-OK\015");
    is @replies, 2, "Found 2 replies";
    is $parser->callbacks, 1, "One callback left";
    eq_or_diff \@replies, [ "OK", "And here we have something long" ],
      "OK, And here we have something long";
    @replies = ();
    $parser->add("OK$lf");
    is @replies, 1, "Got a reply";
    isa $replies[0], "Redis::Error";
    eq_or_diff( "$replies[0]", "OK\015OK", "got an error reply with \\r in it" );
}

sub integer_reply {
    @replies = ();
    $parser->add_callback( \&cb ) for 1 .. 3;
    $parser->add(":");
    is @replies, 0, ":";
    $parser->add("12");
    is @replies, 0, ":12";
    $parser->add("34$lf");
    is @replies, 1, "Got a reply";
    eq_or_diff shift(@replies), 1234, "got 1234";
    $parser->add(":0$lf:-123$lf");
    is @replies, 2, "Got two replies";
    eq_or_diff \@replies, [ 0, -123 ], "got 0 and -123";
    my $parser2 = RedisDB::Parse::Redis->new();
    dies_ok { $parser2->add(":123a$lf") } "Dies on invalid integer reply";
}

sub bulk_reply {
    @replies = ();
    $parser->add_callback( \&cb ) for 1 .. 3;
    $parser->add('$');
    is @replies, 0, '$';
    $parser->add("6${lf}foobar");
    is @replies, 0, '$6\\r\\nfoobar';
    $parser->add("${lf}\$-1${lf}\$0$lf$lf");
    is @replies, 3, "Got three replies";
    eq_or_diff \@replies, [ 'foobar', undef, '' ], "Got foobar, undef, and empty string";
}

sub multi_bulk_reply {
    @replies = ();
    $parser->add_callback( \&cb ) for 1 .. 4;
    $parser->add("*4$lf\$3${lf}foo$lf\$");
    is @replies, 0, '*4$3foo$';
    $parser->add("-1${lf}\$0$lf$lf\$5${lf}Hello");
    is @replies, 0, '*4$3foo$-1$0$5Hello';
    $parser->add($lf);
    is @replies, 1, "Got a reply";
    eq_or_diff shift(@replies), [ 'foo', undef, '', 'Hello' ], 'got correct reply foo/undef//Hello';
    $parser->add("*0$lf*-1$lf");
    is @replies, 2, "Got two replies";
    eq_or_diff \@replies, [ [], undef ], "*0 is empty list, *-1 is undef";

    # redis docs don't say that this is possible, but that's what I got
    @replies = ();
    $parser->add("*3$lf\$9${lf}subscribe$lf\$3${lf}foo$lf:2$lf");
    is @replies, 1, "Got a reply";
    eq_or_diff $replies[0], [qw(subscribe foo 2)], 'subscribe foo :2';
}

sub nested_mb_reply {
    @replies = ();
    $parser->add_callback( \&cb );
    $parser->add( "*3${lf}"
          . "*4${lf}:5${lf}:1336734898${lf}:43${lf}"
          . "*2${lf}\$3${lf}get${lf}\$4${lf}test${lf}"
          . "*4${lf}:4${lf}:1336734895${lf}:175${lf}"
          . "*3${lf}\$3${lf}set${lf}\$4${lf}test${lf}\$2${lf}43${lf}" );
    is @replies, 0, 'waits for the last chunk';
    $parser->add(
        "*4${lf}:3${lf}:1336734889${lf}:20${lf}" . "*2${lf}\$7${lf}slowlog${lf}\$3${lf}len${lf}" );
    is @replies, 1, "Got a reply";
    my $exp = [
        [ 5, 1336734898, 43,  [ 'get',     'test' ], ],
        [ 4, 1336734895, 175, [ 'set',     'test', '43' ], ],
        [ 3, 1336734889, 20,  [ 'slowlog', 'len' ], ],
    ];
    eq_or_diff shift(@replies), $exp, 'got correct nested multi-bulk reply';
}

sub transaction {
    @replies = ();
    $parser->add_callback( \&cb ) for 1 .. 4;
    $parser->add("*7$lf+OK$lf:5$lf:6$lf:7$lf:8$lf*4$lf\$4$lf");
    is @replies, 0, 'Incomplete result - not parsed';
    $parser->add("this$lf\$2${lf}is$lf\$1${lf}a$lf\$4${lf}list$lf");
    is @replies, 0, 'After encapsulated multi-bulk part - still not parsed';
    $parser->add("\$5${lf}value$lf");
    is @replies, 1, 'Got a reply';
    eq_or_diff(
        shift(@replies),
        [ qw(OK 5 6 7 8), [qw(this is a list)], 'value' ],
        "Successfuly parsed a transaction reply"
    );
    $parser->add(
        "*6$lf+OK$lf:1$lf:2$lf:3$lf:4$lf*4$lf\$4${lf}this$lf\$2${lf}is${lf}\$1${lf}a$lf\$4${lf}list$lf"
    );
    is @replies, 1, 'Got a reply';
    eq_or_diff(
        shift(@replies),
        [ qw(OK 1 2 3 4), [qw(this is a list)] ],
        "Parsed with list in the end too"
    );
    $parser->add( "*4$lf*0$lf+OK$lf*-1$lf*2$lf\$2${lf}aa$lf\$2${lf}bb$lf" );
    is @replies, 1, 'Got a reply';
    eq_or_diff shift(@replies), [ [], 'OK', undef, [qw(aa bb)] ],
      "Parsed reply with empty list and undef";
    $parser->add( "*3$lf*0$lf-Oops$lf+OK$lf" );
    is @replies, 1, 'Got a reply with error inside';
    my $reply = shift @replies;
    eq_or_diff $reply->[0], [], "  has empty list";
    isa_ok $reply->[1], "RedisDB::Error", "  has error object";
    is "$reply->[1]", "Oops", "  Oops";
    is $reply->[2], "OK", "  has OK";
}

