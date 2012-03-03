use Test::Most 0.22;

use ok 'RedisDB';
use utf8;

subtest "Request encoding" => \&request_encoding;

my $redis = RedisDB->new( lazy => 1 );

subtest "One line reply" => \&one_line_reply;

subtest "Integer reply" => \&integer_reply;

subtest "Bulk reply" => \&bulk_reply;

subtest "Multi-bulk reply" => \&multi_bulk_reply;

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

    my $lf = "\015\012";
    eq_or_diff(
        RedisDB::_build_redis_request( $redis, $command ),
        join( $lf, '*1', '$4', 'test', '' ),
        "Single command is ok"
    );
    eq_or_diff( RedisDB::_build_redis_request( $redis, $command, $int ),
        join( $lf, '*2', '$4', 'test', '$2', '12', '' ), "Integer" );
    eq_or_diff(
        RedisDB::_build_redis_request( $redis, $command, $string ),
        join( $lf, '*2', '$4', 'test', '$24', $string, '' ),
        "ASCII string"
    );
    my $ulen = length $ustring;
    ok $ulen > 7, "Length is in bytes";
    eq_or_diff(
        RedisDB::_build_redis_request( $redis, $command, $ustring, $string ),
        join( $lf, '*3', '$4', 'test', "\$$ulen", $ustring, '$24', $string, '' ),
        "unicode string"
    );
    my $blen = length $binary;
    eq_or_diff(
        RedisDB::_build_redis_request( $redis, $command, $binary, $ustring ),
        join( $lf, '*3', '$4', 'test', "\$$blen", $binary, "\$$ulen", $ustring, '' ),
        "binary string"
    );
}

my $reply;

sub cb {
    shift;    # redis
    $reply = shift;
}

sub one_line_reply {
    $redis->{_callbacks} = [ \&cb, \&cb, \&cb ];
    $redis->{_buffer} = "+";
    ok( !$redis->_parse_reply, "+" );
    $redis->{_buffer} .= "OK";
    ok( !$redis->_parse_reply, "OK" );
    $redis->{_buffer} .= "\015";
    ok( !$redis->_parse_reply, "\\015" );
    $redis->{_buffer} .= "\012+And here we have something long\015\012-OK\015";
    ok( $redis->_parse_reply, "Got reply" );
    eq_or_diff( $reply, 'OK', "got first OK" );
    ok( $redis->_parse_reply, "Got reply" );
    eq_or_diff( $reply, 'And here we have something long' );
    ok( !$redis->_parse_reply, "-OK\\015" );
    $redis->{_buffer} .= "OK\015\012";
    ok( $redis->_parse_reply, "Got reply" );
    isa $reply, "Redis::Error";
    eq_or_diff( "$reply", "OK\015OK", "got error reply with \\r in it" );
}

sub integer_reply {
    $redis->{_callbacks} = [ \&cb, \&cb, \&cb ];
    $redis->{_buffer} = ":";
    ok( !$redis->_parse_reply, ":" );
    $redis->{_buffer} .= "12";
    ok( !$redis->_parse_reply, "'12'" );
    $redis->{_buffer} .= "34\015\012";
    ok( $redis->_parse_reply, "Got reply" );
    eq_or_diff( $reply, 1234, "got 1234" );
    $redis->{_buffer} .= ":0\015\012:-123\015\012";
    ok( $redis->_parse_reply, "Got reply" );
    eq_or_diff( $reply, 0, "got zero" );
    ok( $redis->_parse_reply, "Got reply" );
    eq_or_diff( $reply, -123, "got -120" );
    my $redis2 = RedisDB->new( lazy => 1 );
    $redis2->{_buffer} = ":123a\015\012";
    dies_ok { $redis2->_parse_reply } "Dies on invalid integer reply";
}

sub bulk_reply {
    $redis->{_callbacks} = [ \&cb, \&cb, \&cb ];
    $redis->{_buffer} = '$';
    ok( !$redis->_parse_reply, '$' );
    $redis->{_buffer} .= "6\015\012foobar";
    ok( !$redis->_parse_reply, '6\\r\\nfoobar' );
    $redis->{_buffer} .= "\015\012\$-1\015\012\$0\015\012\015\012";
    ok( $redis->_parse_reply, "Got reply" );
    eq_or_diff( $reply, 'foobar', 'got foobar' );
    ok( $redis->_parse_reply, "Got reply" );
    eq_or_diff( $reply, undef, 'got undef' );
    ok( $redis->_parse_reply, "Got reply" );
    eq_or_diff( $reply, '', 'got empty string' );
}

sub multi_bulk_reply {
    $redis->{_callbacks} = [ \&cb, \&cb, \&cb, \&cb ];
    $redis->{_buffer} = "*4\015\012\$3\015\012foo\015\012\$";
    ok( !$redis->_parse_reply, '*4$3foo$' );
    $redis->{_buffer} .= "-1\015\012\$0\015\012\015\012\$5\015\012Hello";
    ok( !$redis->_parse_reply, '*4$3foo$-1$0$5Hello' );
    $redis->{_buffer} .= "\015\012";
    ok( $redis->_parse_reply, "Got reply" );
    eq_or_diff(
        $reply,
        [ 'foo', undef, '', 'Hello' ],
        'got correct reply foo/undef//Hello'
    );
    $redis->{_buffer} .= "*0\015\012*-1\015\012";
    ok( $redis->_parse_reply, "Got reply" );
    eq_or_diff( $reply, [], '*0 is empty list' );
    ok( $redis->_parse_reply, "Got reply" );
    eq_or_diff( $reply, undef, '*1 is undef' );

    # redis docs don't say that this is possible, but that's what I got
    $redis->{_buffer} .= "*3\015\012\$9\015\012subscribe\015\012\$3\015\012foo\015\012:2\015\012";
    ok( $redis->_parse_reply, "Got reply" );
    eq_or_diff( $reply, [qw(subscribe foo 2)], 'subscribe foo :2' );
}

sub transaction {
    $redis->{_callbacks} = [ \&cb, \&cb, \&cb ];
    $redis->{_buffer} =
      "*7\015\012+OK\015\012:5\015\012:6\015\012:7\015\012:8\015\012*4\015\012\$4\015\012";
    ok( !$redis->_parse_reply, 'incomplete result - not parsed' );
    $redis->{_buffer} .=
      "this\015\012\$2\015\012is\015\012\$1\015\012a\015\012\$4\015\012list\015\012";
    ok( !$redis->_parse_reply, 'after encapsulated multi-bulk part - still not parsed' );
    $redis->{_buffer} .= "\$5\015\012value\015\012";
    ok( $redis->_parse_reply, 'reply ready' );
    eq_or_diff(
        $reply,
        [ qw(OK 5 6 7 8), [qw(this is a list)], 'value' ],
        "complex transaction result successfuly parsed"
    );
    $redis->{_buffer} = "*6\r\n+OK\r\n:1\r\n:2\r\n:3\r\n:4\r\n*4\r\n\$4\r\nthis\r\n\$2\r\nis\r\n\$1\r\na\r\n\$4\r\nlist\r\n";
    ok( $redis->_parse_reply, 'reply ready' );
    eq_or_diff(
        $reply,
        [ qw(OK 1 2 3 4), [qw(this is a list)] ],
        "parsed with list in the end too"
    );
    $redis->{_buffer} = "*4\015\012*0\015\012+OK\015\012*-1\015\012*2\015\012\$2\015\012aa\015\012\$2\015\012bb\015\012";
    ok( $redis->_parse_reply, 'empty list in transaction result' );
    eq_or_diff( $reply, [ [], 'OK', undef, [ qw(aa bb) ] ] );
}

