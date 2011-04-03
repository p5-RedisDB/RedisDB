use Test::Most 0.22 qw(no_plan);

use ok 'RedisDB';
use utf8;

subtest "Request encoding" => \&request_encoding;

my $redis = RedisDB->new(lazy => 1);

subtest "One line reply" => \&one_line_reply;

subtest "Integer reply" => \&integer_reply;

subtest "Bulk reply" => \&bulk_reply;

subtest "Multi-bulk reply" => \&multi_bulk_reply;

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
        RedisDB::_build_redis_request($command),
        join( $lf, '*1', '$4', 'test', '' ),
        "Single command is ok"
    );
    eq_or_diff( RedisDB::_build_redis_request( $command, $int ),
        join( $lf, '*2', '$4', 'test', '$2', '12', '' ), "Integer" );
    eq_or_diff(
        RedisDB::_build_redis_request( $command, $string ),
        join( $lf, '*2', '$4', 'test', '$24', $string, '' ),
        "ASCII string"
    );
    my $ulen = length $ustring;
    ok $ulen > 7, "Length is in bytes";
    eq_or_diff(
        RedisDB::_build_redis_request( $command, $ustring, $string ),
        join( $lf, '*3', '$4', 'test', "\$$ulen", $ustring, '$24', $string, '' ),
        "unicode string"
    );
    my $blen = length $binary;
    eq_or_diff(
        RedisDB::_build_redis_request( $command, $binary, $ustring ),
        join( $lf, '*3', '$4', 'test', "\$$blen", $binary, "\$$ulen", $ustring, '' ),
        "binary string"
    );
}

sub one_line_reply {
    $redis->{_buffer} = "+";
    ok( !$redis->_parse_reply, "+" );
    $redis->{_buffer} .= "OK";
    ok( !$redis->_parse_reply, "OK" );
    $redis->{_buffer} .= "\015";
    ok( !$redis->_parse_reply, "\\015" );
    $redis->{_buffer} .= "\012+And here we have something long\015\012-OK\015";
    ok( $redis->_parse_reply, "Got reply" );
    eq_or_diff( shift @{$redis->{_replies}}, [ '+', 'OK' ], "got first OK" );
    ok( $redis->_parse_reply, "Got reply" );
    eq_or_diff( shift @{$redis->{_replies}}, [ '+', 'And here we have something long' ] );
    ok( !$redis->_parse_reply, "-OK\\015" );
    $redis->{_buffer} .= "OK\015\012";
    ok( $redis->_parse_reply, "Got reply" );
    eq_or_diff( shift @{$redis->{_replies}}, [ '-', "OK\015OK" ], "got error reply with \\r in it" );
}

sub integer_reply {
    $redis->{_buffer} = ":";
    ok( !$redis->_parse_reply, ":" );
    $redis->{_buffer} .= "12";
    ok( !$redis->_parse_reply, "'12'" );
    $redis->{_buffer} .= "34\015\012";
    ok( $redis->_parse_reply, "Got reply" );
    eq_or_diff( shift @{$redis->{_replies}}, [ ':', 1234 ], "got 1234" );
    $redis->{_buffer} .= ":0\015\012:-123\015\012";
    ok( $redis->_parse_reply, "Got reply" );
    eq_or_diff( shift @{$redis->{_replies}}, [ ':', 0 ],    "got zero" );
    ok( $redis->_parse_reply, "Got reply" );
    eq_or_diff( shift @{$redis->{_replies}}, [ ':', -123 ], "got -120" );
    my $redis2 = RedisDB->new(lazy => 1);
    $redis2->{_buffer} = ":123a\015\012";
    dies_ok { $redis2->_parse_reply } "Dies on invalid integer reply";
}

sub bulk_reply {
    $redis->{_buffer} = '$';
    ok( !$redis->_parse_reply, '$' );
    $redis->{_buffer} .= "6\015\012foobar";
    ok( !$redis->_parse_reply, '6\\r\\nfoobar' );
    $redis->{_buffer} .= "\015\012\$-1\015\012\$0\015\012\015\012";
    ok( $redis->_parse_reply, "Got reply" );
    eq_or_diff( shift @{$redis->{_replies}}, [ '$', 'foobar' ], 'got foobar' );
    ok( $redis->_parse_reply, "Got reply" );
    is_deeply( shift @{$redis->{_replies}}, [ '$', undef ], 'got undef' );
    ok( $redis->_parse_reply, "Got reply" );
    eq_or_diff( shift @{$redis->{_replies}}, [ '$', '' ], 'got empty string' );
}

sub multi_bulk_reply {
    $redis->{_buffer} = "*4\015\012\$3\015\012foo\015\012\$";
    ok( !$redis->_parse_reply, '*4$3foo$' );
    $redis->{_buffer} .= "-1\015\012\$0\015\012\015\012\$5\015\012Hello";
    ok( !$redis->_parse_reply, '*4$3foo$-1$0$5Hello' );
    $redis->{_buffer} .= "\015\012";
    ok( $redis->_parse_reply, "Got reply" );
    is_deeply(
        shift @{$redis->{_replies}},
        [ '*', [ 'foo', undef, '', 'Hello' ] ],
        'got correct reply foo/undef//Hello'
    );
    $redis->{_buffer} .= "*0\015\012*-1\015\012";
    ok( $redis->_parse_reply, "Got reply" );
    eq_or_diff( shift @{$redis->{_replies}}, [ '*', [] ], '*0 is empty list' );
    ok( $redis->_parse_reply, "Got reply" );
    is_deeply( shift @{$redis->{_replies}}, [ '*', undef ], '*1 is undef' );
}

