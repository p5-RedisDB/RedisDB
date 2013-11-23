use 5.010;
use strict;
use warnings;

use Benchmark qw(cmpthese);
use RedisDB::Parse::Redis_XS;
use RedisDB::Parse::Redis_PP;
use Protocol::Redis;
use Protocol::Redis::XS;
use Redis::Parser::XS;
use Data::Dumper;

$Data::Dumper::Sortkeys = 1;

say "Please read source code to make sure you understand what this script is benchmarking";

sub eq_or_die {
    my ( $got, $expected, $parser ) = @_;
    unless ( Dumper($got) eq Dumper($expected) ) {
        die "Invalid result in $parser: " . Dumper($got);
    }
}

my $reply = join "\r\n", qw(
  *3
  :123
  $-1
  $9
  hhhhhhhhh
  ), "";

my $true = sub { 1 };
my $rdb_pp = RedisDB::Parse::Redis_PP->new;
$rdb_pp->set_default_callback($true);
my $rdb_xs = RedisDB::Parse::Redis_XS->new;
$rdb_xs->set_default_callback($true);

my $rdb_parsed = [ 123, undef, 'hhhhhhhhh' ];
$rdb_pp->add_callback( sub { eq_or_die( $_[1], $rdb_parsed, "RedisDB PP" ) } );
$rdb_pp->add($reply);
$rdb_xs->add_callback( sub { eq_or_die( $_[1], $rdb_parsed, "RedisDB XS" ) } );
$rdb_xs->add($reply);

my $pr_pp = Protocol::Redis->new( api => 1 );
my $pr_xs = Protocol::Redis::XS->new( api => 1 );

my $pr_parsed = {
    data => [
        {
            data => '123',
            type => ':'
        },
        {
            type => '$',
            data => undef
        },
        {
            type => '$',
            data => 'hhhhhhhhh'
        }
    ],
    type => '*'
};
$pr_pp->on_message( sub { eq_or_die( $_[1], $pr_parsed, "Protocol::Redis" ) } );
$pr_pp->parse($reply);

# Protocol::Redis::XS returns slightly different result
$pr_parsed->{data}[0]{data} = 123;
$pr_xs->on_message( sub { eq_or_die( $_[1], $pr_parsed, "Protocol::Redis::XS" ) } );
$pr_xs->parse($reply);
$pr_pp->on_message($true);
$pr_xs->on_message($true);

my $parse_redis_parsed = [ '*', [ [ ':', '123' ], undef, 'hhhhhhhhh' ] ];
parse_redis $reply, \my @res;
eq_or_die $res[0], $parse_redis_parsed, "Parse::Redis::XS";

# comparing other parsers against bare parse_redis is not correct
sub redis_parser {
    state $buf = '';
    $buf .= shift;
    my $len = parse_redis $buf, \my @res;
    substr $buf, 0, $len, '';
    $true->($_) for @res;
}

my @replies = (
    [ "Status message",  "+OK\r\n" ],
    [ "Bulk reply",      "\$3\r\nfoo\r\n" ],
    [ "Multibulk reply", $reply ]
);

for (@replies) {
    my ( $test, $reply ) = @$_;
    say "\nParsing $test\n";
    cmpthese - 5,
      {
        "RedisDB PP"               => sub { $rdb_pp->add($reply)  for 1 .. 10000 },
        "RedisDB XS"               => sub { $rdb_xs->add($reply)  for 1 .. 10000 },
        "Protocol::Redis"          => sub { $pr_pp->parse($reply) for 1 .. 10000 },
        "Protocol::Redis::XS"      => sub { $pr_xs->parse($reply) for 1 .. 10000 },
        "wrapped Parse::Redis::XS" => sub { redis_parser($reply)  for 1 .. 10000 },
        "bare Parse::Redis::XS" => sub { parse_redis $reply, \my @res for 1 .. 10000 },
      };
}
