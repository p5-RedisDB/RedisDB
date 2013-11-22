use 5.010;
use strict;
use warnings;

use Benchmark qw(cmpthese);
use RedisDB::Parse::Redis_XS;
use RedisDB::Parse::Redis_PP;
use Protocol::Redis;
use Protocol::Redis::XS;
use Data::Dumper;

$Data::Dumper::Sortkeys = 1;

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

my $rdb_pp = RedisDB::Parse::Redis_PP->new;
$rdb_pp->set_default_callback( sub { 1 } );
my $rdb_xs = RedisDB::Parse::Redis_XS->new;
$rdb_xs->set_default_callback( sub { 1 } );

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
$pr_pp->on_message( sub { 1 } );
$pr_xs->on_message( sub { 1 } );

cmpthese - 5,
  {
    "RedisDB PP"          => sub { $rdb_pp->add($reply)  for 1 .. 1000 },
    "RedisDB XS"          => sub { $rdb_xs->add($reply)  for 1 .. 1000 },
    "Protocol::Redis"     => sub { $pr_pp->parse($reply) for 1 .. 1000 },
    "Protocol::Redis::XS" => sub { $pr_xs->parse($reply) for 1 .. 1000 },
  };
