package RedisDB::Parse::Redis_XS;
use strict;
use warnings;
our $VERSION = "1.99_02";
my $XS_VERSION = $VERSION;
$VERSION = eval $VERSION;

require XSLoader;
XSLoader::load("RedisDB", $XS_VERSION);

sub new {
    my ($class, %params) = @_;
    return _new($params{redisdb}, $params{utf8} ? 1 : 0);
}

1;
