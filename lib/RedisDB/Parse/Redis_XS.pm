package RedisDB::Parse::Redis_XS;
use strict;
use warnings;
our $VERSION = "2.13_02";
my $XS_VERSION = $VERSION;
$VERSION = eval $VERSION;

=head1 NAME

RedisDB::Parse::Redis_XS - redis protocol parser for RedisDB

=head1 DESCRIPTION

XS implementation of L<RedisDB::Parse::Redis>. You should not use this
module directly.

=cut

require XSLoader;
XSLoader::load("RedisDB", $XS_VERSION);

sub new {
    my ($class, %params) = @_;
    return _new($params{redisdb}, $params{utf8} ? 1 : 0);
}

1;

__END__

=head1 SEE ALSO

L<RedisDB::Parse::Redis>

=head1 AUTHOR

Pavel Shaydo, C<< <zwon at cpan.org> >>

=head1 LICENSE AND COPYRIGHT

Copyright 2011-2013 Pavel Shaydo.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut
