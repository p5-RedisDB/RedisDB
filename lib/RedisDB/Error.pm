package RedisDB::Error;

use strict;
use base 'RedisDB::Parser::Error';
our $VERSION = "2.57";
$VERSION = eval $VERSION;

=head1 NAME

RedisDB::Error - Error class for RedisDB

=head1 SYNOPSIS

    sub callback {
        my ($redis, $reply) = @_;
        die "$reply" if ref $reply eq 'RedisDB::Error';
        # do something with reply
    }

=head1 DESCRIPTION

Object of this class maybe passed as argument to callback specified in
I<send_command_cb> if redis server return error.  In string context object
returns description of the error. This class inherits from
L<RedisDB::Parser::Error>.

=cut

*new = \&RedisDB::Parser::Error::new;

package RedisDB::Error::EAGAIN;
our @ISA = qw(RedisDB::Error);

package RedisDB::Error::DISCONNECTED;
our @ISA = qw(RedisDB::Error);

package RedisDB::Error::MOVED;
our @ISA = qw(RedisDB::Parser::Error::MOVED RedisDB::Error);

package RedisDB::Error::ASK;
our @ISA = qw(RedisDB::Parser::Error::ASK RedisDB::Error);

1;

__END__

=head1 SEE ALSO

L<RedisDB>

=head1 AUTHOR

Pavel Shaydo, C<< <zwon at cpan.org> >>

=head1 LICENSE AND COPYRIGHT

Copyright 2011-2021 Pavel Shaydo.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut
