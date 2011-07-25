package RedisDB::Error;

use strict;
use warnings;
our $VERSION = "0.18_2";
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
returns description of the error.

=head1 METHODS

=cut

use overload '""' => \&as_string;

=head2 $class->new($message)

Create new error object with specified error message.

=cut

sub new {
    my ($class, $message) = @_;
    return bless { message => $message }, $class;
}

=head2 $self->as_string

Return error message. Also you can just use object in string context.

=cut

sub as_string {
    return shift->{message};
}

1;

__END__

=head1 SEE ALSO

L<RedisDB>

=head1 AUTHOR

Pavel Shaydo, C<< <zwon at cpan.org> >>

=head1 LICENSE AND COPYRIGHT

Copyright 2011 Pavel Shaydo.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut
