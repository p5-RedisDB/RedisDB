package RedisDB::Parse::Redis;

use strict;
use warnings;
our $VERSION = "2.14";
$VERSION = eval $VERSION;

use Try::Tiny;

my $implementation;

unless ( $ENV{REDISDB_PARSER_PP} ) {
    try {
        require RedisDB::Parse::Redis_XS;
        $implementation = "RedisDB::Parse::Redis_XS";
    }
}

unless ($implementation) {
    require RedisDB::Parse::Redis_PP;
    $implementation = "RedisDB::Parse::Redis_PP";
}

=head1 NAME

RedisDB::Parse::Redis - redis protocol parser for RedisDB

=head1 SYNOPSIS

    use RedisDB::Parse::Redis;
    my $parser = RedisDB::Parse::Redis->new( redisdb => $ref );
    $parser->add_callback(\&cb);
    $parser->add($data);

=head1 DESCRIPTION

This module provides functions to build redis requests and parse replies from
the server. Normally you don't want to use this module directly, see
L<RedisDB> instead.

=head1 METHODS

=head2 $class->new(redisdb => $redis, utf8 => $flag)

Creates new parser object. I<redisdb> parameter points to the L<RedisDB>
object which owns this parser. The reference will be passed to callbacks as
the first argument. I<utf8> flag may be set if you want all data encoded as
UTF-8 before sending to server, and decoded when received.

=cut

sub new {
    shift;
    return $implementation->new(@_);
}

=head2 $class->implementation

Returns name of the package that actually implements parser functionality. It
may be either L<RedisDB::Parse::Redis_PP> or L<RedisDB::Parse::Redis_XS>.

=cut

sub implementation {
    return $implementation;
}

=head2 $self->build_request($command, @arguments)

Encodes I<$command> and I<@arguments> as redis request.

=head2 $self->add_callback(\&cb)

Pushes callback to the queue of callbacks.

=head2 $self->set_default_callback(\&cb)

Set callback to invoke when there are no callbacks in queue.
Used by subscription loop.

=head2 $self->callbacks

Returns true if there are callbacks in queue

=head2 $self->propagate_reply($reply)

Invoke every callback from queue and the default callback with the given
I<$reply>.

=head2 $self->add($data)

Process new data received from the server. For every reply found callback from
the queue will be invoked with two parameters -- reference to L<RedisDB>
object, and decoded reply from the server.

=cut

1;

__END__

=head1 SEE ALSO

L<RedisDB>

=head1 AUTHOR

Pavel Shaydo, C<< <zwon at cpan.org> >>

=head1 LICENSE AND COPYRIGHT

Copyright 2011-2013 Pavel Shaydo.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut
