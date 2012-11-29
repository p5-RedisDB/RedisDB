package RedisDB::Parse::Redis_PP;

use strict;
use warnings;
our $VERSION = "2.11";
$VERSION = eval $VERSION;

=head1 NAME

RedisDB::Parse::Redis - redis protocol parser for RedisDB

=head1 DESCRIPTION

Pure Perl implementation of L<RedisDB::Parse::Redis>. You should not use this
module directly.

=cut

use Encode qw();
use RedisDB::Error;
use Carp;
use Try::Tiny;
use Scalar::Util qw(weaken);

sub new {
    my ( $class, %params ) = @_;
    my $self = {
        redisdb    => $params{redisdb},
        utf8       => $params{utf8},
        _callbacks => [],
        _buffer    => '',
    };
    weaken( $self->{redisdb} );
    return bless $self, $class;
}

sub build_request {
    my $self  = shift;
    my $nargs = @_;

    my $req = "*$nargs\015\012";
    if ( $self->{utf8} ) {
        $req .= '$' . length($_) . "\015\012" . $_ . "\015\012"
          for map { Encode::encode( 'UTF-8', $_, Encode::FB_CROAK | Encode::LEAVE_SRC ) } @_;
    }
    else {
        use bytes;
        $req .= '$' . length($_) . "\015\012" . $_ . "\015\012" for @_;
    }
    return $req;
}

sub add_callback {
    my ( $self, $cb ) = @_;
    push @{ $self->{_callbacks} }, $cb;
}

sub set_default_callback {
    my ( $self, $cb ) = @_;
    $self->{_default_cb} = $cb;
}

sub callbacks {
    @{ shift->{_callbacks} };
}

sub add {
    my ( $self, $data ) = @_;
    $self->{_buffer} .= $data;
    1 while length $self->{_buffer} and $self->_parse_reply;
}

# $self->_parse_reply
#
# checks if buffer contains full reply. Returns 1 if it is,
# invokes callback for the reply
my ( $READ_LINE, $READ_ERROR, $READ_NUMBER, $READ_BULK_LEN, $READ_BULK, $READ_MBLK_LEN,
    $WAIT_BUCKS ) = 1 .. 7;

sub _parse_reply {
    my $self = shift;
    return unless length $self->{_buffer};

    # if we not yet started parsing reply
    unless ( $self->{_parse_state} ) {
        my $type = substr( $self->{_buffer}, 0, 1, '' );
        delete $self->{_parse_mblk_level};
        if ( $type eq '+' ) {
            $self->{_parse_state} = $READ_LINE;
        }
        elsif ( $type eq '-' ) {
            $self->{_parse_state} = $READ_ERROR;
        }
        elsif ( $type eq ':' ) {
            $self->{_parse_state} = $READ_NUMBER;
        }
        elsif ( $type eq '$' ) {
            $self->{_parse_state} = $READ_BULK_LEN;
        }
        elsif ( $type eq '*' ) {
            $self->{_parse_state}      = $READ_MBLK_LEN;
            $self->{_parse_mblk_level} = 1;
        }
        else {
            die "Got invalid reply: $type$self->{_buffer}";
        }
    }

    # parse data
    while (1) {
        return unless length $self->{_buffer} >= 2;
        if ( $self->{_parse_state} == $READ_LINE ) {
            return unless defined( my $line = $self->_read_line );
            return 1 if $self->_reply_completed($line);
        }
        elsif ( $self->{_parse_state} == $READ_ERROR ) {
            return unless defined( my $line = $self->_read_line );
            my $err = RedisDB::Error->new($line);
            return 1 if $self->_reply_completed($err);
        }
        elsif ( $self->{_parse_state} == $READ_NUMBER ) {
            return unless defined( my $line = $self->_read_line );
            die "Received invalid integer reply :$line" unless $line =~ /^-?[0-9]+$/;
            return 1 if $self->_reply_completed( 0 + $line );
        }
        elsif ( $self->{_parse_state} == $READ_BULK_LEN ) {
            return unless defined( my $len = $self->_read_line );
            if ( $len >= 0 ) {
                $self->{_parse_state}    = $READ_BULK;
                $self->{_parse_bulk_len} = $len;
            }
            elsif ( $len == -1 ) {
                return 1 if $self->_reply_completed(undef);
            }
        }
        elsif ( $self->{_parse_state} == $READ_BULK ) {
            return unless length $self->{_buffer} >= 2 + $self->{_parse_bulk_len};
            my $bulk = substr( $self->{_buffer}, 0, $self->{_parse_bulk_len}, '' );
            substr $self->{_buffer}, 0, 2, '';
            if ( $self->{utf8} ) {
                try {
                    $bulk = Encode::decode( 'UTF-8', $bulk, Encode::FB_CROAK | Encode::LEAVE_SRC );
                }
                catch {
                    confess "Couldn't decode reply from the server, invalid UTF-8: '$bulk'";
                };
            }
            return 1 if $self->_reply_completed($bulk);
        }
        elsif ( $self->{_parse_state} == $READ_MBLK_LEN ) {
            return unless defined( my $len = $self->_read_line );
            if ( $len > 0 ) {
                $self->{_parse_mblk_len} = $len;
                $self->{_parse_state}    = $WAIT_BUCKS;
                $self->{_parse_reply}    = [];
            }
            elsif ( $len == 0 || $len == -1 ) {
                $self->{_parse_mblk_level}--;
                return 1 if $self->_reply_completed( $len ? undef : [] );
            }
            else {
                die "Invalid multi-bulk reply: *$len\015\012$self->{_buffer}";
            }
        }
        elsif ( $self->{_parse_state} == $WAIT_BUCKS ) {
            my $char = substr( $self->{_buffer}, 0, 1, '' );
            if ( $char eq '$' ) {
                $self->{_parse_state} = $READ_BULK_LEN;
            }
            elsif ( $char eq ':' ) {
                $self->{_parse_state} = $READ_NUMBER;
            }
            elsif ( $char eq '+' ) {
                $self->{_parse_state} = $READ_LINE;
            }
            elsif ( $char eq '-' ) {
                $self->{_parse_state} = $READ_ERROR;
            }
            elsif ( $char eq '*' ) {
                $self->{_parse_state} = $READ_MBLK_LEN;
                $self->{_parse_mblk_level}++;
                push @{ $self->{_parse_mblk_store} },
                  [ $self->{_parse_mblk_len}, $self->{_parse_reply} ];
            }
            else {
                die "Invalid multi-bulk reply. Expected [\$:+-*] but got $char";
            }
        }
    }
    return;
}

sub _read_line {
    my $self = shift;
    my $pos = index $self->{_buffer}, "\015\012";
    my $line;
    if ( $pos >= 0 ) {

        # Got end of the line, add all stuff before \r\n
        # to the reply string. Strip \r\n from the buffer
        $line = substr( $self->{_buffer}, 0, $pos, '' );
        substr $self->{_buffer}, 0, 2, '';
    }
    return $line;
}

sub _mblk_item {
    my ( $self, $value ) = @_;

    push @{ $self->{_parse_reply} }, $value;
    my $repeat;
    if ( --$self->{_parse_mblk_len} ) {
        $self->{_parse_state} = $WAIT_BUCKS;
        $repeat = 1;
    }
    elsif ( --$self->{_parse_mblk_level} ) {
        my $res = $self->{_parse_reply};
        ( $self->{_parse_mblk_len}, $self->{_parse_reply} ) =
          @{ pop @{ $self->{_parse_mblk_store} } };
        $repeat = $self->_mblk_item($res);
    }
    else {
        $repeat = 0;
    }

    return $repeat;
}

sub _reply_completed {
    my ( $self, $reply ) = @_;

    if ( $self->{_parse_mblk_level} ) {
        return if $self->_mblk_item($reply);
        $reply = delete $self->{_parse_reply};
    }

    $self->{_parse_state} = undef;
    my $cb = shift( @{ $self->{_callbacks} } ) || $self->{_default_cb};
    $cb->( $self->{redisdb}, $reply );
    return 1;
}

1;

__END__

=head1 SEE ALSO

L<RedisDB::Parse::Redis>

=head1 AUTHOR

Pavel Shaydo, C<< <zwon at cpan.org> >>

=head1 LICENSE AND COPYRIGHT

Copyright 2011, 2012 Pavel Shaydo.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut
