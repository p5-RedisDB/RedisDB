package RedisDB::Parse::Redis;
use strict;
use warnings;
use Encode;
use RedisDB::Error;
use Carp;

sub new {
    my ($class, %params) = @_;
    my $self = { utf8 => $params{utf8}, _callbacks => [], _buffer => '', };
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
    my ($self, $cb) = @_;
    push @{$self->{_callbacks}}, $cb;
}

sub callbacks {
    @{shift->{_callbacks}};
}

sub add {
    my ($self, $data) = @_;
    $self->{_buffer} .= $data;
    1 while $self->{_buffer} and $self->_parse_reply;
}

# $self->_parse_reply
#
# checks if buffer contains full reply. Returns 1 if it is,
# invokes callback for the reply
my ( $READ_LINE, $READ_ERROR, $READ_NUMBER, $READ_BULK_LEN, $READ_BULK, $READ_MBLK_LEN, $WAIT_BUCKS ) = 1 .. 7;

sub _parse_reply {
    my $self = shift;
    return unless length $self->{_buffer};

    # if we not yet started parsing reply
    unless ( $self->{_parse_state} ) {
        my $type = substr( $self->{_buffer}, 0, 1, '' );
        $self->{_parse_reply} = [$type];
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
    my $repeat    = 1;
    my $completed = 0;
    while ($repeat) {
        $repeat = 0;
        return unless length $self->{_buffer} >= 2;
        if ( $self->{_parse_state} == $READ_LINE ) {
            if ( defined( my $line = $self->_read_line ) ) {
                if ( $self->{_parse_reply}[0] eq '+' ) {
                    $self->{_parse_reply}[1] = $line;
                    return $self->_reply_completed;
                }
                else {
                    $repeat    = $self->_mblk_item($line);
                    $completed = !$repeat;
                }
            }
        }
        elsif ( $self->{_parse_state} == $READ_ERROR ) {
            if ( defined( my $line = $self->_read_line ) ) {
                $line = RedisDB::Error->new($line);
                if ( $self->{_parse_reply}[0] eq '-' ) {
                    $self->{_parse_reply}[1] = $line;
                    return $self->_reply_completed;
                }
                else {
                    $repeat    = $self->_mblk_item($line);
                    $completed = !$repeat;
                }
            }
        }
        elsif ( $self->{_parse_state} == $READ_NUMBER ) {
            if ( defined( my $line = $self->_read_line ) ) {
                die "Received invalid integer reply :$line" unless $line =~ /^-?[0-9]+$/;
                if ( $self->{_parse_reply}[0] eq ':' ) {
                    $self->{_parse_reply}[1] = $line;
                    return $self->_reply_completed;
                }
                else {
                    $repeat    = $self->_mblk_item($line);
                    $completed = !$repeat;
                }
            }
        }
        elsif ( $self->{_parse_state} == $READ_BULK_LEN ) {
            if ( defined( my $len = $self->_read_line ) ) {
                if ( $len >= 0 ) {
                    $self->{_parse_state}    = $READ_BULK;
                    $self->{_parse_bulk_len} = $len;
                    $repeat                  = 1;
                }
                elsif ( $len == -1 ) {
                    if ( $self->{_parse_reply}[0] eq '$' ) {
                        $self->{_parse_reply}[1] = undef;
                        return $self->_reply_completed;
                    }
                    else {
                        $repeat    = $self->_mblk_item(undef);
                        $completed = !$repeat;
                    }
                }
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
                    croak "Couldn't decode reply from the server, invalid UTF-8: '$bulk'";
                };
            }
            if ( $self->{_parse_reply}[0] eq '$' ) {
                $self->{_parse_reply}[1] = $bulk;
                return $self->_reply_completed;
            }
            else {
                $repeat    = $self->_mblk_item($bulk);
                $completed = !$repeat;
            }
        }
        elsif ( $self->{_parse_state} == $READ_MBLK_LEN ) {
            if ( defined( my $len = $self->_read_line ) ) {
                if ( $len > 0 ) {
                    $self->{_parse_mblk_len} = $len;
                    $self->{_parse_state}    = $WAIT_BUCKS;
                    $self->{_parse_reply}[1] = [];
                    $repeat                  = 1;
                }
                elsif ( $len == 0 || $len == -1 ) {
                    if ( $self->{_parse_mblk_level}-- == 1 ) {
                        $self->{_parse_reply}[1] = $len ? undef : [];
                        return $self->_reply_completed;
                    }
                    else {
                        ( $self->{_parse_mblk_len}, $self->{_parse_reply} ) =
                          @{ $self->{_parse_mblk_store} };
                        push @{ $self->{_parse_reply}[1] }, $len ? undef : [];
                        if ( --$self->{_parse_mblk_len} ) {
                            $self->{_parse_state} = $WAIT_BUCKS;
                            $repeat = 1;
                        }
                        else {
                            $repeat = 0;
                        }
                        $completed = !$repeat;
                    }
                }
                else {
                    die "Invalid multi-bulk reply: *$len\015\012$self->{_buffer}";
                }
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
                $self->{_parse_mblk_store} = [ $self->{_parse_mblk_len}, $self->{_parse_reply} ];
                $self->{_parse_reply} = ['*'];
            }
            else {
                die "Invalid multi-bulk reply. Expected [\$:+-*] but got $char";
            }
            $repeat = 1;
        }
    }
    return $completed ? $self->_reply_completed : undef;
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

    push @{ $self->{_parse_reply}[1] }, $value;
    my $repeat;
    if ( --$self->{_parse_mblk_len} ) {
        $self->{_parse_state} = $WAIT_BUCKS;
        $repeat = 1;
    }
    elsif ( --$self->{_parse_mblk_level} ) {
        $self->{_parse_mblk_len} = shift @{ $self->{_parse_mblk_store} };
        $self->{_parse_mblk_len}--;
        my $reply = shift @{ $self->{_parse_mblk_store} };
        push @{ $reply->[1] }, $self->{_parse_reply}[1];
        $self->{_parse_reply} = $reply;
        $self->{_parse_state} = $WAIT_BUCKS;
        $repeat               = $self->{_parse_mblk_len} > 0;
    }
    else {
        $repeat = 0;
    }

    return $repeat;
}

sub _reply_completed {
    my $self = shift;
    $self->{_parse_state} = undef;
    my $cb = shift @{ $self->{_callbacks} };
    my $rep = $self->{_parse_reply}[1];
    $cb->( $self, $rep );
    $self->{_parse_reply} = undef;
    return 1;
}

1;
