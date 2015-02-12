package RedisDB::Cluster;

use strict;
use warnings;
our $VERSION = "2.38";
$VERSION = eval $VERSION;

use Carp;
use RedisDB;
use Time::HiRes qw(usleep);

our $DEBUG = 0;

=head1 NAME

RedisDB::Cluster - client for redis cluster

=head1 SYNOPSIS

    my $cluster = RedisDB::Cluster->new( startup_nodes => \@nodes );
    $cluster->set( 'foo', 'bar' );
    my $res = $cluster->get('foo');

=head1 DESCRIPTION

This module allows you to access redis cluster.

B<WARNING:> this module is at early stage of development

=head1 METHODS

=cut

=head2 $self->new(startup_nodes => \@nodes)

create a new connection to cluster. Startup nodes are used to retrieve
information about all cluster nodes and slots mappings.

=cut

sub new {
    my ( $class, %params ) = @_;

    my $self = {
        _slot       => [],
        _connection => {},
        _nodes      => $params{startup_nodes},
    };

    bless $self, $class;
    $self->_initialize_slots;

    return $self;
}

sub _initialize_slots {
    my $self = shift;

    unless ( $self->{_nodes} and @{ $self->{_nodes} } ) {
        confess "list of cluster nodes is empty";
    }

    my %new_nodes;
    my $new_nodes;
    for my $node ( @{ $self->{_nodes} } ) {
        my $redis = RedisDB->new(
            host        => $node->{host},
            port        => $node->{port},
            raise_error => 0,
        );

        my $nodes = $redis->cluster_nodes;
        next if ref $nodes =~ /^RedisDB::Error/;
        $new_nodes = $nodes;
        for (@$nodes) {
            $new_nodes{"$_->{host}:$_->{port}"}++;
        }

        my $slots = $redis->cluster('SLOTS');
        confess "got an error trying retrieve a list of cluster slots: $slots"
          if ref $slots =~ /^RedisDB::Error/;
        for (@$slots) {
            my ( $ip, $port ) = @{ $_->[2] };
            my $host_id = "$ip:$port";
            for ( $_->[0] .. $_->[1] ) {
                $self->{_slot}[$_] = $host_id;
            }
        }
        last;
    }

    unless (@$new_nodes) {
        confess "couldn't get list of cluster nodes";
    }
    $self->{_nodes} = $new_nodes;

    # close connections to nodes that are not in cluster
    for (keys %{$self->{_connection}}) {
        delete $self->{_connection}{$_} unless $new_nodes{$_};
    }

    return;
}

=head2 $self->execute($command, $key, @args)

sends command to redis and returns the reply. It determines the cluster node to
send command to from the I<$key>.

=cut

sub execute {
    my $self    = shift;
    my $command = uc shift;
    my $key     = shift;

    if ( $self->{_refresh_slots} ) {
        $self->_initialize_slots;
    }
    my $slot    = key_slot($key);
    my $host_id = $self->{_slot}[$slot];
    my $asking;
    my $last_connection;

    my $attempts = 10;
    while ( $attempts-- ) {
        my $redis = $self->{_connection}{$host_id};
        unless ($redis) {
            my ( $host, $port ) = split /:/, $host_id;
            $redis = $self->{_connection}{$host_id} = RedisDB->new(
                host        => $host,
                port        => $port,
                raise_error => 0,
            );
        }

        $redis->asking(RedisDB::IGNORE_REPLY) if $asking;
        $asking = 0;
        my $res = $redis->execute( $command, $key, @_ );
        if ( ref $res eq 'RedisDB::Error::MOVED' ) {
            if ( $res->{slot} ne $slot ) {
                confess
                  "Incorrectly computed slot for key '$key', ours $slot, theirs $res->{slot}";
            }
            warn "slot $slot moved to $res->{host}:$res->{port}" if $DEBUG;
            $host_id = $self->{_slot}[$slot] = "$res->{host}:$res->{port}";
            $self->{_refresh_slots} = 1;
            next;
        }
        elsif ( ref $res eq 'RedisDB::Error::ASK' ) {
            warn "asking $res->{host}:$res->{port} about slot $slot" if $DEBUG;
            $host_id = "$res->{host}:$res->{port}";
            $asking  = 1;
            next;
        }
        elsif ( ref $res eq 'RedisDB::Error::DISCONNECTED' ) {
            warn "connection to $host_id lost" if $DEBUG;
            delete $self->{_connection}{$host_id};
            usleep 100_000;
            if ( $last_connection and $last_connection eq $host_id ) {

                # if we couldn't reconnect to host, then refresh slots table
                warn "refreshing slots table" if $DEBUG;
                $self->_initialize_slots;

                # if it's still the same host, then just return the error
                return $res if $self->{_slot}[$slot] eq $host_id;
                warn "got a new host for the slot" if $DEBUG;
            }
            else {
                warn "trying to reconnect" if $DEBUG;
                $last_connection = $host_id;
            }
            next;
        }
        return $res;
    }

    return RedisDB::Error::DISCONNECTED->new(
        "Couldn't send command after 10 attempts");
}

sub add_new_node {
    my ( $self, $addr, $master_id ) = @_;
    $addr = _ensure_hash_address($addr);

    my $redis = RedisDB->new(
        %$addr,
        raise_error => 0,
    );
    my $ok;
    for my $node ( @{ $self->{_nodes} } ) {
        $redis->cluster( 'MEET', $node->{host}, $node->{port},
            sub { $ok++ if not ref $_[1] and $_[1] eq 'OK'; warn $_[1] if ref $_[1]; }
        );
    }
    $redis->mainloop;
    croak "failed to attach node to cluster" unless $ok;

    if ($master_id) {
        my $attempt = 0;
        my $nodes   = $redis->cluster_nodes;
        while ( not grep { $_->{node_id} eq $master_id } @$nodes ) {
            croak "failed to start replication from $master_id - node is not present"
              if $attempt++ >= 10;
            usleep 100_000 * $attempt;
            $nodes = $redis->cluster_nodes;
        }
        my $res = $redis->cluster( 'REPLICATE', $master_id );
        croak $res if ref $res =~ /^RedisDB::Error/;
    }

    return 'OK';
}

sub _ensure_hash_address {
    my $addr = shift;
    unless ( ref $addr eq 'HASH' ) {
        my ( $host, $port ) = split /:/, $addr;
        croak "invalid address spec: $addr" unless $host and $port;
        $addr = {
            host => $host,
            port => $port
        };
    }
    return $addr;
}

sub _connect_to_node {
    my ( $self, $node ) = @_;
    my $redis = RedisDB->new(
        host        => $node->{host},
        port        => $node->{port},
        raise_error => 0,
    );
    $redis = $redis->{_socket} ? $redis : undef;
    $self->{_connection}{"$_->{host}:$_->{port}"} = $redis if $redis;
    return $redis;
}

sub random_connection {
    my $self = shift;
    my ($connection) = values %{ $self->{_connection} };
    unless ($connection) {
        for ( @{ $self->{_nodes} } ) {
            $connection = _connect_to_node( $self, $_ );
            last if $connection;
        }
    }
    return $connection;
}

my @crc16tab = (
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
    0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
    0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
    0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
    0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
    0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
    0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
    0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
    0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
    0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
    0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
    0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
    0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
    0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
    0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
    0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
    0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
    0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
    0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
    0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
    0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
    0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
    0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
    0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
);

=head2 $self->crc16($buf)

compute crc16 for the specified buffer as defined in redis cluster
specification

=cut

sub crc16 {
    my $buf = shift;
    if ( utf8::is_utf8($buf) ) {
        die "Can't compute crc16 for string with wide characters.\n"
          . "You should encode strings you pass to redis as bytes";
    }
    my $crc = 0;
    for ( split //, $buf ) {
        $crc =
          ( $crc << 8 & 0xFF00 ) ^ $crc16tab[ ( ( $crc >> 8 ) ^ ord ) & 0x00FF ];
    }
    return $crc;
}

=head2 $self->key_slot($key)

return slot number for the given I<$key>

=cut

sub key_slot {
    my $key = shift;

    if ( $key =~ /\{([^}]+)\}/ ) {
        $key = $1;
    }

    return crc16($key) & 16383;
}

1;

__END__

=head1 AUTHOR

Pavel Shaydo, C<< <zwon at cpan.org> >>

=head1 LICENSE AND COPYRIGHT

Copyright 2011-2015 Pavel Shaydo.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut
