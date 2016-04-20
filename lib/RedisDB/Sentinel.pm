package RedisDB::Sentinel;

use strict;
use warnings;
our $VERSION = "2.52";
$VERSION = eval $VERSION;

use Carp;
use RedisDB;
use Try::Tiny;

=head1 NAME

RedisDB::Sentinel - interface to redis servers managed by sentinel

=head1 SYNOPSIS

    use RedisDB::Sentinel;

    my $redis = RedisDB::Sentinel->connect_to_master(
        service => $service_name,
        sentinels => [
            {
                host => 'host1',
                port => 26379
            },
            {
                host => 'host2',
                port => 26379
            },
        ],
    );
    $redis->set( $key, $value );
    my $value = $redis->get($key);

=head1 DESCRIPTION

This module provides interface to access redis servers managed by sentinels, it
handles communication with sentinels and dispatches commands to the master
redis.

=head1 METHODS

=cut

=head2 $class->connect_to_master(%params)

retrieve information about master from sentinel and return RedisDB object
connected to master. Additionally sets on_conect_error handler to retrieve
information about new master in case of reconnect. Requires two parameters:

=over 4

=item B<service>

service name to which you want to connect

=item B<sentinels>

list of sentinels. Each element is a hash with "host" and "port" elements.

=back

Other parameters are passed as is to constructor of RedisDB object. Note, that
host, port, and on_connect_error parameters will be overwritten by
RedisDB::Sentinel

=cut

sub connect_to_master {
    my ( $class, %args ) = @_;

    my $service = delete $args{service}
      or croak '"service" parameter is required';
    my @sentinels = @{ delete $args{sentinels} }
      or croak '"sentinels" parameter is required';

    my ( $host, $port ) = _get_master_from_sentinel( $service, \@sentinels );
    return RedisDB->new(
        %args,
        host             => $host,
        port             => $port,
        on_connect_error => sub {
            my ( $redis, $error ) = @_;
            my ( $host, $port ) = _get_master_from_sentinel( $service, \@sentinels );
            $redis->{host} = $host;
            $redis->{port} = $port;
            return;
        },
    );
}

sub _get_master_from_sentinel {
    my ( $service, $sentinels ) = @_;

    for ( 1 .. @$sentinels ) {
        my $master = try {
            my $sentinel = RedisDB->new( %{ $sentinels->[0] } );
            $sentinel->execute( 'sentinel', 'get-master-addr-by-name', $service );
        };
        return @$master if $master;
        push @$sentinels, shift @$sentinels;
    }
}

1;

__END__

=head1 AUTHOR

Pavel Shaydo, C<< <zwon at cpan.org> >>

=head1 LICENSE AND COPYRIGHT

Copyright 2011-2016 Pavel Shaydo.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut
