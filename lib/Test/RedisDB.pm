package Test::RedisDB;
use strict;
use warnings;
our $VERSION = "2.50";
$VERSION = eval $VERSION;

=head1 NAME

Test::RedisDB - start redis-server for testing

=head1 SYNOPSIS

    use Test::RedisDB;

    my $test_server = Test::RedisDB->new;
    my $redis = $test_server->redisdb_client;
    $redis->set('foo', 1);
    my $res = $redis->get('foo');

=head1 DESCRIPTION

This module allows you to start an instance of redis-server for testing your
modules that use RedisDB.

=head1 METHODS

=cut

use File::Spec;
use File::Temp;
use RedisDB;
use Test::TCP;

my $REDIS_SERVER = 'redis-server';
$REDIS_SERVER .= '.exe' if $^O eq 'MSWin32';

=head2 $class->new(%options)

start a new redis-server instance, return Test::RedisDB object tied to this
instance. Accepts the following options:

=head3 password

server should require a password to connect

=cut

sub new {
    my $class = shift;
    my %args  = @_;

    # check if we have redis-server
    return unless grep { -x } map { File::Spec->catfile( $_, $REDIS_SERVER ) } File::Spec->path;

    my $requirepass = $args{password} ? "requirepass $args{password}" : "";
    $args{dir} = File::Temp::tempdir( 'test_redisXXXXXX', TMPDIR => 1, CLEANUP => 0 );

    my $self = bless \%args, $class;
    $self->{_t_tcp} = Test::TCP->new(
        code => sub {
            my $port    = shift;
            my $logfile = File::Spec->catfile( $args{dir}, "redis_test.log" );
            my $cfg     = <<EOC;
daemonize no
port $port
timeout 0
loglevel notice
logfile $logfile
databases 2
dbfilename dump_test.rdb
dir $args{dir}
$requirepass
EOC
            open my $cfg_fd, ">", File::Spec->catfile( $args{dir}, "redis.cfg" ) or die $!;
            print $cfg_fd $cfg;
            close $cfg_fd;
            exec $REDIS_SERVER, File::Spec->catfile( $self->{dir}, "redis.cfg" );
        },
    );
    $self->{port} = $self->{_t_tcp}->port;
    return $self;
}

=head2 $self->start

start the server. You only need to call this method if you stopped the server
using the I<stop> method

=cut

sub start {
    shift->{_t_tcp}->start;
}

=head2 $self->stop

stop the server

=cut

sub stop {
    shift->{_t_tcp}->stop;
}

=head2 $self->host

return hostname or address, at the moment always 'localhost'

=cut

sub host {
    'localhost';
}

=head2 $self->port

return port number on which server accept connections

=cut

sub port {
    shift->{port};
}

=head2 $self->redisdb_client(%options)

return a new RedisDB client object connected to the test server, I<%options>
passed to RedisDB constructor

=cut

sub redisdb_client {
    my $self = shift;

    if($self->{password}) {
        unshift @_, password => $self->{password};
    }

    return RedisDB->new(host => 'localhost', port => $self->{port}, @_);
}

=head2 $self->url

return URL for the server

=cut

sub url {
    my $self = shift;

    return 'redis://' . ($self->{password} ? ":$self->{password}@" : "") . "localhost:$self->{port}";
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
