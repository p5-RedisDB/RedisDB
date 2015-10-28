package Test::RedisDB;
use strict;
use warnings;

use File::Spec;
use File::Temp;
use RedisDB;
use Test::TCP;

my $REDIS_SERVER = 'redis-server';
$REDIS_SERVER .= '.exe' if $^O eq 'MSWin32';

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

sub start {
    shift->{_t_tcp}->start;
}

sub stop {
    shift->{_t_tcp}->stop;
}

sub port {
    shift->{port};
}

sub redisdb_client {
    my $self = shift;

    if($self->{password}) {
        unshift @_, password => $self->{password};
    }

    return RedisDB->new(host => 'localhost', port => $self->{port}, @_);
}

1;
