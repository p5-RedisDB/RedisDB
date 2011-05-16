package RedisServer;

use strict;
use warnings;
use POSIX qw(SIGTERM);
use File::Spec;

sub start {
    my $class = shift;
    my %args = @_;

    # check if we have redis-server
    return unless grep { -x } map { File::Spec->catfile($_, 'redis-server') } File::Spec->path;

    $args{port} ||= $ENV{TEST_REDIS_PORT} || 6380;
    unless ( $args{dir} ) {
        require File::Temp;
        $args{dir} = File::Temp::tempdir( 'test_redisXXXXXX', TMPDIR => 1, CLEANUP => 0 );
        my $cfg = <<EOC;
daemonize no
port $args{port}
timeout 0
loglevel notice
logfile $args{dir}/redis_test.log
databases 2
dbfilename dump_test.rdb
dir $args{dir}
EOC
        open my $cfg_fd, ">", "$args{dir}/redis.cfg" or die $!;
        print $cfg_fd $cfg;
        close $cfg_fd;
    }

    my $self = bless \%args, $class;
    $self->_start;
    return $self;
}

sub _start {
    my $self = shift;
    $self->{pid} = fork;
    if ( $self->{pid} == 0 ) {
        exec 'redis-server', "$self->{dir}/redis.cfg" or die "Couldn't start redis server: $!";
    }
    $self->{mypid} = $$;
    sleep 2;
}

sub stop {
    my $self = shift;

    # do not kill server from the child process
    if ( $self->{pid} and $self->{mypid} == $$ ) {
        kill SIGTERM, $self->{pid};
        waitpid $self->{pid}, 0;
        delete $self->{pid};
    }
    return;
}

sub restart {
    my $self = shift;
    $self->stop;
    $self->_start;
    return;
}

sub DESTROY {
    local $?;
    shift->stop;
}

1;
