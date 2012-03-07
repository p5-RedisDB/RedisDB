package RedisServer;

use strict;
use warnings;
use POSIX qw(SIGTERM);
use File::Spec;

my $REDIS_SERVER = 'redis-server';
$REDIS_SERVER .= '.exe' if $^O eq 'MSWin32';

sub start {
    my $class = shift;
    my %args  = @_;

    # check if we have redis-server
    return unless grep { -x } map { File::Spec->catfile( $_, $REDIS_SERVER ) } File::Spec->path;

    $args{port} ||= $ENV{TEST_REDIS_PORT} || 6380;
    my $requirepass = $args{password} ? "requirepass $args{password}" : "";
    unless ( $args{dir} ) {
        require File::Temp;
        $args{dir} = File::Temp::tempdir( 'test_redisXXXXXX', TMPDIR => 1, CLEANUP => 0 );
        my $logfile = File::Spec->catfile( $args{dir}, "redis_test.log" );
        my $cfg = <<EOC;
daemonize no
port $args{port}
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
    }

    my $self = bless \%args, $class;
    $self->_start;
    return $self;
}

sub _start {
    my $self = shift;
    $self->{pid} = fork;
    if ( $self->{pid} == 0 ) {
        exec $REDIS_SERVER, File::Spec->catfile( $self->{dir}, "redis.cfg" )
          or die "Couldn't start redis server: $!";
    }
    $self->{mypid} = $$;
    sleep 2;
}

sub stop {
    my $self = shift;

    return if $^O eq 'MSWin32';

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

1;
