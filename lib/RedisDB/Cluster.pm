package RedisDB::Cluster;

use strict;
use warnings;

our $VERSION = "2.56";
$VERSION = eval $VERSION;

use Carp;
use RedisDB;
use Time::HiRes qw(usleep);

our $DEBUG = 0;

# use util/generate_key_positions.pl to generate this
# command / first key position
my %key_pos = (
    append           => 1,
    bitcount         => 1,
    bitop            => 2,
    bitpos           => 1,
    blpop            => 1,
    brpop            => 1,
    brpoplpush       => 1,
    decr             => 1,
    decrby           => 1,
    del              => 1,
    dump             => 1,
    exists           => 1,
    expire           => 1,
    expireat         => 1,
    get              => 1,
    getbit           => 1,
    getrange         => 1,
    getset           => 1,
    hdel             => 1,
    hexists          => 1,
    hget             => 1,
    hgetall          => 1,
    hincrby          => 1,
    hincrbyfloat     => 1,
    hkeys            => 1,
    hlen             => 1,
    hmget            => 1,
    hmset            => 1,
    hscan            => 1,
    hset             => 1,
    hsetnx           => 1,
    hvals            => 1,
    incr             => 1,
    incrby           => 1,
    incrbyfloat      => 1,
    lindex           => 1,
    linsert          => 1,
    llen             => 1,
    lpop             => 1,
    lpush            => 1,
    lpushx           => 1,
    lrange           => 1,
    lrem             => 1,
    lset             => 1,
    ltrim            => 1,
    mget             => 1,
    move             => 1,
    mset             => 1,
    msetnx           => 1,
    object           => 2,
    persist          => 1,
    pexpire          => 1,
    pexpireat        => 1,
    pfadd            => 1,
    pfcount          => 1,
    pfmerge          => 1,
    psetex           => 1,
    pttl             => 1,
    rename           => 1,
    renamenx         => 1,
    restore          => 1,
    'restore-asking' => 1,
    rpop             => 1,
    rpoplpush        => 1,
    rpush            => 1,
    rpushx           => 1,
    sadd             => 1,
    scard            => 1,
    sdiff            => 1,
    sdiffstore       => 1,
    set              => 1,
    setbit           => 1,
    setex            => 1,
    setnx            => 1,
    setrange         => 1,
    sinter           => 1,
    sinterstore      => 1,
    sismember        => 1,
    smembers         => 1,
    smove            => 1,
    sort             => 1,
    spop             => 1,
    srandmember      => 1,
    srem             => 1,
    sscan            => 1,
    strlen           => 1,
    substr           => 1,
    sunion           => 1,
    sunionstore      => 1,
    ttl              => 1,
    type             => 1,
    watch            => 1,
    zadd             => 1,
    zcard            => 1,
    zcount           => 1,
    zincrby          => 1,
    zlexcount        => 1,
    zrange           => 1,
    zrangebylex      => 1,
    zrangebyscore    => 1,
    zrank            => 1,
    zrem             => 1,
    zremrangebylex   => 1,
    zremrangebyrank  => 1,
    zremrangebyscore => 1,
    zrevrange        => 1,
    zrevrangebylex   => 1,
    zrevrangebyscore => 1,
    zrevrank         => 1,
    zscan            => 1,
    zscore           => 1,
);

=head1 NAME

RedisDB::Cluster - client for redis cluster

=head1 SYNOPSIS

    my $cluster = RedisDB::Cluster->new( startup_nodes => \@nodes );
    $cluster->set( 'foo', 'bar' );
    my $res = $cluster->get('foo');

=head1 DESCRIPTION

This module allows you to access redis cluster.

=head1 METHODS

=cut

=head2 $self->new(startup_nodes => \@nodes)

create a new connection to cluster. Startup nodes should contain array of
hashes that contains addresses of some nodes in the cluster. Each hash should
contain 'host' and 'port' elements. Constructor will try to connect to nodes
from the list and from the first node to which it will be able to connect it
will retrieve information about all cluster nodes and slots mappings.

=over 4

=item password

Password, if redis server requires authentication.

=back

=cut

sub new {
    my ( $class, %params ) = @_;

    my $self = {
        _slots       => [],
        _connections => {},
        _nodes       => $params{startup_nodes},
        _password    => $params{password},
    };
    $self->{no_slots_initialization} = 1 if $params{no_slots_initialization};

    bless $self, $class;
    $self->_initialize_slots;

    return $self;
}

sub _initialize_slots {
    my $self = shift;

    return if $self->{no_slots_initialization};
    unless ( $self->{_nodes} and @{ $self->{_nodes} } ) {
        confess "list of cluster nodes is empty";
    }

    my %new_nodes;
    my $new_nodes;
    for my $node ( @{ $self->{_nodes} } ) {
        my $redis = _connect_to_node( $self, $node );
        next unless $redis;

        my $nodes = $redis->cluster_nodes;
        next if ref ($nodes) =~ /^RedisDB::Error/;
        $new_nodes = $nodes;
        for (@$nodes) {
            $new_nodes{"$_->{host}:$_->{port}"}++;
        }

        my $slots = $redis->cluster('SLOTS');
        confess "got an error trying retrieve a list of cluster slots: $slots"
          if ref $slots =~ /^RedisDB::Error/;
        for (@$slots) {
            my ( $ip, $port ) = @{ $_->[2] };
            my $node_key = "$ip:$port";
            for ( $_->[0] .. $_->[1] ) {
                $self->{_slots}[$_] = $node_key;
            }
        }
        last;
    }

    unless ( $new_nodes and @$new_nodes ) {
        confess "couldn't get list of cluster nodes";
    }
    $self->{_nodes} = $new_nodes;

    # close connections to nodes that are not in cluster
    for ( keys %{ $self->{_connections} } ) {
        delete $self->{_connections}{$_} unless $new_nodes{$_};
    }

    return;
}

=head2 $self->execute($command, @args)

sends command to redis and returns the reply. It determines the cluster node to
send command to from the first key in I<@args>, sending commands that does not
include key as an argument is not supported. If I<@args> contains several keys,
all of them should belong to the same slot, otherwise redis-server will return
an error if some of the keys are stored on a different node.

Module also defines wrapper methods with names matching corresponding redis
commands, so you can use

    $cluster->set( "foo", "bar" );
    $cluster->inc("baz");

instead of

    $cluster->execute( "set", "foo", "bar" );
    $cluster->execute( "inc", "baz" );

=cut

sub execute {
    my $self = shift;
    my @args = @_;

    my $command = lc $args[0];
    confess "Command $command does not have key" unless $key_pos{$command};
    my $key = $args[ $key_pos{$command} ];
    confess "Key is not specified in: ", join " ", @args unless length $key;

    if ( $self->{_refresh_slots} ) {
        $self->_initialize_slots;
    }
    my $slot     = key_slot($key);
    my $node_key = $self->{_slots}[$slot]
      || "$self->{_nodes}[0]{host}:$self->{_nodes}[0]{port}";
    my $asking;
    my $last_connection;

    my $attempts = 10;
    while ( $attempts-- ) {
        my $redis = $self->{_connections}{$node_key};
        unless ($redis) {
            my ( $host, $port ) = split /:([^:]+)$/, $node_key;
            $redis = _connect_to_node(
                $self,
                {
                    host => $host,
                    port => $port
                }
            );
        }

        my $res;
        if ($redis) {
            $redis->asking(RedisDB::IGNORE_REPLY) if $asking;
            $asking = 0;
            $res    = $redis->execute(@args);
        }
        else {
            $res = RedisDB::Error::DISCONNECTED->new(
                "Couldn't connect to redis server at $node_key");
        }

        if ( ref $res eq 'RedisDB::Error::MOVED' ) {
            if ( $res->{slot} ne $slot ) {
                confess
                  "Incorrectly computed slot for key '$key', ours $slot, theirs $res->{slot}";
            }
            warn "slot $slot moved to $res->{host}:$res->{port}" if $DEBUG;
            $node_key = $self->{_slots}[$slot] = "$res->{host}:$res->{port}";
            $self->{_refresh_slots} = 1;
            next;
        }
        elsif ( ref $res eq 'RedisDB::Error::ASK' ) {
            warn "asking $res->{host}:$res->{port} about slot $slot" if $DEBUG;
            $node_key = "$res->{host}:$res->{port}";
            $asking   = 1;
            next;
        }
        elsif ( ref $res eq 'RedisDB::Error::DISCONNECTED' ) {
            warn "$res" if $DEBUG;
            delete $self->{_connections}{$node_key};
            usleep 100_000;
            if ( $last_connection and $last_connection eq $node_key ) {

                # if we couldn't reconnect to host, then refresh slots table
                warn "refreshing slots table" if $DEBUG;
                $self->_initialize_slots;

                # if it's still the same host, then just return the error
                return $res if $self->{_slots}[$slot] eq $node_key;
                warn "got a new host for the slot" if $DEBUG;
            }
            else {
                warn "trying to reconnect" if $DEBUG;
                $last_connection = $node_key;
            }
            next;
        }
        return $res;
    }

    return RedisDB::Error::DISCONNECTED->new(
        "Couldn't send command after 10 attempts");
}

for my $command (keys %key_pos) {
    no strict 'refs';
    *{ __PACKAGE__ . "::$command" } = sub { execute(shift, $command, @_) };
}

=head2 $self->random_connection

return RedisDB object that is connected to some node of the cluster. Note, that
in most cases this method will return the same connection every time.

=cut

sub random_connection {
    my $self = shift;
    my ($connection) = values %{ $self->{_connections} };
    unless ($connection) {
        for ( @{ $self->{_nodes} } ) {
            $connection = _connect_to_node( $self, $_ );
            last if $connection;
        }
    }
    return $connection;
}

=head2 $self->node_for_slot($slot, %params)

return L<RedisDB> object connected to cluster node that is master node for the
given slot. I<%params> are passed to RedisDB constructor as is. This method is
using information about mappings between slots and nodes that is cached by
RedisDB::Cluster object, if there were changes in cluster configuration since
the last time that information has been obtained, then the method will return
RedisDB object connected to a wrong server, you can detect that situation by
checking results returned by server, it should return MOVED or ASK error if you
accessing the wrong server or slot is being migrated. Each time you call this
method a new RedisDB object is returned and consequently a new connection is
being established, so it is not something very fast.

=cut

sub node_for_slot {
    my ( $self, $slot, %params ) = @_;

    if ( $self->{_refresh_slots} ) {
        $self->_initialize_slots;
    }
    my $node_key = $self->{_slots}[$slot]
      or confess "Don't know master node for slot $slot";
    my ( $host, $port ) = split /:([^:]+)$/, $node_key;
    return RedisDB->new(
        %params,
        host => $host,
        port => $port
    );
}

=head2 $self->node_for_key($key, %params)

same as I<node_for_slot> but accepts key instead of slot number as the first
argument. Internally just calculates the slot number and then invokes
node_for_slot method.

=cut

sub node_for_key {
    my ($self, $key, %params) = @_;

    return $self->node_for_slot(key_slot($key), %params);
}

=head1 CLUSTER MANAGEMENT METHODS

The following methods can be used for cluster management -- to add or remove a
node, or migrate slot from one node to another.

=cut

=head2 $self->add_new_node($address[, $master_id])

attach node with the specified I<$address> to the cluster. If I<$master_id> is
specified, the new node is configured as a replica of the master with the
specified ID, otherwise it will be a master node itself. Address should be
specified as a hash containing I<host> and I<port> elements.

=cut

sub add_new_node {
    my ( $self, $addr, $master_id ) = @_;
    $addr = _ensure_hash_address($addr);

    my $redis = _connect_to_node( $self, $addr );
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

=head2 $self->migrate_slot($slod, $destination_node)

migrates specified slot to the given I<$destination_node> from the current node
responsible for this slot. Destinations node should be specified as a hash
containing I<host> and I<port> elements. For details check "Cluster live
reconfiguration" section in the L<Redis Cluster
Specification|http://redis.io/topics/cluster-spec>.

=cut

sub migrate_slot {
    my ( $self, $slot, $dst ) = @_;

    # make sure we have up to date information about slots mapping
    $self->_initialize_slots;
    my $src_key = $self->{_slots}[$slot];
    confess "mapping for slot $slot is not defined" unless $src_key;

    # destination node should be part of the cluster
    $dst = $self->_get_node_info($dst)
      or confess "destination node is seems not a part of the cluster";
    my $dst_key = "$dst->{host}:$dst->{port}";
    warn "migrating slot $slot from $src_key to $dst_key" if $DEBUG;

    # if slot is already on destination node, just return
    return if $src_key eq $dst_key;
    my $src = $self->_get_node_info($src_key);

    my $dst_redis = _connect_to_node( $self, $dst )
      or confess "couldn't connect to destination node";
    my $src_redis = _connect_to_node( $self, $src )
      or confess "couldn't connect to source node";

    # set importing/migrating state for the slot
    my $res =
      $dst_redis->cluster( 'setslot', $slot, 'importing', $src->{node_id} );
    confess "$res" unless "$res" eq 'OK';
    $res =
      $src_redis->cluster( 'setslot', $slot, 'migrating', $dst->{node_id} );
    confess "$res" unless "$res" eq 'OK';
    warn "set slots on dst/src nodes to importing/migrating state" if $DEBUG;

    # migrate all keys from src to dst
    my $migrated = 0;
    while (1) {
        my $keys = $src_redis->cluster( 'getkeysinslot', $slot, 1000 );
        confess "Migration failed: $keys" if ref $keys =~ /^RedisDB::Error/;
        last unless @$keys;
        for (@$keys) {
            $res = $src_redis->migrate( $dst->{host}, $dst->{port}, $_, 0, 60 );
            confess "Migration failed: $res" unless "$res" eq 'OK';
            $migrated++;
        }
    }
    warn "migrated $migrated keys from the slot" if $DEBUG;

    $res = $dst_redis->cluster( 'setslot', $slot, 'node', $dst->{node_id} );
    confess "$res" unless "$res" eq 'OK';
    $res = $src_redis->cluster( 'setslot', $slot, 'node', $src->{node_id} );
    confess "$res" unless "$res" eq 'OK';
    warn "migration is finished" if $DEBUG;

    return 1;
}

=head2 $self->remove_node($node)

removes node from the cluster. If the node is a slave, it simply shuts the node
down and sends CLUSTER FORGET command to all other cluster nodes. If the node
is a master node, the method first migrates all slots from it to other nodes.

=cut

sub remove_node {
    my ( $self, $node ) = @_;

    $self->_initialize_slots;
    $node = $self->_get_node_info($node);
    my $node_key = "$node->{host}:$node->{port}";
    if ( $node->{flags}{master} ) {
        my @masters;
        my @slaves;
        for ( @{ $self->{_nodes} } ) {
            if ( $_->{flags}{slave} ) {
                push @slaves, $_ if $_->{master_id} eq $node->{node_id};
                next;
            }
            next if $_->{node_id} eq $node->{node_id};
            push @masters, $_;
        }
        my @slots;
        my %slots_at;
        for my $i ( 0 .. 16383 ) {
            push @slots, $i if $self->{_slots}[$i] eq $node_key;
            $slots_at{ $self->{_slots}[$i] }++;
        }
        if ($DEBUG) {
            warn "Node to remove is a master with "
              . scalar(@slaves)
              . "\nIt holds "
              . scalar(@slots)
              . " slots."
              . "\nThere are "
              . scalar(@masters)
              . " other masters in cluster\n";
        }
        my $slots_per_master  = int( 16384 / @masters + 1 );
        my $slaves_per_master = int( @slaves / @masters + 1 );
        for my $master (@masters) {
            my $key = "$master->{host}:$master->{port}";
            for ( $slots_at{$key} + 1 .. $slots_per_master ) {
                my $slot = shift @slots;
                last unless defined $slot;
                $self->migrate_slot( $slot, $master );
            }
            for ( 1 .. $slaves_per_master ) {
                my $slave = shift @slaves or last;
                my $redis = $self->_connect_to_node($slave) or next;
                my $res = $redis->cluster( 'replicate', $master->{node_id} );
                warn "Failed to reconfigure slave $slave->{host}:$slave->{port}"
                  . " to replicate from $master->{node_id}: $res"
                  if ref $res =~ /^RedisDB::Error/;
            }
        }
    }

    my $redis = delete $self->{_connections}{$node_key};
    $redis->shutdown;
    my @nodes;
    for ( @{ $self->{_nodes} } ) {
        next if $_->{node_id} eq $node->{node_id};
        push @nodes, $_;
        my $redis = $self->_connect_to_node($_) or next;
        my $res = $redis->cluster( 'forget', $node->{node_id} );
        warn "$_->{host}:$_->{port} could not forget the node: $res"
          if $res =~ /^RedisDB::Error/;
    }
    $self->{_nodes} = \@nodes;

    return 1;
}

sub _get_node_info {
    my ( $self, $node ) = @_;
    $node = _ensure_hash_address($node);
    for ( @{ $self->{_nodes} } ) {
        return $_ if $node->{host} eq $_->{host} and $node->{port} eq $_->{port};
    }
    return;
}

sub _ensure_hash_address {
    my $addr = shift;
    unless ( ref $addr eq 'HASH' ) {
        my ( $host, $port ) = split /:([^:]+)$/, $addr;
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
    my $host_key = "$node->{host}:$node->{port}";
    unless ( $self->{_connections}{$host_key} ) {
        my $redis = RedisDB->new(
            host        => $node->{host},
            port        => $node->{port},
            raise_error => 0,
            password    => $self->{_password},
        );
        $self->{_connections}{$host_key} = $redis->{_socket} ? $redis : undef;
    }
    return $self->{_connections}{$host_key};
}

=head1 SERVICE FUNCTIONS

=cut

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

=head2 crc16($buf)

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

=head2 key_slot($key)

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

Copyright 2011-2019 Pavel Shaydo.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut
