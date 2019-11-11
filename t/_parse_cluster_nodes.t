use Test::Most 0.22;
use RedisDB;

subtest '_parse_cluster_nodes' => sub {
    subtest 'IPv4' => sub {
        my $redis = RedisDB->new(lazy => 1, host => 'localhost');
        my $nodes = $redis->_parse_cluster_nodes(<<'NODES');
64efdda59b24c32f47600f102db575bb6b1d07f1 172.21.0.5:6380 master - 0 1573454998851 2 connected 5461-10922
4afb93978bddcda15f7df72e77ce0f533c228af8 172.21.0.5:6381 master - 0 1573454999858 3 connected 10923-16383
298420d2cddeb8cd2b3f0b45414fc405c8894fb8 172.21.0.5:6379 myself,master - 0 1573454996000 1 connected 0-5460
NODES
        is $nodes->[0]->{host}, '172.21.0.5';
        is $nodes->[0]->{port}, '6380';
        is $nodes->[1]->{host}, '172.21.0.5';
        is $nodes->[1]->{port}, '6381';
        is $nodes->[2]->{host}, '172.21.0.5';
        is $nodes->[2]->{port}, '6379';
    };

    subtest 'IPv6' => sub {
        my $redis = RedisDB->new(lazy => 1, host => 'localhost');
        my $nodes = $redis->_parse_cluster_nodes(<<'NODES');
64efdda59b24c32f47600f102db575bb6b1d07f1 2001:db8:a:b:1:2:3:4:6380 master - 0 1573454998851 2 connected 5461-10922
4afb93978bddcda15f7df72e77ce0f533c228af8 2001:db8:a:b:1:2:3:4:6381 master - 0 1573454999858 3 connected 10923-16383
298420d2cddeb8cd2b3f0b45414fc405c8894fb8 2001:db8:a:b:1:2:3:4:6379 myself,master - 0 1573454996000 1 connected 0-5460
NODES
        is $nodes->[0]->{host}, '2001:db8:a:b:1:2:3:4';
        is $nodes->[0]->{port}, '6380';
        is $nodes->[1]->{host}, '2001:db8:a:b:1:2:3:4';
        is $nodes->[1]->{port}, '6381';
        is $nodes->[2]->{host}, '2001:db8:a:b:1:2:3:4';
        is $nodes->[2]->{port}, '6379';
    };
};

subtest '_parse_cluster_nodes with cport' => sub {
    subtest 'IPv4' => sub {
        my $redis = RedisDB->new(lazy => 1, host => 'localhost');
        my $nodes = $redis->_parse_cluster_nodes(<<'NODES');
64efdda59b24c32f47600f102db575bb6b1d07f1 172.21.0.5:6380@16380 master - 0 1573454998851 2 connected 5461-10922
4afb93978bddcda15f7df72e77ce0f533c228af8 172.21.0.5:6381@16381 master - 0 1573454999858 3 connected 10923-16383
298420d2cddeb8cd2b3f0b45414fc405c8894fb8 172.21.0.5:6379@16379 myself,master - 0 1573454996000 1 connected 0-5460
NODES
        is $nodes->[0]->{host}, '172.21.0.5';
        is $nodes->[0]->{port}, '6380';
        is $nodes->[1]->{host}, '172.21.0.5';
        is $nodes->[1]->{port}, '6381';
        is $nodes->[2]->{host}, '172.21.0.5';
        is $nodes->[2]->{port}, '6379';
    };

    subtest 'IPv6' => sub {
        my $redis = RedisDB->new(lazy => 1, host => 'localhost');
        my $nodes = $redis->_parse_cluster_nodes(<<'NODES');
64efdda59b24c32f47600f102db575bb6b1d07f1 2001:db8:a:b:1:2:3:4:6380@16380 master - 0 1573454998851 2 connected 5461-10922
4afb93978bddcda15f7df72e77ce0f533c228af8 2001:db8:a:b:1:2:3:4:6381@16381 master - 0 1573454999858 3 connected 10923-16383
298420d2cddeb8cd2b3f0b45414fc405c8894fb8 2001:db8:a:b:1:2:3:4:6379@16379 myself,master - 0 1573454996000 1 connected 0-5460
NODES
        is $nodes->[0]->{host}, '2001:db8:a:b:1:2:3:4';
        is $nodes->[0]->{port}, '6380';
        is $nodes->[1]->{host}, '2001:db8:a:b:1:2:3:4';
        is $nodes->[1]->{port}, '6381';
        is $nodes->[2]->{host}, '2001:db8:a:b:1:2:3:4';
        is $nodes->[2]->{port}, '6379';
    };
};

done_testing;
