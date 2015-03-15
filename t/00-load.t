#!perl -T

use Test::More tests => 3;
BEGIN { use_ok('RedisDB'); }
BEGIN { use_ok('RedisDB::Cluster'); }
BEGIN { use_ok('RedisDB::Sentinel'); }

diag("Testing RedisDB $RedisDB::VERSION, Perl $], $^X");

