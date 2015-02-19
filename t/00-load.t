#!perl -T

use Test::More tests => 2;
BEGIN { use_ok('RedisDB'); }
BEGIN { use_ok('RedisDB::Cluster'); }

diag("Testing RedisDB $RedisDB::VERSION, Perl $], $^X");

