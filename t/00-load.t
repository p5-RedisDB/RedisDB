#!perl -T

use Test::More tests => 1;
BEGIN { use_ok('RedisDB'); }

diag("Testing RedisDB $RedisDB::VERSION, Perl $], $^X");

