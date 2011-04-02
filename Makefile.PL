use inc::Module::Install;

name     'RedisDB';
all_from 'lib/RedisDB.pm';
author   q{Pavel Shaydo <zwon@cpan.org>};
license  'perl';

build_requires 'Test::More';
build_requires 'Test::Most' => 0.22;

auto_install;

WriteAll;

