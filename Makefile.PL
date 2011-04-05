use inc::Module::Install;

name     'RedisDB';
all_from 'lib/RedisDB.pm';
author   q{Pavel Shaydo <zwon@cpan.org>};
license  'perl';

build_requires 'Test::More' => 0.96;
build_requires 'Test::Most' => 0.22;
build_requires 'Test::use::ok';

auto_install;

WriteAll;

