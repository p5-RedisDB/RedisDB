use inc::Module::Install;

name     'RedisDB';
all_from 'lib/RedisDB.pm';
author   q{Pavel Shaydo <zwon@cpan.org>};
license  'perl';

build_requires 'Test::More';

auto_install;

WriteAll;

