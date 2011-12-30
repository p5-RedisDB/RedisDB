use inc::Module::Install;

name     'RedisDB';
all_from 'lib/RedisDB.pm';
author   q{Pavel Shaydo <zwon@cpan.org>};
license  'perl';
perl_version '5.008004';

homepage 'http://github.com/trinitum/RedisDB';
repository 'git://github.com/trinitum/RedisDB';
bugtracker 'http://github.com/trinitum/RedisDB/issues';

requires 'Try::Tiny';

build_requires 'Test::More' => 0.96;
build_requires 'Test::Most' => 0.22;
build_requires 'Test::Differences' => 0.61;
build_requires 'Test::use::ok';

auto_install;

WriteAll;

