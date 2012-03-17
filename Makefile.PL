use 5.008004;
use strict;
use warnings;
use ExtUtils::MakeMaker 6.3002;

WriteMakefile(
    NAME           => 'RedisDB',
    AUTHOR         => q{Pavel Shaydo <zwon@cpan.org>},
    VERSION_FROM   => 'lib/RedisDB.pm',
    ABSTRACT_FROM  => 'lib/RedisDB.pm',
    LICENSE        => 'perl',
    PL_FILES       => {},
    PREREQ_PM      => { 'Try::Tiny' => 0, },
    BUILD_REQUIRES => {
        'Test::More'        => 0.96,
        'Test::Most'        => 0.22,
        'Test::Differences' => 0.61,
    },
    dist       => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean      => { FILES    => 'RedisDB-*' },
    META_MERGE => {
        resources => {
            homepage   => 'https://github.com/trinitum/RedisDB',
            bugtracker => 'https://github.com/trinitum/RedisDB/issues',
            repository => 'git://github.com/trinitum/RedisDB',
            license    => 'http://dev.perl.org/licenses/',
        },
    },
);
