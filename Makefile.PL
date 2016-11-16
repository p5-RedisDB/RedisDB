use 5.008004;
use strict;
use warnings;
use ExtUtils::MakeMaker 6.3002;

if ( $^O eq 'MSWin32' ) {
    die "On Win32 module requires perl >= 5.12" if not $^V or $^V lt v5.12;
}

WriteMakefile(
    NAME          => 'RedisDB',
    AUTHOR        => q{Pavel Shaydo <zwon@cpan.org>},
    VERSION_FROM  => 'lib/RedisDB.pm',
    ABSTRACT_FROM => 'lib/RedisDB.pm',
    LICENSE       => 'perl',
    PL_FILES      => {},
    PREREQ_PM     => {
        'Try::Tiny'       => 0,
        Encode            => 2.10,
        'IO::Socket::IP'  => 0,
        'RedisDB::Parser' => 2.21,
        'URI'             => 0,
        'URI::redis'      => 0,
    },
    CONFIGURE_REQUIRES => {
        'ExtUtils::MakeMaker' => 6.3002,
    },
    BUILD_REQUIRES => {
        'Test::More'         => 0.96,
        'Test::Most'         => 0.22,
        'Test::Differences'  => 0.61,
        'Test::FailWarnings' => 0,
        'Test::TCP'          => 1.17,
        'Digest::SHA'        => 0,
    },
    dist       => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean      => { FILES    => 'RedisDB-*' },
    META_MERGE => {
        requires  => { perl => 5.008004, },
        resources => {
            homepage   => 'https://github.com/trinitum/RedisDB',
            bugtracker => 'https://github.com/trinitum/RedisDB/issues',
            repository => 'git://github.com/trinitum/RedisDB',
            license    => 'http://dev.perl.org/licenses/',
        },
        keywords       => ['redis'],
        x_contributors => [
            'Pavel Shaydo <zwon@cpan.org>',
            'HIROSE Masaaki <hirose31@gmail.com>',
            'Norbert Buchmüller <mendel@cpan.org>',
            'Steve Ratcliffe <sr@itinken.com>',
            'Uwe Voelker <uwe.voelker@xing.com>',
            "Andrew O'Brien",
            'Troy Ablan',
        ],
    },
);
