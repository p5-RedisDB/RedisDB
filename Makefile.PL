use 5.008004;
use strict;
use warnings;
use ExtUtils::MakeMaker 6.3002;
use ExtUtils::CBuilder 0.27;

# Should we build the XS version?
my $make_xs = undef;
$make_xs = 1 if ExtUtils::CBuilder->new( quiet => 1 )->have_compiler;
foreach (@ARGV) {
    /^-pm/ and $make_xs = 0;
}

if ($make_xs) {
    print <<EOM;
**************************************
* Building XS version of the parser. *
* If build failed, you can try to    *
* build PP version by running        *
*                                    *
*     perl Makefile.PL -pm           *
*     make                           *
**************************************
EOM
}

WriteMakefile(
    NAME          => 'RedisDB',
    AUTHOR        => q{Pavel Shaydo <zwon@cpan.org>},
    VERSION_FROM  => 'lib/RedisDB.pm',
    ABSTRACT_FROM => 'lib/RedisDB.pm',
    LICENSE       => 'perl',
    PL_FILES      => {},
    OBJECT        => "parser.o RedisDB.o",
    PREREQ_PM     => {
        'Try::Tiny' => 0,
        Encode      => 2.10,
    },
    CONFIGURE_REQUIRES => {
        'ExtUtils::MakeMaker' => 6.3002,
        'ExtUtils::CBuilder'  => 0.27,
    },
    BUILD_REQUIRES => {
        'Test::More'        => 0.96,
        'Test::Most'        => 0.22,
        'Test::Differences' => 0.61,
        'Digest::SHA'       => 0,
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
    },
    CONFIGURE => sub {
        my $hash = $_[1];
        unless ($make_xs) {
            $hash->{XS}     = {};
            $hash->{C}      = [];
            $hash->{OBJECT} = '';
        }
        return $hash;
    },
);
