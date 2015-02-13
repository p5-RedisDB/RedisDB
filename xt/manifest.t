#!perl -T

use strict;
use warnings;
use Test::More;
use Test::CheckManifest 0.9;

ok_manifest({exclude => ['/.git', '/.travis.yml']});
