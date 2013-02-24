use strict;
use warnings;
use Test::More;
use Test::Spelling 0.15;
use RedisDB;

chomp(my @stopwords = <DATA>);
push @stopwords, RedisDB->_simple_commands;
add_stopwords(@stopwords);
all_pod_files_spelling_ok();
__DATA__
XS
latin
op
postprocessing
psubscribe
unsubscribed
unwatches
redis
redisdb
Redis
RedisDB
utf
UTF
SETNAME
mainloop
unwatch
unsubscribe
Unsubscribe
auth
callback
callbacks
OSes
SNDTIMEO
RCVTIMEO
utf
Pavel
Shaydo
zwon
cpan
org
http
trinitum
GitHub
com
cb
perl
dev
HIROSE
Masaake
FunkyMonk
Sanko
