/* vim: set expandtab sts=4: */
#include <EXTERN.h>
#include <perl.h>
#include <XSUB.h>

#include "ppport.h"

struct redisdb_parser {
    int utf8;
    SV* redisdb;
    AV* callbacks;
    SV* buffer;
};

typedef struct redisdb_parser RDB_parser;

MODULE = RedisDB    PACKAGE = RedisDB::Parse::Redis_XS    PREFIX = rdb_parser_
PROTOTYPES: DISABLE

RDB_parser*
rdb_parser__new(redisdb, utf8)
        SV* redisdb;
        int utf8;
    CODE:
        Newx(RETVAL, 1, RDB_parser);
        if(RETVAL == NULL) {
            croak("Couldn't allocate memory for RETVAL");
        }

        RETVAL->utf8 = utf8;

        if(SvROK(redisdb)) {
            RETVAL->redisdb = SvRV(redisdb);
        }
        else {
            RETVAL->redisdb = &PL_sv_undef;
        }

        RETVAL->callbacks = newAV();

        RETVAL->buffer = newSVpvn("", 0);
    OUTPUT:
        RETVAL

void
rdb_parser_DESTROY(parser)
        RDB_parser *parser;
    CODE:
        SvREFCNT_dec(parser->callbacks);
        SvREFCNT_dec(parser->buffer);
        Safefree(parser);
