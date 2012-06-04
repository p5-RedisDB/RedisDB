/* vim: set expandtab sts=4: */
#include <EXTERN.h>
#include <perl.h>
#include <XSUB.h>

#include "ppport.h"

struct redisdb_parser {
    int utf8;
    SV* redisdb;
    AV* callbacks;
    SV* default_cb;
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
        RETVAL->default_cb = NULL;

        RETVAL->buffer = newSVpvn("", 0);
    OUTPUT:
        RETVAL

void
rdb_parser_DESTROY(parser)
        RDB_parser *parser;
    CODE:
        SvREFCNT_dec(parser->callbacks);
        SvREFCNT_dec(parser->default_cb);
        SvREFCNT_dec(parser->buffer);
        Safefree(parser);

SV*
rdb_parser_build_request(parser, ...)
        RDB_parser *parser;
    INIT:
        int i;
        STRLEN len;
        char *pv;
    CODE:
        RETVAL = newSV(128);
        sv_setpvf(RETVAL, "*%ld\r\n", items - 1);
        for(i = 1; i < items; i++) {
            pv = SvPV(ST(i), len);
            sv_catpvf(RETVAL, "$%d\r\n", len);
            sv_catpvn(RETVAL, pv, len);
            sv_catpvn(RETVAL, "\r\n", 2);
        }
    OUTPUT:
        RETVAL

int
rdb_parser_add_callback(parser, cb)
        RDB_parser *parser;
        SV* cb;
    CODE:
        SvREFCNT_inc(cb);
        av_push(parser->callbacks, cb);
        RETVAL = 1 + av_len(parser->callbacks);
    OUTPUT:
        RETVAL

SV*
rdb_parser_set_default_callback(parser, cb)
        RDB_parser *parser;
        SV* cb;
    CODE:
        if (parser->default_cb != NULL)
            SvSetSV(parser->default_cb, cb);
        else
            parser->default_cb = newSVsv(cb);
        RETVAL = newSVsv(cb);
    OUTPUT:
        RETVAL

void
rdb_parser_callbacks(parser)
        RDB_parser *parser;
    INIT:
        int i, len;
        SV **ptr;
    PPCODE:
        len = 1 + av_len(parser->callbacks);
        if (GIMME_V != G_ARRAY) {
            XPUSHs(sv_2mortal(newSViv(len)));
        }
        else {
            EXTEND(SP, len);
            for (i=0; i < len; i++) {
                ptr = av_fetch(parser->callbacks, i, 0);
                if (ptr == NULL) {
                    croak("Callback doesn't exist");
                }
                PUSHs(*ptr);
            }
        }

int
add(parser, data)
        RDB_parser *parser;
        SV* data;
    CODE:
        RETVAL = 1;
    OUTPUT:
        RETVAL
