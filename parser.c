#include <EXTERN.h>
#include <perl.h>
#include <XSUB.h>

#define NEED_newRV_noinc
#define NEED_sv_2pv_flags
#include "ppport.h"

#include "parser.h"

RDB_parser* rdb_parser__init(SV* redisdb, int utf8) {
    RDB_parser *parser;

    Newx(parser, 1, RDB_parser);
    if (parser == NULL) {
        croak("Couldn't allocate memory for parser");
    }

    if (SvROK(redisdb)) {
        parser->redisdb = SvRV(redisdb);
    }
    else {
        parser->redisdb = &PL_sv_undef;
    }

    parser->utf8 = utf8;
    parser->callbacks = newAV();
    parser->default_cb = NULL;
    parser->mblk_reply = NULL;
    parser->mblk_store = NULL;
    parser->buffer = newSVpvn("", 0);
    parser->state = RDBP_CLEAN;

    return parser;
}

void rdb_parser__free(RDB_parser *parser) {
    struct rdbp_mblk_store *store, *next;

    SvREFCNT_dec(parser->callbacks);
    SvREFCNT_dec(parser->default_cb);
    SvREFCNT_dec(parser->buffer);
    SvREFCNT_dec(parser->mblk_reply);

    store = parser->mblk_store;
    while (store != NULL) {
        next = store->next;
        SvREFCNT_dec(store->mblk_reply);
        Safefree(store);
        store = next;
    }

    Safefree(parser);
}

void rdb_parser__propagate_reply(RDB_parser *parser, SV *reply) {
    SV *cb;

    while (1) {
        if(av_len(parser->callbacks) >= 0) {
            cb = av_shift(parser->callbacks);
            sv_2mortal(cb);
        }
        else if (parser->default_cb != NULL) {
            cb = parser->default_cb;
            parser->default_cb = NULL;
        }
        else {
            break;
        }

        {
            dSP;
            ENTER;
            SAVETMPS;
            PUSHMARK(SP);
            XPUSHs(sv_2mortal(newRV_inc(parser->redisdb)));
            XPUSHs(sv_2mortal(newSVsv(reply)));
            PUTBACK;
            call_sv(cb, G_VOID|G_DISCARD);
            FREETMPS;
            LEAVE;
        }
    }
}

static
long _line_length(char *start, size_t length) {
    int i;
    char *pos = start;

    for (i=0; i < length - 1; i++, pos++) {
        if (*pos == '\r' && pos[1] == '\n') {
            return (long)(pos - start);
        }
    }

    return -1;
}

/*
 * read line from the buffer and return it as SV
 */
static
SV* _read_line(SV *buffer) {
    char *pv;
    long len;
    SV *line = NULL;

    pv  = SvPVX(buffer);
    len = _line_length(pv, sv_len(buffer));
    if (len >= 0) {
        line = newSVpvn(pv, len);
        sv_chop(buffer, pv + len + 2);
    }

    return line;
};

/*
 * read line containing integer number
 */
static
SV* _read_number(SV *buffer) {
    char *pv;
    long len;
    SV *num = NULL;

    pv = SvPVX(buffer);
    len = _line_length(pv, sv_len(buffer));
    if (len >= 0) {
        pv[len] = 0;
        num = newSViv(atol(pv));
        sv_chop(buffer, pv + len + 2);
    }

    return num;
}

/*
 * read line containing length
 * returns length >= -1, or -2 if line is not finished
 */
static
long _read_length(SV *buffer) {
    char *pv;
    long len;
    long num = -2;

    pv = SvPVX(buffer);
    len = _line_length(pv, sv_len(buffer));
    if (len >= 0) {
        pv[len] = 0;
        num = atol(pv);
        sv_chop(buffer, pv + len + 2);
    }

    return num;
}

/*
 * creates RedisDB::Error object from the message
 */
static
SV* _create_rdb_error(SV *msg) {
    int count;
    SV* err;

    dSP;
    ENTER;
    SAVETMPS;
    PUSHMARK(SP);
    XPUSHs(sv_2mortal(newSVpv("RedisDB::Error", 0)));
    XPUSHs(sv_2mortal(msg));
    PUTBACK;
    count = call_pv("RedisDB::Error::new", G_SCALAR);
    if (count != 1)
        croak("Expected single return value from new, but got many");
    SPAGAIN;
    err = newSVsv(POPs);
    PUTBACK;
    FREETMPS;
    LEAVE;

    return err;
}

/*
 * stores current multibulk reply status in mblk_stack
 */
static
void _mblk_status_store(RDB_parser *parser) {
    struct rdbp_mblk_store *store;
    Newx(store, 1, struct rdbp_mblk_store);
    store->mblk_reply  = parser->mblk_reply;
    parser->mblk_reply = NULL;
    store->mblk_len    = parser->mblk_len;
    store->next        = parser->mblk_store;
    parser->mblk_store = store;
}

/*
 * fetches status of the multibulk reply from the mblk_stack
 */
static
void _mblk_status_fetch(RDB_parser *parser) {
    struct rdbp_mblk_store *store;

    store = parser->mblk_store;
    if (store == NULL)
        croak("Already at the upper level of multi-bulk reply");
    parser->mblk_len   = store->mblk_len;
    parser->mblk_reply = store->mblk_reply;
    parser->mblk_store = store->next;
    Safefree(store);
}

/*
 * process new value of multi-bulk reply
 */
static
int _mblk_item(RDB_parser *parser, SV *value) {
    SV *tmp;
    int repeat = 0;

    av_push(parser->mblk_reply, value);
    if (parser->mblk_len > 1) {
        parser->mblk_len--;
        parser->state = RDBP_WAIT_BUCKS;
        repeat = 1;
    }
    else if (parser->mblk_level > 1) {
        parser->mblk_level--;
        tmp = newRV_noinc((SV *)(parser->mblk_reply));
        _mblk_status_fetch(parser);
        repeat = _mblk_item(parser, tmp);
    }

    return repeat;
}

/*
 * check if we finished with this reply, and invoke callback if needed.
 * returns 1 if reply completed or 0 otherwise
 */
static
int _reply_completed(RDB_parser *parser, SV *value) {
    SV *reply, *cb;

    if (parser->mblk_level) {
        if (_mblk_item(parser, value))
            return 0;
        reply = newRV_noinc((SV *)(parser->mblk_reply));
        parser->mblk_reply = NULL;
    }
    else
        reply = value;

    parser->state = RDBP_CLEAN;

    {
        dSP;
        ENTER;
        SAVETMPS;
        if (av_len(parser->callbacks) >= 0) {
            cb = av_shift(parser->callbacks);
            sv_2mortal(cb);
        }
        else if (parser->default_cb != NULL) {
            cb = parser->default_cb;
        }
        else croak("No callbacks in the queue and no default callback set");
        PUSHMARK(SP);
        XPUSHs(sv_2mortal(newRV_inc(parser->redisdb)));
        XPUSHs(sv_2mortal(reply));
        PUTBACK;
        call_sv(cb, G_VOID|G_DISCARD);
        FREETMPS;
        LEAVE;
    }

    return 1;
}

int rdb_parser__parse_reply(RDB_parser *parser) {
    char op;
    char *pv;
    SV *line, *err, *bulk, *mblk;
    long length;

    if (sv_len(parser->buffer) == 0) return 0;

    if (parser->state == RDBP_CLEAN) {
        parser->mblk_level = 0;

        /* remove first character from the buffer */
        pv = SvPVX(parser->buffer);
        op = *pv;
        sv_chop(parser->buffer, pv + 1);

        if (op == '+')
            parser->state = RDBP_READ_LINE;
        else if (op == '-')
            parser->state = RDBP_READ_ERROR;
        else if (op == ':')
            parser->state = RDBP_READ_NUMBER;
        else if (op == '$')
            parser->state = RDBP_READ_BULK_LEN;
        else if (op == '*') {
            parser->state = RDBP_READ_MBLK_LEN;
            parser->mblk_level = 1;
        }
        else {
            croak("Got invalid reply");
        }
    }

    while (1) {
        if (sv_len(parser->buffer) < 2) return 0;

        if (parser->state == RDBP_READ_LINE) {
            line = _read_line(parser->buffer);
            if (line == NULL) return 0;
            if (_reply_completed(parser, line)) return 1;
        }
        else if (parser->state == RDBP_READ_ERROR) {
            line = _read_line(parser->buffer);
            if (line == NULL) return 0;
            err = _create_rdb_error(line);
            if (_reply_completed(parser, err)) return 1;
        }
        else if (parser->state == RDBP_READ_NUMBER) {
            line = _read_number(parser->buffer);
            if (line == NULL) return 0;
            if (_reply_completed(parser, line)) return 1;
        }
        else if (parser->state == RDBP_READ_BULK_LEN) {
            length = _read_length(parser->buffer);
            if (length >= 0) {
                parser->state = RDBP_READ_BULK;
                parser->bulk_len = length;
            }
            else if (length == -1) {
                if (_reply_completed(parser, newSVpvn(NULL, 0))) return 1;
            }
            else return 0;
        }
        else if (parser->state == RDBP_READ_BULK) {
            if (sv_len(parser->buffer) < 2 + parser->bulk_len) return 0;
            pv = SvPVX(parser->buffer);
            bulk = newSVpvn(pv, parser->bulk_len);
            sv_chop(parser->buffer, pv + parser->bulk_len + 2);
            if (parser->utf8) {
                if (!sv_utf8_decode(bulk))
                    croak("Received invalid UTF-8 string from the server");
            }
            if (_reply_completed(parser, bulk)) return 1;
        }
        else if (parser->state == RDBP_READ_MBLK_LEN) {
            length = _read_length(parser->buffer);
            if (length > 0) {
                parser->mblk_len = length;
                parser->state = RDBP_WAIT_BUCKS;
                parser->mblk_reply = newAV();
            }
            else if (length == 0 || length == -1) {
                mblk = (length == 0) ? newRV_noinc((SV *)newAV()) : newSVpvn(NULL, 0);
                parser->mblk_level--;
                if (parser->mblk_level > 0)
                    _mblk_status_fetch(parser);
                if (_reply_completed(parser, mblk)) return 1;
            }
            else return 0;
        }
        else if (parser->state == RDBP_WAIT_BUCKS) {
            /* remove first character from the buffer */
            pv = SvPVX(parser->buffer);
            op = *pv;
            sv_chop(parser->buffer, pv + 1);

            if (op == '$') parser->state = RDBP_READ_BULK_LEN;
            else if (op == ':') parser->state = RDBP_READ_NUMBER;
            else if (op == '+') parser->state = RDBP_READ_LINE;
            else if (op == '-') parser->state = RDBP_READ_ERROR;
            else if (op == '*') {
                parser->state = RDBP_READ_MBLK_LEN;
                parser->mblk_level++;
                _mblk_status_store(parser);
            }
            else croak("Invalid multi-bulk reply. Expected [$:+-*] but got something else");
        }
    }

    return 0;
}

