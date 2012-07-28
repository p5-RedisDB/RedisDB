#ifndef REDISDB_PARSER_H
#define REDISDB_PARSER_H

struct rdbp_mblk_store {
    AV* mblk_reply;
    unsigned long mblk_len;
    struct rdbp_mblk_store *next;
};

struct redisdb_parser {
    int utf8;
    SV* redisdb;
    AV* callbacks;
    SV* default_cb;
    SV* buffer;
    int state;
    int mblk_level;
    AV* mblk_reply;
    struct rdbp_mblk_store* mblk_store;
    unsigned long mblk_len;
    unsigned long bulk_len;
    IV thx;
};

typedef struct redisdb_parser RDB_parser;

#define RDBP_CLEAN          0
#define RDBP_READ_LINE      1
#define RDBP_READ_ERROR     2
#define RDBP_READ_NUMBER    3
#define RDBP_READ_BULK_LEN  4
#define RDBP_READ_BULK      5
#define RDBP_READ_MBLK_LEN  6
#define RDBP_WAIT_BUCKS     7

RDB_parser* rdb_parser__init(SV *redisdb, int utf8);
void rdb_parser__free(RDB_parser *parser);
int rdb_parser__parse_reply(RDB_parser *parser);

#endif
