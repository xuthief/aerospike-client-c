#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_lset.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_arraylist_iterator.h>
#include <aerospike/as_error.h>
#include <aerospike/as_ldt.h>
#include <aerospike/as_list.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>

#include "example_utils.h"
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <hiredis/hiredis.h>

static 
int add_elements_to_as(aerospike *p_as, as_ldt *p_lset, char *str_key, size_t count, redisReply **elements);

int
main(int argc, char* argv[])
{
    // Parse command line arguments.
    if (! example_get_opts(argc, argv, EXAMPLE_BASIC_OPTS)) {
        exit(-1);
    }

    // Connect to the aerospike database cluster.
    aerospike as;
    example_connect_to_aerospike(&as);

    // Start clean.
    // example_remove_test_record(&as);

    // Create a large set object to use. No need to destroy lset if using
    // as_ldt_init() on stack object.
    as_ldt lset;
    if (! as_ldt_init(&lset, "mylset", AS_LDT_LSET, NULL)) {
        LOG("unable to initialize ldt");
        // example_cleanup(&as);
        exit(-1);
    }

    // connect redis
    char *str_redis_host = getenv("REDIS_HOST");
    if (!str_redis_host) str_redis_host = "localhost";

    char *str_redis_port = getenv("REDIS_PORT");
    int i_redis_port = 6379;
    if (str_redis_port) i_redis_port = atoi(str_redis_port);

    LOG("redis=%s:%d", str_redis_host, i_redis_port);

    redisContext *c;
    redisReply *reply;

    struct timeval timeout = { 1, 500000 }; // 1.5 seconds
    c = redisConnectWithTimeout(str_redis_host, i_redis_port, timeout);
    if (c == NULL || c->err) {
        if (c) {
            LOG("Connection error: %s\n", c->errstr);
            redisFree(c);
        } else {
            LOG("Connection error: can't allocate redis context\n");
        }
        exit(1);
    }

    /* PING server */
    reply = redisCommand(c,"PING");
    LOG("PING: %s\n", reply->str);
    freeReplyObject(reply);

    /* Set a key */
    unsigned scan_index = 0;
    reply = redisCommand(c,"SCAN %u", scan_index);
    LOG("SCAN %u : %s %u\n", scan_index, reply->str, (unsigned)reply->element[1]->elements);
    freeReplyObject(reply);

    do {
        reply = redisCommand(c,"SCAN %u", scan_index);
        scan_index = reply->element[0]->integer;
        
        for (int i = 0; i<reply->element[1]->elements; i++) {
            char *str_key = reply->element[1]->element[i]->str;
            if (!str_key) continue;

            unsigned sub_sscan_index = 0;
            do {
                redisReply *sub_reply = redisCommand(c,"SSCAN %s %u", str_key, sub_sscan_index);
                sub_sscan_index = sub_reply->element[0]->integer;
                LOG("SSCAN %s %u : %s %u\n", str_key, sub_sscan_index, sub_reply->element[0]->str, (unsigned)sub_reply->element[1]->elements);
                add_elements_to_as(&as, &lset, str_key, sub_reply->element[1]->elements, sub_reply->element[1]->element);
                freeReplyObject(sub_reply);
            } while (sub_sscan_index != 0);
        }

        freeReplyObject(reply);
    } while (scan_index != 0);
}

int add_elements_to_as(aerospike *p_as, as_ldt *p_lset, char *str_key, size_t count, redisReply **elements)
{
    if (!str_key) return 0;

    if (!elements && !count) {
        LOG("no elements (%x) or count (%u)", count, elements);
        return -1;
    }

    as_key_init_str(&g_key, g_namespace, g_set, str_key);

    static as_error err;

    as_arraylist vals;
    as_arraylist_inita(&vals, count);

    for (size_t i=0; i<count; i++) {
        as_arraylist_append_str(&vals, elements[i]->str);
        LOG("append key %s - %s", str_key, elements[i]->str);
    }

    // Add a string value to the set.
    if (aerospike_lset_add_all(p_as, &err, NULL, &g_key, p_lset,
                (const as_list*)&vals) != AEROSPIKE_OK) {
        LOG("aerospike_set_addall() returned %d - %s", err.code,
                err.message);
        return -2;
    }
    as_arraylist_destroy(&vals);
    return 0;
}

