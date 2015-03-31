/*************************************
 *
 * usage:
 *
 * env -i REDIS_HOST="localhost" REDIS_PORT=6379 ./target/example -h 10.37.129.10 -n topic -s topic
 *
 * ***********************************/

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

//#undef LOG
//#define LOG(...) (void)0
#define ERROR(_fmt, _args...) { printf(_fmt "\n", ## _args); fflush(stdout); }

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
    if (! as_ldt_init(&lset, "lset", AS_LDT_LSET, NULL)) {
        ERROR("unable to initialize ldt");
        // example_cleanup(&as);
        exit(-1);
    }

    // connect redis
    char *str_redis_host = getenv("REDIS_HOST");
    if (!str_redis_host) str_redis_host = "localhost";

    char *str_redis_port = getenv("REDIS_PORT");
    int i_redis_port = 6379;
    if (str_redis_port) i_redis_port = atoi(str_redis_port);

    ERROR("redis = %s:%d", str_redis_host, i_redis_port);

    redisContext *c;
    redisReply *reply;

    struct timeval timeout = { 1, 500000 }; // 1.5 seconds
    c = redisConnectWithTimeout(str_redis_host, i_redis_port, timeout);
    if (c == NULL || c->err) {
        if (c) {
            ERROR("Connection error: %s\n", c->errstr);
            redisFree(c);
        } else {
            ERROR("Connection error: can't allocate redis context\n");
        }
        exit(1);
    }

    /* PING server */
    reply = redisCommand(c,"PING");
    LOG("PING: %s\n", reply->str);
    freeReplyObject(reply);

    /* Set a key */
    long scan_index = 0;
    reply = redisCommand(c,"SCAN %ld", scan_index);
    freeReplyObject(reply);

    do {
        reply = redisCommand(c,"SCAN %ld", scan_index);
        scan_index = atol(reply->element[0]->str);
        LOG("SCAN %u : %s %u\n", scan_index, reply->element[0]->str, (unsigned)reply->element[1]->elements);
        
        for (int i = 0; i<reply->element[1]->elements; i++) {
            char *str_key = reply->element[1]->element[i]->str;
            if (!str_key) continue;

            long sub_sscan_index = 0;
            do {
                redisReply *sub_reply = redisCommand(c,"SSCAN %s %ld", str_key, sub_sscan_index);
                sub_sscan_index = atol(sub_reply->element[0]->str);
                LOG("SSCAN %s %ld : %s %u\n", str_key, sub_sscan_index, sub_reply->element[0]->str, (unsigned)sub_reply->element[1]->elements);

                if (add_elements_to_as(&as, &lset, str_key, sub_reply->element[1]->elements, sub_reply->element[1]->element) != 0) {
                    ERROR("add elements to as failed for key %s sub_sscan_index %ld", str_key, sub_sscan_index);
                }
                freeReplyObject(sub_reply);
            } while (sub_sscan_index != 0);
        }

        freeReplyObject(reply);
    } while (scan_index != 0);
}

int add_elements_to_as(aerospike *p_as, as_ldt *p_lset, char *str_key, size_t count, redisReply **elements)
{
    if (!str_key) return 0;

    size_t len = strlen(str_key);

    if (!elements && !count) {
        ERROR("no elements (%x) or count (%u)", count, elements);
        return -1;
    }

    as_key_init_rawp(&g_key, g_namespace, g_set, str_key, len, false);

    static as_error err;
    //as_string sval;
    as_bytes bval;

    for (size_t i=0; i<count; i++) {
        LOG("add key %s(%u) - %s(%u)", str_key, len, elements[i]->str, elements[i]->len);

        //as_string_init(&sval, elements[i]->str, false);
        as_bytes_init_wrap(&bval, elements[i]->str, elements[i]->len, false);

        if (aerospike_lset_add(p_as, &err, NULL, &g_key, p_lset,
                    (const as_val*)&bval) != AEROSPIKE_OK) {
            if (err.code != AEROSPIKE_ERR_UDF) {
                ERROR("aerospike_set_add() returned %d - %s", err.code,
                    err.message);
                return -2;
            }
        }
    }

#if 0
    as_arraylist vals;
    as_arraylist_inita(&vals, count);

    for (size_t i=0; i<count; i++) {
        as_arraylist_append_str(&vals, elements[i]->str);
        LOG("append key %s - %s", str_key, elements[i]->str);
    }

    // Add a string value to the set.
    if (aerospike_lset_add_all(p_as, &err, NULL, &g_key, p_lset,
                (const as_list*)&vals) != AEROSPIKE_OK) {
        if (err.code != AEROSPIKE_ERR_UDF) {
            ERROR("aerospike_set_add_all() returned %d - %s", err.code,
                    err.message);
            return -2;
        }
    }
    as_arraylist_destroy(&vals);

#endif

    if (1) {
        uint32_t count = 0;
        if(aerospike_lset_size(p_as, &err, NULL, &g_key, p_lset, &count) != AEROSPIKE_OK) {
            ERROR("get count failed");
            return -3;
        }
        ERROR("key %s count after add %u", str_key, count);
    }

    return 0;
}

