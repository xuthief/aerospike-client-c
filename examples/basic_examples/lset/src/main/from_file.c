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

    FILE *fp = fopen("in.log", "r");
    if(!fp) 
    {
        LOG("open file failed %s", strerror(errno));
        return -1;
    }

    size_t linecap = 4096;
    char * line = (char *)malloc(linecap);
    ssize_t linelen;
    as_error err;
    while ((linelen = getline(&line, &linecap, fp)) > 0)
        add_line_to_as(line);
}

int add_line_to_as(char *line)
{
    if (!line) return 0;
    char *str_key = strtok(line, " ");
    if (!str_key)
    {
        LOG("no strkey, error %s", strerror(errno));
        return -1;
    }

    as_key_init_str(&g_key, g_namespace, g_set, str_key);

    static size_t sz_size = 1;
    static char **sz_val = (char **)malloc(sizeof(char*) * sz_size);

    size_t sz_count = 0;
    char *str_val = NULL;
    while( (str_val = strtok(NULL, " ")) )
    {
        LOG("%s - %s", str_key, str_val);
        while(sz_count >= sz_size) {
            sz_size *= 2;
            sz_val = (char **)realloc(sizeof(char*) * sz_size);
        }
        sz_val[sz_count++] = str_val;
    }

    if (!sz_count) continue;

    as_arraylist vals;
    as_arraylist_inita(&vals, sz_count);

    for (size_t i=0; i<sz_count; i++) {
        as_arraylist_append_str(&vals, sz_val[i]);
    }

    // Add a string value to the set.
    if (aerospike_lset_add_all(&as, &err, NULL, &g_key, &lset,
                (const as_list*)&vals) != AEROSPIKE_OK) {
        LOG("aerospike_set_addall() returned %d - %s", err.code,
                err.message);
        return -2;
    }
    as_arraylist_destroy(&vals);
    return 0;
}

