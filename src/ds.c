#include "redis.h"

void ds_init() {
    char *err = NULL;

    server.ds_cache = leveldb_cache_create_lru(server.ds_lru_cache * 1048576);
    server.ds_options = leveldb_options_create();

    server.policy = leveldb_filterpolicy_create_bloom(10);


    //leveldb_options_set_comparator(server.ds_options, cmp);
    leveldb_options_set_filter_policy(server.ds_options, server.policy);
    leveldb_options_set_create_if_missing(server.ds_options, server.ds_create_if_missing);
    leveldb_options_set_error_if_exists(server.ds_options, server.ds_error_if_exists);
    leveldb_options_set_cache(server.ds_options, server.ds_cache);
    leveldb_options_set_info_log(server.ds_options, NULL);
    leveldb_options_set_write_buffer_size(server.ds_options, server.ds_write_buffer_size * 1048576);
    leveldb_options_set_paranoid_checks(server.ds_options, server.ds_paranoid_checks);
    leveldb_options_set_max_open_files(server.ds_options, server.ds_max_open_files);
    leveldb_options_set_block_size(server.ds_options, server.ds_block_size * 1024);
    leveldb_options_set_block_restart_interval(server.ds_options, server.ds_block_restart_interval);
    leveldb_options_set_compression(server.ds_options, leveldb_snappy_compression);

    server.ds_db = leveldb_open(server.ds_options, server.ds_path, &err);
    if (err != NULL) {
        fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, err);
        leveldb_free(err);
        exit(1);
    }

    server.woptions = leveldb_writeoptions_create();
    server.roptions = leveldb_readoptions_create();
    leveldb_readoptions_set_verify_checksums(server.roptions, 0);
    leveldb_readoptions_set_fill_cache(server.roptions, 1);

    leveldb_writeoptions_set_sync(server.woptions, 0);
}

void existsCommand(redisClient *c) {
    char *err;
    size_t val_len;
    char *key = NULL;
    char *value = NULL;

    err = NULL;
    key = (char *) c->argv[1]->ptr;
    value = leveldb_get(server.ds_db, server.roptions, 
                        key, sdslen((sds) key), &val_len, &err);
    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
        return;
    } else if (value == NULL) {
        addReply(c, shared.czero);
        return;
    }

    addReply(c, shared.cone);
    leveldb_free(value);
    return;
}


void getCommand(redisClient *c) {
    char *err;
    size_t val_len;
    char *key = NULL;
    char *value = NULL;


    err = NULL;
    key = (char *) c->argv[1]->ptr;
    value = leveldb_get(server.ds_db, server.roptions, key, sdslen((sds) key), &val_len, &err);
    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
        leveldb_free(value);

        return;
    } else if (value == NULL) {
        addReply(c, shared.nullbulk);
        return;
    }


    addReplyBulkCBuffer(c, value, val_len);

    leveldb_free(value);

    return;
}


/**
 * set if not exists.
 * return 1 (not exists and saved)
 * return 0 (already exists)
 */
void setnxCommand(redisClient *c) {
    char *value;
    sds keyword;
    size_t val_len;
    char *err = NULL;
    leveldb_writebatch_t *wb;

    err = NULL;
    val_len = 0;
    keyword = c->argv[1]->ptr;

    value = leveldb_get(server.ds_db, server.roptions, keyword, sdslen(keyword), &val_len, &err);
    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
        return;
    } else if (val_len > 0) {
        // already exists
        leveldb_free(value);
        addReplyLongLong(c, 0);
        return;
    }

    wb = leveldb_writebatch_create();
    leveldb_writebatch_put(wb, keyword, sdslen(keyword), (char *) c->argv[2]->ptr, sdslen((sds) c->argv[2]->ptr));
    leveldb_write(server.ds_db, server.woptions, wb, &err);
    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
    } else {
        addReplyLongLong(c, 1);
        server.dirty++;
        signalModifiedKey(c->db, c->argv[1]);
    }

    leveldb_writebatch_destroy(wb);
    return;
}

void incrDecrCommand(redisClient *c, long long incr) {
    sds data;
    char *value;
    int64_t val, recore;

    size_t val_len;
    char *err = NULL;

    err = NULL;
    val_len = 0;

    value = leveldb_get(server.ds_db, server.roptions, c->argv[1]->ptr, sdslen((sds) c->argv[1]->ptr), &val_len, &err);
    if (err != NULL) {
        if (val_len > 0) leveldb_free(value);
        addReplyError(c, err);
        leveldb_free(err);
        return;
    } else if (val_len < 1) {
        val = 0;
    } else {
        val = strtoll(value, NULL, 10);
    }

    err = NULL;
    recore = val + incr;
    data = sdsfromlonglong(recore);

    leveldb_put(server.ds_db, server.woptions, c->argv[1]->ptr, sdslen((sds) c->argv[1]->ptr), data, sdslen(data), &err);
    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
    } else {
        addReplyLongLong(c, recore);
        server.dirty++;
        signalModifiedKey(c->db, c->argv[1]);
    }

    sdsfree(data);
    leveldb_free(value);
    return;
}

void incrCommand(redisClient *c) {
    incrDecrCommand(c,1);
}

void decrCommand(redisClient *c) {
    incrDecrCommand(c,-1);
}

void incrbyCommand(redisClient *c) {
    long long incr;

    if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != REDIS_OK) return;
    incrDecrCommand(c,incr);
}

void decrbyCommand(redisClient *c) {
    long long incr;

    if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != REDIS_OK) 
        return;
    incrDecrCommand(c,-incr);
}

void setCommand(redisClient *c) {
    char *key, *value;
    char *err = NULL;
    key = (char *) c->argv[1]->ptr;
    value = (char *) c->argv[2]->ptr;
    leveldb_put(server.ds_db, server.woptions, key, sdslen((sds) key), 
                value, sdslen((sds) value), &err);
    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
        return;
    }
    addReply(c, shared.ok);
    server.dirty++;
    signalModifiedKey(c->db, c->argv[1]);
    return;
}

void delCommand(redisClient *c) {
    int i;
    char *key;
    char *err = NULL;
    int deleted = 0;

    for (i = 1; i < c->argc; i++) {
        key = (char *) c->argv[i]->ptr;
        leveldb_delete(server.ds_db, server.woptions, 
                       key, sdslen((sds) key), &err);
        if (err != NULL) {
            leveldb_free(err);
            continue;
        }
        signalModifiedKey(c->db, c->argv[i]);
        server.dirty++;
        deleted++;
    }

    addReplyLongLong(c, deleted);
    return;
}

void ds_close() {
    leveldb_close(server.ds_db);
    leveldb_readoptions_destroy(server.roptions);
    leveldb_writeoptions_destroy(server.woptions);
    leveldb_options_set_filter_policy(server.ds_options, NULL);
    leveldb_filterpolicy_destroy(server.policy);
    leveldb_options_destroy(server.ds_options);
    leveldb_cache_destroy(server.ds_cache);
}
