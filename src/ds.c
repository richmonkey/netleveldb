#include "redis.h"

#define KEY_PREFIX_HASH "\xFF\x01"
#define KEY_PREFIX_ZSET "\xFF\x02"
#define KEY_PREFIX_SET  "\xFF\x03"
#define KEY_PREFIX_LENGTH 2
#define MEMBER_PREFIX   "\x01"
#define MEMBER_PREFIX_LENGTH 1

// 常规key不使用前缀，用户自己控制自己的key不带有上述前缀

void ds_init() {
    if(!server.ds_open) {
        return ;
    }
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

void ds_exists(redisClient *c) {
    int i;
    char *err;
    leveldb_iterator_t *iter;
    char *kp;
    size_t kl;
    sds str, ret;

    iter = leveldb_create_iterator(server.ds_db, server.roptions);
    //addReplyMultiBulkLen(c, c->argc-1);

    str = sdsempty();

    for (i = 1; i < c->argc; i++) {
        leveldb_iter_seek(iter, c->argv[i]->ptr, sdslen((sds) c->argv[i]->ptr));
        if (leveldb_iter_valid(iter)) {

            kp = (char *) leveldb_iter_key(iter, &kl);

            if (sdslen((sds) c->argv[i]->ptr) == kl && 0 == memcmp(c->argv[i]->ptr, kp, kl))
                //addReplyLongLong(c,1);
                str = sdscatlen(str, ":1\r\n", 4);
            else
                //addReplyLongLong(c,0);
                str = sdscatlen(str, ":0\r\n", 4);

        } else
            //addReplyLongLong(c, 0);
            str = sdscatlen(str, ":0\r\n", 4);
    }

    err = NULL;
    leveldb_iter_get_error(iter, &err);
    leveldb_iter_destroy(iter);

    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
        sdsfree(str);
        return;
    }

    ret = sdsempty();
    ret = sdscatprintf(ret, "*%d\r\n", c->argc - 1);
    ret = sdscatsds(ret, str);
    addReplySds(c, ret);
    sdsfree(str);
    return;
}

/**
 * usage: ds_keys_count startKey endKey
 * return: integer
 */
void ds_keys_count(redisClient *c) {
    char *err;

    const char *key;
    size_t key_len, len, i;

    leveldb_iterator_t *iter;

    char *skey, *ekey; //  key pair
    size_t skey_len, ekey_len;

    char *k1, *k2;
    int cmp;

    skey = c->argv[1]->ptr;
    skey_len = sdslen(c->argv[1]->ptr);

    ekey = c->argv[2]->ptr;
    ekey_len = sdslen(c->argv[2]->ptr);


    // return 0 when skey > ekey
    k1 = zmalloc(skey_len + 1);
    k2 = zmalloc(ekey_len + 1);
    memcpy(k1, skey, skey_len);
    memcpy(k2, ekey, ekey_len);
    k1[skey_len] = k2[ekey_len] = '\0';

    cmp = strcmp(k1, k2);

    if (cmp > 0) {
        zfree(k1);
        zfree(k2);
        addReplyLongLong(c, 0);
        return;
    }

    i = 0;
    len = 0;

    iter = leveldb_create_iterator(server.ds_db, server.roptions);

    leveldb_iter_seek(iter, skey, skey_len);

    for (; leveldb_iter_valid(iter); leveldb_iter_next(iter)) {
        key_len = 0;
        key = leveldb_iter_key(iter, &key_len);

        // skip KEY_PREFIX_
        if (((unsigned char*) key)[0] == 0xFF) {

            //位于haskkey 空间，后面的key无需扫描
            break;
            //continue;
        }

        if (k1 != NULL) {
            zfree(k1);
            k1 = NULL;
        }

        k1 = zmalloc(key_len + 1);
        memcpy(k1, key, key_len);
        k1[key_len] = '\0';

        cmp = strcmp(k1, k2);

        if (cmp > 0)
            break;

        i++;
    }

    err = NULL;
    leveldb_iter_get_error(iter, &err);
    leveldb_iter_destroy(iter);

    if (err) {
        addReplyError(c, err);
        leveldb_free(err);
    } else {
        addReplyLongLong(c, i);
    }

    if (k1) zfree(k1);
    if (k2) zfree(k2);

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
void ds_hsetnx(redisClient *c) {
    char *value;
    sds keyword;

    size_t val_len;
    char *err = NULL;
    leveldb_writebatch_t *wb;

    err = NULL;
    val_len = 0;
    keyword = sdsempty();
    keyword = sdscatlen(keyword, KEY_PREFIX_HASH, KEY_PREFIX_LENGTH);
    keyword = sdscatsds(keyword, c->argv[1]->ptr);
    keyword = sdscatlen(keyword, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);

    wb = leveldb_writebatch_create();
    // <hash_prefix>key<member prefix>
    leveldb_writebatch_put(wb, keyword, sdslen(keyword), "1", 1);

    // append hash field
    keyword = sdscatsds(keyword, c->argv[2]->ptr);

    value = leveldb_get(server.ds_db, server.roptions, keyword, sdslen(keyword), &val_len, &err);
    if (err != NULL) {
        //error
        sdsfree(keyword);
        if (val_len > 0) leveldb_free(value);
        addReplyError(c, err);
        leveldb_free(err);
        leveldb_writebatch_destroy(wb);
        return;
    } else if (val_len > 0) {
        // already exists
        sdsfree(keyword);
        leveldb_free(value);
        addReplyLongLong(c, 0);
        leveldb_writebatch_destroy(wb);
        return;
    }

    err = NULL;
    leveldb_writebatch_put(wb, keyword, sdslen(keyword), (char *) c->argv[3]->ptr, sdslen((sds) c->argv[3]->ptr));
    leveldb_write(server.ds_db, server.woptions, wb, &err);
    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
    } else {
        addReplyLongLong(c, 1);
    }

    sdsfree(keyword);
    leveldb_writebatch_destroy(wb);
    return;
}


void ds_incrby(redisClient *c) {
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
    recore = strtoll(c->argv[2]->ptr, NULL, 10);
    recore = val + recore;
    data = sdsfromlonglong(recore);

    leveldb_put(server.ds_db, server.woptions, c->argv[1]->ptr, sdslen((sds) c->argv[1]->ptr), data, sdslen(data), &err);
    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
    } else {
        addReplyLongLong(c, recore);
    }

    sdsfree(data);
    leveldb_free(value);
    return;
}

void setCommand(redisClient *c) {
    char *key, *value;
    char *err = NULL;
    key = (char *) c->argv[1]->ptr;
    value = (char *) c->argv[2]->ptr;
    leveldb_put(server.ds_db, server.woptions, key, sdslen((sds) key), value, sdslen((sds) value), &err);
    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
        return;
    }
    addReply(c, shared.ok);
    return;
}

void delCommand(redisClient *c) {
    int i;
    char *key;
    char *err = NULL;
    leveldb_writebatch_t *wb;

    if (c->argc < 3) {
        key = (char *) c->argv[1]->ptr;
        leveldb_delete(server.ds_db, server.woptions, key, sdslen((sds) key), &err);
        if (err != NULL) {
            addReplyError(c, err);
            leveldb_free(err);
            return;
        }
        addReply(c, shared.cone);
        return;
    }

    wb = leveldb_writebatch_create();
    for (i = 1; i < c->argc; i++) {
        leveldb_writebatch_delete(wb, (char *) c->argv[i]->ptr, sdslen((sds) c->argv[i]->ptr));
    }
    leveldb_write(server.ds_db, server.woptions, wb, &err);
    leveldb_writebatch_clear(wb);
    leveldb_writebatch_destroy(wb);
    wb = NULL;

    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
        return;
    }

    addReplyLongLong(c, c->argc - 1);

    return;
}

void ds_close() {
    if(server.ds_open) {
        leveldb_readoptions_destroy(server.roptions);
        leveldb_writeoptions_destroy(server.woptions);
        leveldb_options_set_filter_policy(server.ds_options, NULL);
        leveldb_filterpolicy_destroy(server.policy);
        leveldb_close(server.ds_db);
        leveldb_options_destroy(server.ds_options);
        leveldb_cache_destroy(server.ds_cache);
    }
}
