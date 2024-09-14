#include "mvcc.h"
#include "spin_lock.h"

#define MIN(a, b) ((a) < (b) ? (a) : (b))
struct key {
    TAILQ_ENTRY(key) list;
    val_list_t committed_vals;
    val_list_t active_vals;
    cur_list_t curs;
    u64 xid;
    u16 len;
    u8 data[0];
};

struct val {
    struct txn *txn;
    struct key *key;
    TAILQ_ENTRY(val) txn_list;
    TAILQ_ENTRY(val) key_list;
    u16 len;
    u8 data[0];
};

static spinlock_t kv_lock = SPIN_LOCK_INITIALIZER;

/* This is called serially on snap by snap, and txn by txn, as child snapshot
 * takes ref on parent snapshot. No protection need to access txn_wr_snap() as
 * only modifier of txn->wr_snap is the caller itself. */
void vals_purge(val_list_t *vals, struct snap *snap) {
    struct val *val, *tmp;
    struct val *next_val;

    TAILQ_FOREACH_SAFE(val, vals, txn_list, tmp) {
        struct key *key = val->key;
        spin_lock(&kv_lock);
        next_val = TAILQ_NEXT(val, key_list);
        /* snap will never be cur_wr_snap, so no need to worry about
         * txn->wr_snap being set by a parallel txn commit.*/
        if (next_val && txn_wr_snap(next_val->txn) == snap) {
            // assert(snap_parent(snap));
            TAILQ_REMOVE(vals, val, txn_list);
            TAILQ_REMOVE(&key->committed_vals, val, key_list);
            val->txn = NULL;  // TODO: remove this line
            spin_unlock(&kv_lock);
            free(val);
        } else {
            spin_unlock(&kv_lock);
        }
    }
}

static int __cmp(void *key1, u16 klen1, void *key2, u16 klen2) {
    u16 len = klen1 < klen2 ? klen1 : klen2;
    int ret = memcmp(key1, key2, len);

    if (ret == 0) {
        if (klen1 < klen2)
            return -1;
        if (klen1 > klen2)
            return 1;
    }
    return ret;
}

static bool __txn_conflict(struct txn *txn, struct key *key) {
    bool conflict = false;
    struct val *val;

    spin_lock(&kv_lock);
    TAILQ_FOREACH_REVERSE(val, &key->committed_vals, val_list, key_list) {
        assert(txn_is_committed(val->txn));
        if (txn_wr_xid(val->txn) > txn_rd_xid(txn))
            conflict = true;
        break;
    }
    spin_unlock(&kv_lock);
    return conflict;
}

bool vals_conflict(struct txn *txn, val_list_t *vals) {
    struct val *val;

    TAILQ_FOREACH(val, vals, txn_list) {
        if (__txn_conflict(txn, val->key)) {
            return true;
        }
    }
    return false;
}

void vals_free(val_list_t *vals) {
    struct val *val, *tmp;
    struct key *key;

    TAILQ_FOREACH_SAFE(val, vals, txn_list, tmp) {
        TAILQ_REMOVE(vals, val, txn_list);
        key = val->key;
        spin_lock(&kv_lock);
        TAILQ_REMOVE(&key->active_vals, val, key_list);
        spin_unlock(&kv_lock);
        val->txn = NULL;  // TODO: remove this line
        free(val);
    }
}

void vals_commit(val_list_t *vals) {
    struct val *val;

    spin_lock(&kv_lock);
    TAILQ_FOREACH(val, vals, txn_list) {
        struct key *key = val->key;
        TAILQ_REMOVE(&key->active_vals, val, key_list);
        TAILQ_INSERT_TAIL(&key->committed_vals, val, key_list);
    }
    spin_unlock(&kv_lock);
}

static key_list_t active_keys = TAILQ_HEAD_INITIALIZER(active_keys);

int val_insert(struct txn *txn, val_list_t *txn_vals, u8 *key_in, u16 key_len,
    u8 *val_in, u16 val_len) {
    struct key *key, *new_key;
    struct val *val;

    new_key = calloc(1, sizeof(struct key) + key_len);
    if (!new_key)
        return -ENOMEM;

    new_key->len = key_len;
    memcpy(new_key->data, key_in, key_len);
    TAILQ_INIT(&new_key->committed_vals);
    TAILQ_INIT(&new_key->active_vals);
    TAILQ_INIT(&new_key->curs);

    val = calloc(1, sizeof(struct val) + val_len);
    if (!val) {
        free(new_key);
        return -ENOMEM;
    }
    val->len = val_len;
    memcpy(val->data, val_in, val_len);
    val->key = new_key;
    val->txn = txn;

    spin_lock(&kv_lock);
    TAILQ_FOREACH(key, &active_keys, list) {
        int cmp = __cmp(key->data, key->len, key_in, key_len);
        if (cmp == 0) {
            val->key = key;
            TAILQ_INSERT_HEAD(&key->active_vals, val, key_list);
            TAILQ_INSERT_TAIL(txn_vals, val, txn_list);
            spin_unlock(&kv_lock);
            free(new_key);
            return 0;
        }
        if (cmp > 0) {
            TAILQ_INSERT_BEFORE(key, new_key, list);
            TAILQ_INSERT_HEAD(&new_key->active_vals, val, key_list);
            TAILQ_INSERT_TAIL(txn_vals, val, txn_list);
            spin_unlock(&kv_lock);
            return 0;
        }
    }
    TAILQ_INSERT_TAIL(&active_keys, new_key, list);
    TAILQ_INSERT_HEAD(&new_key->active_vals, val, key_list);
    TAILQ_INSERT_TAIL(txn_vals, val, txn_list);
    spin_unlock(&kv_lock);
    return 0;
}

int val_delete(struct txn *txn, val_list_t *txn_vals, u8 *key_in, u16 key_len) {
    struct key *key, *new_key;
    struct val *val;

    new_key = calloc(1, sizeof(struct key) + key_len);
    if (!new_key)
        return -ENOMEM;
    new_key->len = key_len;
    memcpy(new_key->data, key_in, key_len);
    TAILQ_INIT(&new_key->committed_vals);
    TAILQ_INIT(&new_key->active_vals);
    TAILQ_INIT(&new_key->curs);

    val = calloc(1, sizeof(struct val));
    if (!val) {
        free(new_key);
        return -ENOMEM;
    }
    val->len = 0;
    val->txn = txn;

    spin_lock(&kv_lock);
    TAILQ_FOREACH(key, &active_keys, list) {
        int cmp = __cmp(key->data, key->len, key_in, key_len);
        if (cmp == 0) {
            val->key = key;
            TAILQ_INSERT_HEAD(&key->active_vals, val, key_list);
            TAILQ_INSERT_TAIL(txn_vals, val, txn_list);
            spin_unlock(&kv_lock);
            return 0;
        }
        if (cmp > 0) {
            break;
        }
    }
    val->key = new_key;
    if (key) {
        TAILQ_INSERT_BEFORE(key, new_key, list);
    } else {
        TAILQ_INSERT_TAIL(&active_keys, new_key, list);
    }
    TAILQ_INSERT_HEAD(&new_key->active_vals, val, key_list);
    TAILQ_INSERT_TAIL(txn_vals, val, txn_list);
    spin_unlock(&kv_lock);
    return -ENOENT;
}

int val_lookup(struct txn *txn, val_list_t *txn_vals, u8 *key_in, u16 key_len,
    u8 *val_in, u16 val_buf_len, u16 *out_val_len) {
    struct key *key;
    struct val *val;

    TAILQ_FOREACH(val, txn_vals, txn_list) {
        key = val->key;
        int cmp = __cmp(key->data, key->len, key_in, key_len);
        if (cmp == 0) {
            if (val->len == 0) {
                return -ENOENT;
            }
            memcpy(val_in, val->data, MIN(val->len, val_buf_len));
            *out_val_len = MIN(val->len, val_buf_len);
            return 0;
        }
    }
    spin_lock(&kv_lock);
    TAILQ_FOREACH(key, &active_keys, list) {
        int cmp = __cmp(key->data, key->len, key_in, key_len);
        if (cmp == 0) {
            TAILQ_FOREACH_REVERSE(val, &key->committed_vals, val_list,
                key_list) {
                if (txn_is_committed(val->txn) == false) {
                    /* It may be in the process of committing. @txn->rd_snap
                     * will surely not point to the snap being committed to, so
                     * just continue on previous version of the val. */
                    continue;
                }
                if (txn_wr_xid(val->txn) <= txn_rd_xid(txn)) {
                    if (val->len == 0) {
                        spin_unlock(&kv_lock);
                        return -ENOENT;
                    }
                    memcpy(val_in, val->data, MIN(val->len, val_buf_len));
                    *out_val_len = MIN(val->len, val_buf_len);
                    spin_unlock(&kv_lock);
                    return 0;
                }
            }
        }
        if (cmp > 0) {
            break;
        }
    }
    assert(0);
    spin_unlock(&kv_lock);
    return -ENOENT;
}
