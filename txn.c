#include "mvcc.h"
#include "spin_lock.h"

struct txn {
    u64 id;
    struct snap *rd_snap;
    struct snap *wr_snap;
    val_list_t vals;
    cur_list_t curs;
    TAILQ_ENTRY(txn) snap_list;
};

static struct {
    u64 xid; /* protected by txn_lock */
    spinlock_t lock;
} NEXT = {1, SPIN_LOCK_INITIALIZER};

struct txn *txn_alloc(void) {
    struct txn *txn = NULL;
    struct snap *snap = NULL;

    txn = calloc(1, sizeof(struct txn));
    if (!txn)
        return NULL;

    TAILQ_INIT(&txn->vals);
    TAILQ_INIT(&txn->curs);

    snap = snapshot_create();
    if (IS_ERR(snap)) {
        free(txn);
        return (struct txn *)snap;
    }
    txn->rd_snap = snap;
    spin_lock(&NEXT.lock);
    txn->id = NEXT.xid++;
    spin_unlock(&NEXT.lock);
    return txn;
}

/* This is called serially on snap by snap, as child takes ref on parent
 * snapshot */
void txns_purge(struct snap *parent, txn_list_t *txns) {
    struct txn *txn, *tmp;

    TAILQ_FOREACH_SAFE(txn, txns, snap_list, tmp) {
        vals_purge(&txn->vals, parent);
        if (TAILQ_EMPTY(&txn->vals)) {
            TAILQ_REMOVE(txns, txn, snap_list);
            free(txn);
        } else {
            txn->wr_snap = parent;
        }
    }
}

void txns_splice(struct snap *snap, txn_list_t *to, txn_list_t *from) {
    TAILQ_SPLICE(to, from, snap_list);
}

struct snap *txn_wr_snap(struct txn *txn) {
    return txn->wr_snap;
}

static spinlock_t txn_lock = SPIN_LOCK_INITIALIZER;

int txn_commit(struct txn *txn) {
    struct snap *rd_snap = NULL;

    if (!TAILQ_EMPTY(&txn->vals)) {
        spin_lock(&txn_lock);
        if (vals_conflict(txn, &txn->vals)) {
            spin_unlock(&txn_lock);
            vals_free(&txn->vals);
            snapshot_release(txn->rd_snap);
            free(txn);
            return -EAGAIN;
        }
        /* Order is important that we inform commit status to kv store first
         * then attach the txn to snap. If we first attach the txn to snap, a
         * new txn will become a reader to this snap, and tries to read a value
         * in this snap that is not marked committed in kv store. That results
         * in stale read. */
        vals_commit(&txn->vals);
        snap_add_txn(txn, &txn->wr_snap, &rd_snap);
        spin_unlock(&txn_lock);

        if (rd_snap)
            snapshot_release(rd_snap);
        snapshot_release(txn->rd_snap);
        txn->rd_snap = NULL;
    } else {
        snapshot_release(txn->rd_snap);
        free(txn);
    }
    return 0;
}

bool txn_is_committed(struct txn *txn) {
    return txn->wr_snap ? true : false;
}
u64 txn_wr_xid(struct txn *txn) {
    return snap_id(txn->wr_snap);
}
u64 txn_rd_xid(struct txn *txn) {
    return snap_id(txn->rd_snap);
}
void txn_add_tail(txn_list_t *txns, struct txn *txn) {
    TAILQ_INSERT_TAIL(txns, txn, snap_list);
    assert(TAILQ_EMPTY(&txn->vals) == 0);
}

int txn_insert(struct txn *txn, u8 *key_in, u16 key_len, u8 *val_in,
    u16 val_len) {
    return val_insert(txn, &txn->vals, key_in, key_len, val_in, val_len);
}
int txn_delete(struct txn *txn, u8 *key_in, u16 key_len) {
    return val_delete(txn, &txn->vals, key_in, key_len);
}
int txn_lookup(struct txn *txn, u8 *key_in, u16 key_len, u8 *val_in,
    u16 val_buf_len, u16 *out_val_len) {
    return val_lookup(txn, &txn->vals, key_in, key_len, val_in, val_buf_len,
        out_val_len);
}
