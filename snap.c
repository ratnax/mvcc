#include "mvcc.h"
#include "spin_lock.h"

struct snap {
    u64 id;
    txn_list_t txns;
    struct snap *parent;
    int refcnt;
} base_snap = {0, TAILQ_HEAD_INITIALIZER(base_snap.txns), NULL, 1};

static struct snap *cur_rd_snap = NULL;
static struct snap *cur_wr_snap = &base_snap;
static spinlock_t snap_lock = SPIN_LOCK_INITIALIZER;
static u64 next_snap_id = 1;

struct snap *snapshot_create(void) {
    struct snap *new_snap = NULL;
    struct snap *snap = NULL;

retry:
    spin_lock(&snap_lock);
    if (cur_rd_snap == NULL) {
        if (!new_snap) {
            spin_unlock(&snap_lock);
            new_snap = calloc(1, sizeof(struct snap));
            if (!new_snap)
                return ERR_PTR(-ENOMEM);
            new_snap->refcnt = 1;
            TAILQ_INIT(&new_snap->txns);
            goto retry;
        }

        new_snap->id = next_snap_id++;
        cur_rd_snap = cur_wr_snap;
        cur_wr_snap = new_snap;
        cur_rd_snap->parent = cur_wr_snap;
        cur_wr_snap->refcnt++;
        new_snap = NULL;
    }
    cur_rd_snap->refcnt++;
    snap = cur_rd_snap;
    spin_unlock(&snap_lock);
    if (new_snap)
        free(new_snap);
    return snap;
}

void snapshot_release(struct snap *snap) {
    struct snap *parent = NULL;
    struct txn *txn, *tmp_txn;
    struct val *val, *tmp_val;

    spin_lock(&snap_lock);
    if (--snap->refcnt) {
        spin_unlock(&snap_lock);
        return;
    }
    spin_unlock(&snap_lock);

    parent = snap->parent;
    txns_purge(parent, &snap->txns);
    txns_splice(snap, &parent->txns, &snap->txns);
    free(snap);

    if (parent)
        snapshot_release(parent);
}

struct snap *snap_parent(struct snap *snap) {
    return snap->parent;
}
u64 snap_id(struct snap *snap) {
    return snap->id;
}

struct snap *snap_current(void) {
    return cur_wr_snap;
}

void snap_add_txn(struct txn *txn, struct snap **wr_snap,
    struct snap **rd_snap) {
    struct snap *snap = NULL;

    spin_lock(&snap_lock);
    *wr_snap = cur_wr_snap;
    *rd_snap = cur_rd_snap;
    cur_rd_snap = NULL;
    txn_add_tail(&cur_wr_snap->txns, txn);
    spin_unlock(&snap_lock);
}

void __snap_exit(void) {
}

int __snap_init(void) {
    cur_wr_snap = calloc(1, sizeof(struct snap));
    if (!cur_wr_snap)
        return -ENOMEM;
    cur_wr_snap->id = next_snap_id++;
    cur_wr_snap->refcnt = 1;
    cur_wr_snap->parent = NULL;
    TAILQ_INIT(&cur_wr_snap->txns);
    return 0;
}
