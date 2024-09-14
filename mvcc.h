#ifndef __MVCC_H__
#define __MVCC_H__
struct val;
struct key;
struct txn;
struct cur;
struct snap;
typedef TAILQ_HEAD(key_list, key) key_list_t;
typedef TAILQ_HEAD(val_list, val) val_list_t;
typedef TAILQ_HEAD(cur_list, cur) cur_list_t;
typedef TAILQ_HEAD(txn_list, txn) txn_list_t;

extern struct snap *snapshot_create(void);
extern void snapshot_release(struct snap *snap);
extern struct snap *snap_parent(struct snap *snap);
extern struct snap *snap_current(void);
extern struct snap *txn_wr_snap(struct txn *txn);
extern void txns_purge(struct snap *snap, txn_list_t *txns);
extern void txns_splice(struct snap *snap, txn_list_t *to, txn_list_t *from);
extern void vals_purge(val_list_t *vals, struct snap *snap);
extern bool txn_is_committed(struct txn *txn);
extern u64 txn_wr_xid(struct txn *txn);
extern u64 txn_rd_xid(struct txn *txn);
extern u64 snap_id(struct snap *snap);
extern bool vals_conflict(struct txn *txn, val_list_t *vals);
extern void vals_free(val_list_t *vals);
extern void vals_commit(val_list_t *vals);
extern void txn_add_tail(txn_list_t *txns, struct txn *txn);
extern void snap_add_txn(struct txn *txn, struct snap **wr_snap,
    struct snap **rd_snap);
extern int val_insert(struct txn *txn, val_list_t *txn_vals, u8 *key_in,
    u16 key_len, u8 *val_in, u16 val_len);
extern int val_delete(struct txn *txn, val_list_t *txn_vals, u8 *key_in,
    u16 key_len);
extern int val_lookup(struct txn *txn, val_list_t *txn_vals, u8 *key_in,
    u16 key_len, u8 *val_in, u16 val_buf_len, u16 *out_val_len);
#endif
