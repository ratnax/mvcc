#ifndef __MVCC_API_H__
#define __MVCC_API_H__

extern struct txn *txn_alloc(void);
extern int txn_commit(struct txn *txn);
extern int txn_insert(struct txn *txn, u8 *key, u16 klen, void *val, u16 vlen);
extern int txn_delete(struct txn *txn, u8 *key, u16 klen);
extern int txn_lookup(struct txn *txn, u8 *key, u16 klen, u8 *val,
    u16 val_buf_len, u16 *out_val_len);
extern int __snap_init(void);
#endif
