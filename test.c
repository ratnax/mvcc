#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "mvcc_api.h"

#define MONEY 10000
#define MAX_THREADS 10

// bear -- cc test.c kv.c snap.c txn.c  -Iinclude -include include/global.h -g
// -O0

static void *run(void *arg) {
    int id = (int)(unsigned long)arg;
    char key = 'A' + id;
    u64 val;
    u64 money = MONEY;
    u16 vlen;
    struct txn *txn;
    int ret;
    int i;
    int k = 1000;

    txn = txn_alloc();
    if (!txn)
        return NULL;
    ret = txn_insert(txn, (u8 *)&key, 1, (void *)&money, sizeof(u64));
    if (ret) {
        printf("insert failed %d\n", ret);
        return NULL;
    }
    ret = txn_commit(txn);
    if (ret) {
        printf("commit failed %d\n", ret);
        return NULL;
    }

    sleep(1);
    while (k--) {
        txn = txn_alloc();
        if (!txn) {
            printf("txn_alloc failed\n");
            break;
        }
        ret = txn_lookup(txn, (u8 *)&key, 1, (u8 *)&val, sizeof(val), &vlen);
        if (ret == 0) {
            money = val;
        } else {
            printf("%c: lookup failed %d\n", key, ret);
            break;
        }
        {
            u64 total = 0;
            for (i = 0; i < MAX_THREADS; i++) {
                u8 peer_key = 'A' + i;
                ret = txn_lookup(txn, (u8 *)&peer_key, 1, (u8 *)&val,
                    sizeof(val), &vlen);
                if (ret == 0) {
                    total += val;
                } else {
                    printf("%c: lookup failed %d\n", key, ret);
                    return NULL;
                }
            }
            assert(total == MONEY * MAX_THREADS);
        }
        u64 split = money / MAX_THREADS;
        u64 given = 0;

        if (split >= 2) {
            for (i = 0; i < MAX_THREADS; i++) {
                u8 peer_key = 'A' + i;
                u64 peer_money;
                u16 vlen;
                u64 give;

                if (i == id)
                    continue;
                if (split < 1) {
                    give = 1;
                } else {
                    give = split / 2 + random() % (split / 2);
                }
                ret = txn_lookup(txn, &peer_key, 1, (void *)&peer_money,
                    sizeof(peer_money), &vlen);
                if (ret) {
                    printf("%c: lookup failed %d\n", key, ret);
                    exit(0);
                } else {
                    peer_money += give;
                }
                ret = txn_insert(txn, &peer_key, 1, (void *)&peer_money,
                    sizeof(u64));
                if (ret) {
                    printf("%c: insert failed: %d\n", key, ret);
                    return NULL;
                }
                given += give;
            }
            money -= given;
            ret = txn_insert(txn, (void *)&key, 1, (void *)&money,
                (u16)sizeof(u64));
            if (ret) {
                printf("%c: insert failed :%d\n", key, ret);
                return NULL;
            }
        }
        ret = txn_commit(txn);
        if (ret) {
            // printf("%c: commit failed :%d\n", key, ret);
            money += given;
        }
        // usleep(1000);
    }
    return NULL;
}

int main() {
    pthread_t threads[MAX_THREADS];
    int i;
    int ret;

    ret = __snap_init();
    if (ret)
        return ret;

    for (i = 0; i < MAX_THREADS; i++)
        pthread_create(&threads[i], NULL, run, (void *)(unsigned long)i);
    for (i = 0; i < MAX_THREADS; i++)
        pthread_join(threads[i], NULL);
    return 0;
}
