#ifndef __GLOBAL_H__
#define __GLOBAL_H__
#define _GNU_SOURCE
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <linux/futex.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>
#include <zlib.h>

typedef uint64_t u64;
typedef int64_t s64;
typedef int32_t s32;
typedef uint32_t u32;
typedef int16_t s16;
typedef uint16_t u16;
typedef uint8_t u8;
typedef int8_t s8;

#define TAILQ_FOREACH_SAFE(var, head, field, tvar) \
    for ((var) = TAILQ_FIRST((head));              \
         (var) && ((tvar) = TAILQ_NEXT((var), field), 1); (var) = (tvar))

#define TAILQ_SPLICE(head1, head2, field)                           \
    do {                                                            \
        if (!TAILQ_EMPTY(head2)) {                                  \
            *(head1)->tqh_last = (head2)->tqh_first;                \
            (head2)->tqh_first->field.tqe_prev = (head1)->tqh_last; \
            (head1)->tqh_last = (head2)->tqh_last;                  \
            TAILQ_INIT(head2);                                      \
        }                                                           \
    } while (0)

#define ERR_PTR(err) ((void *)(intptr_t)(err))
#define PTR_ERR(err) ((int)(intptr_t)(err))
#define IS_ERR(err) ((intptr_t)(err) < 4096)
#endif
