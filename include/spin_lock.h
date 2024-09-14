#ifndef __SPIN_LOCK_H__
#define __SPIN_LOCK_H__
#include <pthread.h>
typedef pthread_mutex_t spinlock_t;

static void spin_lock(spinlock_t *lock) {
    pthread_mutex_lock(lock);
}
static void spin_unlock(spinlock_t *lock) {
    pthread_mutex_unlock(lock);
}
static void spin_lock_init(spinlock_t *lock) {
    pthread_mutex_init(lock, NULL);
}
#define SPIN_LOCK_INITIALIZER PTHREAD_MUTEX_INITIALIZER
#endif
