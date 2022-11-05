// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server() :
        nacquire(0) {
    mtx = PTHREAD_MUTEX_INITIALIZER;
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r) {
    lock_protocol::status ret = lock_protocol::OK;
    printf("stat request from clt %d\n", clt);
    r = nacquire;
    return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r) {
    lock_protocol::status ret = lock_protocol::OK;
    // Your lab2B part2 code goes here
    pthread_mutex_lock(&mtx);
    if (locks.find(lid) == locks.end()) {
        // add a new lock_id
        cvs[lid] = PTHREAD_COND_INITIALIZER;
    } else if (locks[lid]) {
        while (locks[lid])
            pthread_cond_wait(&cvs[lid], &mtx);
    }
    locks[lid] = true;
    pthread_mutex_unlock(&mtx);
    return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r) {
    lock_protocol::status ret = lock_protocol::OK;
    // Your lab2B part2 code goes here
    pthread_mutex_lock(&mtx);
    locks[lid] = false;
    // await a thread which is waiting for the lock
    pthread_cond_signal(&cvs[lid]);
    pthread_mutex_unlock(&mtx);
    return ret;
}