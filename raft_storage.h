#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <fstream>

template <typename command>
class raft_storage {
public:
    raft_storage(const std::string &file_dir);
    ~raft_storage();
    // Lab3: Your code here
    int current_term;
    int vote_for;
    std::vector<log_entry<command>> logs;

private:
    std::mutex mtx;
    // Lab3: Your code here

};

template <typename command>
raft_storage<command>::raft_storage(const std::string &dir) {
    // Lab3: Your code here
//    std::ifstream in(file_path, std::ios::in | std::ios::binary);
//    if(in.is_open()){
//
//    }
}

template <typename command>
raft_storage<command>::~raft_storage() {
    // Lab3: Your code here
}

#endif // raft_storage_h