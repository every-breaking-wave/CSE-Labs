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

    void flush();
    void update(int new_term, int vote_for_);

private:
    std::mutex mtx;
    std::string log_filename;
    // Lab3: Your code here

};

template <typename command>
raft_storage<command>::raft_storage(const std::string &dir) {
    // Lab3: Your code here
//    std::stringstream number_filename;
//    std::stringstream log_filename;
//    std::stringstream snapshot_filename;
//    number_filename = dir << "/number.rft";
    log_filename = dir + "/log.bin";
//    snapshot_filename = dir + "/snapshot.rft";

    vote_for = -1;
    current_term = 0;
    logs.clear();
    int log_size;
    int cmd_size;
    std::ifstream in(log_filename.c_str(), std::ios::in | std::ios::binary);
    if (in.is_open()) {
        in.read((char *) &current_term, sizeof(int));
        in.read((char *) &vote_for, sizeof(int));
        in.read((char *) &log_size, sizeof(int));
        for (int i = 0; i < log_size; ++i) {
            log_entry<command> new_entry;
            in.read((char *) &new_entry.term, sizeof(int));
            in.read((char *) &cmd_size, sizeof(int));
            char *buf = new char [cmd_size];
            in.read(buf, cmd_size);
            new_entry.cmd.deserialize(buf, cmd_size);
            logs.template emplace_back(new_entry);
            delete []buf;
        }
    }
    in.close();
//    printf("read storage from %s, current log size %d", log_filename.c_str(), logs.size());
}

template <typename command>
raft_storage<command>::~raft_storage() {
    // Lab3: Your code here
    flush();
}
template<typename command>
void raft_storage<command>::update(int new_term, int vote_for_) {
    current_term = new_term;
    vote_for = vote_for_;
    flush();
}

// persist logs
template<typename command>
void raft_storage<command>::flush() {
    mtx.lock();
    std::ofstream out(log_filename.c_str(), std::ios::trunc | std::ios::out | std::ios::binary);
    int log_size = logs.size();
    if (out.is_open()) {
        out.write((char *) &current_term, sizeof(int));
        out.write((char *) &vote_for, sizeof(int));
        out.write((char *) &log_size, sizeof(int));
        for (auto &it : logs) {
            int size = it.cmd.size();
            char *buf = new char [size];
            it.cmd.serialize(buf, size);
            out.write((char *) &it.term, sizeof(int));
            out.write((char *) &size, sizeof(int));
            out.write(buf, size);
            delete []buf;

        }
    }
    out.close();
    mtx.unlock();
}
#endif // raft_storage_h