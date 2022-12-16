#include "extent_server_dist.h"

chfs_raft *extent_server_dist::leader() const {
    int leader = this->raft_group->check_exact_one_leader();
    if (leader < 0) {
        return this->raft_group->nodes[0];
    } else {
        return this->raft_group->nodes[leader];
    }
}

int extent_server_dist::create(uint32_t type, extent_protocol::extentid_t &id) {
    // Lab3: your code here
    chfs_command_raft cmd;
    cmd.cmd_tp = chfs_command_raft::CMD_CRT;
    cmd.type = type;
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    int term, index;
    auto now = std::chrono::system_clock::now();
    leader()->new_command(cmd, term, index);
    if(!cmd.res->done){
        std::chrono::milliseconds m1(2000);
        ASSERT(cmd.res->cv.wait_until(lock, now + m1) == std::cv_status::no_timeout, "extent_server_dist::create command timeout");
    }
    id = cmd.res->id;
    printf("extent_server_dist: create file ok, id is %d\n", id);
    return extent_protocol::OK;
}

int extent_server_dist::put(extent_protocol::extentid_t id, std::string buf, int &) {
    // Lab3: your code here
    chfs_command_raft cmd;
    cmd.cmd_tp = chfs_command_raft::CMD_PUT;
    cmd.buf =  buf;
    cmd.id = id;
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    int term, index;
    auto now = std::chrono::system_clock::now();
    leader()->new_command(cmd, term, index);
    if(!cmd.res->done){
        std::chrono::milliseconds m1(2000);
        ASSERT(cmd.res->cv.wait_until(lock, now + m1) == std::cv_status::no_timeout, "extent_server_dist::create command timeout");
    }
    printf("extent_server_dist: put file ok\n");
    return extent_protocol::OK;
}

int extent_server_dist::get(extent_protocol::extentid_t id, std::string &buf) {
    // Lab3: your code here
    chfs_command_raft cmd;
    cmd.cmd_tp = chfs_command_raft::CMD_GET;
    cmd.id = id;
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    int term, index;
    auto now = std::chrono::system_clock::now();
    leader()->new_command(cmd, term, index);
    if(!cmd.res->done){
        std::chrono::milliseconds m1(2000);
        ASSERT(cmd.res->cv.wait_until(lock, now + m1) == std::cv_status::no_timeout, "extent_server_dist::create command timeout");
    }
    buf = cmd.res->buf;
    return extent_protocol::OK;
}

int extent_server_dist::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a) {
    // Lab3: your code here
    chfs_command_raft cmd;
    cmd.cmd_tp = chfs_command_raft::CMD_GETA;
    cmd.id = id;
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    int term, index;
    auto now = std::chrono::system_clock::now();
    leader()->new_command(cmd, term, index);
    if(!cmd.res->done){
        std::chrono::milliseconds m1(2000);
        ASSERT(cmd.res->cv.wait_until(lock, now + m1) == std::cv_status::no_timeout, "extent_server_dist::create command timeout");
    }
    a = cmd.res->attr;
    return extent_protocol::OK;
}

int extent_server_dist::remove(extent_protocol::extentid_t id, int &) {
    // Lab3: your code here
    chfs_command_raft cmd;
    cmd.cmd_tp = chfs_command_raft::CMD_RMV;
    cmd.id = id;
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    int term, index;
    auto now = std::chrono::system_clock::now();
    leader()->new_command(cmd, term, index);
    if(!cmd.res->done){
        std::chrono::milliseconds m1(2000);
        ASSERT(cmd.res->cv.wait_until(lock, now + m1) == std::cv_status::no_timeout, "extent_server_dist::create command timeout");
    }
    return extent_protocol::OK;
}

extent_server_dist::~extent_server_dist() {
    delete this->raft_group;
}