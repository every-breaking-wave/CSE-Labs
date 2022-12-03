#include "chfs_state_machine.h"
#include<bits/stdc++.h> 
chfs_command_raft::chfs_command_raft() {
    // Lab3: Your code here
    this->cmd_tp = CMD_NONE;
    this->type = 0;
    this->id = 0;
    this->buf = "";
    this->res = std::make_shared<result>();
}

chfs_command_raft::chfs_command_raft(const chfs_command_raft &cmd) :
    cmd_tp(cmd.cmd_tp), type(cmd.type),  id(cmd.id), buf(cmd.buf), res(cmd.res) {
    // Lab3: Your code here
}
chfs_command_raft::~chfs_command_raft() {
    // Lab3: Your code here
}

int chfs_command_raft::size() const{ 
    // Lab3: Your code here
    return sizeof(command_type) + sizeof(type) + sizeof(id) + sizeof(uint32_t) + buf.size();
}

void chfs_command_raft::serialize(char *buf_out, int size) const {
    // Lab3: Your code here
    assert(size == this->size());
    int pos = 0;
    int buf_size = buf.size();
    memcpy(buf_out + pos, (char *) &cmd_tp, sizeof(command_type)); // serialize command_type
    pos += sizeof(command_type);
    memcpy(buf_out + pos, (char *) &type, sizeof(type));  // serialize type
    pos += sizeof(uint32_t);   
    memcpy(buf_out + pos, (char *) &id, sizeof(id));  // serialize id
    pos += sizeof(id);
    memcpy(buf_out + pos, (char *) &(buf_size), sizeof(uint32_t)); // serialize buf size
    pos += sizeof(uint32_t);
    memcpy(buf_out + pos, buf.c_str(), buf.size());  // serialize buf
    return;
}

void chfs_command_raft::deserialize(const char *buf_in, int size) {
    // Lab3: Your code here
    int buf_size = 0;
    int pos = 0;

    memcpy((char *) &cmd_tp, buf_in + pos, sizeof(command_type));// deserialize command_type
    pos += sizeof(command_type);
    memcpy((char *) &type, buf_in + pos, sizeof(type));  // deserialize type
    pos += sizeof(type);
    memcpy((char *) &id, buf_in + pos, sizeof(id));  // deserialize id
    pos += sizeof(id);
    memcpy((char *) &buf_size, buf_in + pos, sizeof(uint32_t));  // deserialize buf size
    pos += sizeof (uint32_t);
    buf = std::string(buf_in + pos, buf_size);  // deserialize buf
    return;
}

marshall &operator<<(marshall &m, const chfs_command_raft &cmd) {
    // Lab3: Your code here
    m << (int)cmd.cmd_tp << cmd.type << cmd.id << cmd.buf;
    return m;
}

unmarshall &operator>>(unmarshall &u, chfs_command_raft &cmd) {
    // Lab3: Your code here
    int cmd_tp;
    u >> cmd_tp >> cmd.type >> cmd.id >> cmd.buf;
    cmd.cmd_tp = chfs_command_raft::command_type(cmd_tp);
    return u;
}

void chfs_state_machine::apply_log(raft_command &cmd) {
    chfs_command_raft &chfs_cmd = static_cast<chfs_command_raft &>(cmd);
    // Lab3: Your code here
    // add lock
    std::unique_lock<std::mutex> lock(chfs_cmd.res->mtx);
    mtx.lock();
    chfs_cmd.res->start = std::chrono::system_clock::now();
    switch (chfs_cmd.cmd_tp) {
        case chfs_command_raft::CMD_NONE:{
            chfs_cmd.res->tp = chfs_cmd.cmd_tp;
            chfs_cmd.res->done = true;
            break;
        }
        case chfs_command_raft::CMD_GET:{
            printf("\nchfs_state_machine: get file , id : %d\n", chfs_cmd.id);
            es.get(chfs_cmd.id, chfs_cmd.res->buf);
            chfs_cmd.res->id  = chfs_cmd.id;
            chfs_cmd.res->done = chfs_cmd.res->buf.size() > 0;
            chfs_cmd.res->tp = chfs_cmd.cmd_tp;
            break;
        }

        case chfs_command_raft::CMD_GETA:{
            printf("\nchfs_state_machine: get file attr, id : %d\n", chfs_cmd.id);
            es.getattr(chfs_cmd.id, chfs_cmd.res->attr);
            chfs_cmd.res->id  = chfs_cmd.id;
            chfs_cmd.res->tp = chfs_cmd.cmd_tp;
            chfs_cmd.res->done = chfs_cmd.res->attr.type > 0;
            break;
        }

        case chfs_command_raft::CMD_PUT:{
            printf("\nchfs_state_machine: create file , type : %d\n", chfs_cmd.type);
            int status =  0;
            es.put(chfs_cmd.id, chfs_cmd.buf, status);
            chfs_cmd.res->id  = chfs_cmd.id;
            chfs_cmd.res->buf = chfs_cmd.buf;
            chfs_cmd.res->tp = chfs_cmd.cmd_tp;
            chfs_cmd.res->done = chfs_cmd.res->buf.size() > 0;
            break;
        }

        case chfs_command_raft::CMD_CRT:{
            printf("\nchfs_state_machine: create file , type : %d\n", chfs_cmd.type);
            es.create(chfs_cmd.type, chfs_cmd.id);
            chfs_cmd.res->id  = chfs_cmd.id;
            chfs_cmd.res->done = chfs_cmd.res->id > 0;
            chfs_cmd.res->tp = chfs_cmd.cmd_tp;
            break;
        }

        case chfs_command_raft::CMD_RMV:{
            printf("\nchfs_state_machine: create file , type : %d\n", chfs_cmd.type);
            int status = 0;
            es.remove(chfs_cmd.id, status);
            chfs_cmd.res->id  = chfs_cmd.id;
            chfs_cmd.res->done = status > 0;
            chfs_cmd.res->tp = chfs_cmd.cmd_tp;
        }
    }

    chfs_cmd.res->cv.notify_all();
    mtx.unlock();
    return;
}


