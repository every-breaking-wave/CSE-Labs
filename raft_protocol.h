#ifndef raft_protocol_h
#define raft_protocol_h

#include "rpc.h"
#include "raft_state_machine.h"

enum raft_rpc_opcodes {
    op_request_vote = 0x1212,
    op_append_entries = 0x3434,
    op_install_snapshot = 0x5656
};

enum raft_rpc_status {
    OK,
    RETRY,
    RPCERR,
    NOENT,
    IOERR
};

class request_vote_args {
public:
    // Lab3: Your code here
    int term;
    int candidate_id;
    int last_log_index;
    int last_log_term;

    request_vote_args() = default;

    request_vote_args(int _term, int _candidate_id, int _last_log_index, int _last_log_term) :
            term(_term), candidate_id(_candidate_id), last_log_index(_last_log_index), last_log_term(_last_log_term) {}
};

marshall &operator<<(marshall &m, const request_vote_args &args);

unmarshall &operator>>(unmarshall &u, request_vote_args &args);

class request_vote_reply {
public:
    // Lab3: Your code here
    int term;
    bool vote_granted;
};

marshall &operator<<(marshall &m, const request_vote_reply &reply);

unmarshall &operator>>(unmarshall &u, request_vote_reply &reply);

template<typename command>
class log_entry {
public:
    // Lab3: Your code here
    command cmd;
    int term;

    log_entry() = default;

    log_entry(command cmd, int term) : term(term), cmd(cmd) {}
};

template<typename command>
marshall &operator<<(marshall &m, const log_entry<command> &entry) {
    // Lab3: Your code here
    m << entry.cmd << entry.term;
    return m;
}

template<typename command>
unmarshall &operator>>(unmarshall &u, log_entry<command> &entry) {
    // Lab3: Your code here
    u >> entry.cmd >> entry.term;
    return u;
}

template<typename command>
class append_entries_args {
public:
    // Your code here
    int term;
    int leader_id;
    int prev_log_index;
    int prev_log_term;
    int leader_commit;
    std::vector <log_entry<command>> entries;

    append_entries_args() = default;

    append_entries_args(int term_, int leader_id_, int prev_log_index_, int prev_log_term_, int leader_commit_, std::vector <log_entry<command>> entries_)
            : term(term_), leader_id(leader_id_), prev_log_term(prev_log_term_),
              prev_log_index(prev_log_index_), leader_commit(leader_commit_), entries(entries_) {}

};

template<typename command>
marshall &operator<<(marshall &m, const append_entries_args<command> &args) {
    // Lab3: Your code here
    m << args.term << args.leader_id << args.prev_log_index << args.prev_log_term << args.leader_commit << args.entries;
    return m;
}

template<typename command>
unmarshall &operator>>(unmarshall &u, append_entries_args<command> &args) {
    // Lab3: Your code here
    u >> args.term >> args.leader_id >> args.prev_log_index >> args.prev_log_term >> args.leader_commit >> args.entries;
    return u;
}

class append_entries_reply {
public:
    // Lab3: Your code here
    int term;
    bool success;
};

marshall &operator<<(marshall &m, const append_entries_reply &reply);

unmarshall &operator>>(unmarshall &m, append_entries_reply &reply);

class install_snapshot_args {
public:
    // Lab3: Your code here
};

marshall &operator<<(marshall &m, const install_snapshot_args &args);

unmarshall &operator>>(unmarshall &m, install_snapshot_args &args);

class install_snapshot_reply {
public:
    // Lab3: Your code here
};

marshall &operator<<(marshall &m, const install_snapshot_reply &reply);

unmarshall &operator>>(unmarshall &m, install_snapshot_reply &reply);

#endif // raft_protocol_h