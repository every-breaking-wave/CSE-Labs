#ifndef raft_protocol_h
#define raft_protocol_h

#include "rpc.h"
#include "raft_state_machine.h"

enum raft_rpc_opcodes
{
    op_request_vote = 0x1212,
    op_append_entries = 0x3434,
    op_install_snapshot = 0x5656
};

enum raft_rpc_status
{
    OK,
    RETRY,
    RPCERR,
    NOENT,
    IOERR
};

class request_vote_args
{
public:
    int term;
    int candidateId;
    int lastLogIndex;
    int lastLogTerm;

    request_vote_args()=default;
    request_vote_args(int t, int c, int l1, int l2) : term(t), candidateId(c), lastLogIndex(l1), lastLogTerm(l2){};
};

marshall &operator<<(marshall &m, const request_vote_args &args);
unmarshall &operator>>(unmarshall &u, request_vote_args &args);

class request_vote_reply
{
public:
    int term;
    bool voteGranted;

    request_vote_reply() : term(0), voteGranted(false){};
    request_vote_reply(int t, bool v) : term(t), voteGranted(v){};
};

marshall &operator<<(marshall &m, const request_vote_reply &reply);
unmarshall &operator>>(unmarshall &u, request_vote_reply &reply);

template <typename command>
class log_entry
{
public:
    int index;
    int term;
    command cmd;

    log_entry() : index(0), term(0) {}
    log_entry(int index, int term) : index(index), term(term) {}
    log_entry(int index, int term, command cmd) : index(index), term(term), cmd(cmd) {}
};

template <typename command>
marshall &operator<<(marshall &m, const log_entry<command> &entry)
{
    m << entry.index << entry.term << entry.cmd;
    return m;
}

template <typename command>
unmarshall &operator>>(unmarshall &u, log_entry<command> &entry)
{
    u >> entry.index >> entry.term >> entry.cmd;
    return u;
}

template <typename command>
class append_entries_args
{
public:
    int term;
    int leaderId;
    int prevLogIndex;
    int prevLogTerm;
    std::vector<log_entry<command>> entries;
    int leaderCommit;
    append_entries_args()=default;
    append_entries_args(int term_, int leaderId_, int leaderCommit_, int preLogIndex_, int prevLogTerm_, std::vector<log_entry<command>> entries_)
            : term(term_), leaderId(leaderId_), prevLogIndex(preLogIndex_), leaderCommit(leaderCommit_), prevLogTerm(prevLogTerm_), entries(entries_){}
};

template <typename command>
marshall &operator<<(marshall &m, const append_entries_args<command> &args)
{
    m << args.term << args.leaderId << args.prevLogIndex << args.prevLogTerm << args.entries << args.leaderCommit;
    return m;
}

template <typename command>
unmarshall &operator>>(unmarshall &u, append_entries_args<command> &args)
{
    u >> args.term >> args.leaderId >> args.prevLogIndex >> args.prevLogTerm >> args.entries >> args.leaderCommit;
    return u;
}

class append_entries_reply
{
public:
    int term;
    bool success;
};

marshall &operator<<(marshall &m, const append_entries_reply &reply);
unmarshall &operator>>(unmarshall &u, append_entries_reply &reply);

class install_snapshot_args
{
public:
    int term;
    int leaderId;
    int lastIncludedIndex;
    int lastIncludedTerm;
    std::vector<char> snapshot;
    install_snapshot_args()=default;
    install_snapshot_args(int term_, int leaderId_, int lastIncludedIndex_, int lastIncludedTerm_, std::vector<char> snapshot_)
    : term(term_), leaderId(leaderId_), lastIncludedIndex(lastIncludedIndex_), lastIncludedTerm(lastIncludedTerm_), snapshot(snapshot_){}
};

marshall &operator<<(marshall &m, const install_snapshot_args &args);
unmarshall &operator>>(unmarshall &u, install_snapshot_args &args);

class install_snapshot_reply
{
public:
    int term;
};

marshall &operator<<(marshall &m, const install_snapshot_reply &reply);
unmarshall &operator>>(unmarshall &u, install_snapshot_reply &reply);

#endif // raft_protocol_h