#include "raft_protocol.h"

marshall& operator<<(marshall &m, const request_vote_args& args) {
    m << args.term << args.candidateId << args.lastLogIndex << args.lastLogTerm;
    return m;

}
unmarshall& operator>>(unmarshall &u, request_vote_args& args) {
    u >> args.term >> args.candidateId >> args.lastLogIndex >> args.lastLogTerm;
    return u;
}

marshall& operator<<(marshall &m, const request_vote_reply& reply) {
    m << reply.term << reply.voteGranted;
    return m;
}

unmarshall& operator>>(unmarshall &u, request_vote_reply& reply) {
    u >> reply.term >> reply.voteGranted;
    return u;
}

marshall& operator<<(marshall &m, const append_entries_reply& reply) {
    m << reply.term << reply.success;
    return m;
}
unmarshall& operator>>(unmarshall &u, append_entries_reply& reply) {
    u >> reply.term >> reply.success;
    return u;
}

marshall& operator<<(marshall &m, const install_snapshot_args& args) {
    m << args.term << args.leaderId << args.lastIncludedIndex << args.lastIncludedTerm << args.snapshot;
    return m;
}

unmarshall& operator>>(unmarshall &u, install_snapshot_args& args) {
    u >> args.term >> args.leaderId >> args.lastIncludedIndex >> args.lastIncludedTerm >> args.snapshot;
    return u;
}

marshall& operator<<(marshall &m, const install_snapshot_reply& reply) {
    m << reply.term;
    return m;
}

unmarshall& operator>>(unmarshall &u, install_snapshot_reply& reply) {
    u >> reply.term;
    return u;
}