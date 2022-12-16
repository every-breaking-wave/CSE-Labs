#ifndef raft_h
#define raft_h

#include <algorithm>
#include <atomic>
#include <chrono>
#include <ctime>
#include <mutex>
#include <random>
#include <stdarg.h>
#include <thread>

#include "raft_protocol.h"
#include "raft_state_machine.h"
#include "raft_storage.h"
#include "rpc.h"

using std::chrono::system_clock;

template <typename state_machine, typename command> class raft {

    static_assert(std::is_base_of<raft_state_machine, state_machine>(),
    "state_machine must inherit from raft_state_machine");
    static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");

    friend class thread_pool;

#define RAFT_LOG(fmt, args...)                                                                                         \
    do {                                                                                                               \
        auto now =                                                                                                     \
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()) \
                .count();                                                                                              \
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args);       \
    } while (0);

public:
    raft(rpcs *rpc_server, std::vector<rpcc *> rpc_clients, int idx, raft_storage<command> *storage,
         state_machine *state);
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node.
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped().
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false.
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx; // A big lock to protect the whole data structure
    ThrPool *thread_pool;
    raft_storage<command> *storage; // To persist the raft log
    state_machine *state;           // The state machine that applies the raft log, e.g. a kv store

    rpcs *rpc_server;                // RPC server to recieve and handle the RPC requests
    std::vector<rpcc *> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                       // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role { follower, candidate, leader };
    raft_role role;   // volatile
    int current_term; // persist

    std::thread *background_election;
    std::thread *background_ping;
    std::thread *background_commit;
    std::thread *background_apply;

    // persist states
    int vote_for;
    std::vector<log_entry<command>> log;
    std::vector<char> snapshot;

    // volatile states
    int commitIndex;
    int lastApplied;

    // candidate volatile states
    int vote_count;
    std::vector<bool> votedNodes;

    // leader volatile states
    std::vector<int> nextIndex;
    std::vector<int> matchIndex;
    std::vector<int> matchCount;

    // times
    system_clock::time_point last_time;
    system_clock::duration follower_timeout;
    system_clock::duration candidate_timeout;

private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply &reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply &reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply &reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command> &arg,
                                     const append_entries_reply &reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args &arg,
                                       const install_snapshot_reply &reply);

private:
    bool is_stopped();
    int num_nodes() { return rpc_clients.size(); }

    // background workers
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:

    void init_time();

    inline int get_term(int index);
    std::vector<log_entry<command>> get_entries(int begin_index, int end_index);


    bool judge_append(int pre_log_index, int pre_log_term);
    bool judge_snapshot(int last_included_index, int last_included_term);
    bool judge_vote(int last_log_index, int last_log_term);
    void set_role_and_term(raft_role role, int term);
    void init_state();

};

template <typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs *server, std::vector<rpcc *> clients, int idx, raft_storage<command> *storage,
                                   state_machine *state)
        : storage(storage), state(state), rpc_server(server), rpc_clients(clients), my_id(idx), stopped(false),
          role(follower), current_term(0), background_election(nullptr), background_ping(nullptr),
          background_commit(nullptr), background_apply(nullptr) {
    thread_pool = new ThrPool(32);

    // recover from storage
    if (!storage->restore(current_term, vote_for, log, snapshot)) {
        init_state();
        snapshot.clear();
        storage->updateTotal(current_term, vote_for, log, snapshot);
    }
    if (!snapshot.empty()) {
        state->apply_snapshot(snapshot);
    }

    // volatile states
    commitIndex = log.front().index;
    lastApplied = log.front().index;

    // candidate volatile states
    vote_count = 0;
    votedNodes.assign(num_nodes(), false);

    // leader volatile states
    nextIndex.assign(num_nodes(), 1);
    matchIndex.assign(num_nodes(), 0);
    matchCount.clear();

    // initialize times
    last_time = std::chrono::system_clock::now();
    init_time();

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);
}

template <typename state_machine, typename command> raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template <typename state_machine, typename command> void raft<state_machine, command>::stop() {
    RAFT_LOG("stop");
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template <typename state_machine, typename command> bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template <typename state_machine, typename command> bool raft<state_machine, command>::is_leader(int &term) {
    term = current_term;
    return role == leader;
}

template <typename state_machine, typename command> void raft<state_machine, command>::start() {
    RAFT_LOG("start");
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    std::unique_lock<std::mutex> lock(mtx);
    if (role != leader) {
        return false;
    }
    term = current_term;
    index = log.back().index + 1;
    log_entry<command> entry(index, term, cmd);
    log.push_back(entry);
    nextIndex[my_id] = index + 1;
    matchIndex[my_id] = index;
    matchCount.push_back(1);
    if (!storage->appendLog(entry, log.size())) {
        storage->updateLog(log);
    }

    return true;
}

template <typename state_machine, typename command> bool raft<state_machine, command>::save_snapshot() {
    std::unique_lock<std::mutex> lock(mtx);

    snapshot = state->snapshot();

    if (lastApplied <= log.back().index) {
        log.erase(log.begin(), log.begin() + lastApplied - log.front().index);
    } else {
        log.clear();
    }

    storage->updateSnapshot(snapshot);
    storage->updateLog(log);

    return true;
}

/******************************************************************

                         RPC Related

*******************************************************************/
template <typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args arg, request_vote_reply &reply) {
    std::unique_lock<std::mutex> lock(mtx);

    last_time = std::chrono::system_clock::now();
    reply.term = current_term;
    reply.voteGranted = false;

    if (arg.term < current_term) {
        return 0;
    }

    if (arg.term > current_term) {
        set_role_and_term(follower, arg.term);
    }

    if (vote_for == -1 || vote_for == arg.candidateId) {
        if (judge_vote(arg.lastLogIndex, arg.lastLogTerm)) {
            vote_for = arg.candidateId;
            reply.voteGranted = true;

            storage->updateMetadata(current_term, vote_for);
        }
    }
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args &arg,
                                                             const request_vote_reply &reply) {
    std::unique_lock<std::mutex> lock(mtx);

    if (reply.term > current_term) {
        set_role_and_term(follower, reply.term);
        return;
    }
    if (role != candidate) {
        return;
    }

    if (reply.voteGranted && !votedNodes[target]) {
        votedNodes[target] = true;
        ++vote_count;
        if (vote_count > num_nodes() / 2) {
            set_role_and_term(leader, current_term);
        }
    }
    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply) {
    std::unique_lock<std::mutex> lock(mtx);
    last_time = system_clock::now();
    reply.term = current_term;
    reply.success = false;

    if (arg.term < current_term) {
        return 0;
    }

    if (arg.term > current_term || role == candidate) {
        set_role_and_term(follower, arg.term);
    }
    if (arg.prevLogIndex <= log.back().index && arg.prevLogTerm == get_term(arg.prevLogIndex)) {
        reply.success = true;
        if (!arg.entries.empty()) {
            if (arg.prevLogIndex < log.back().index) {
                if (arg.prevLogIndex + 1 <= log.back().index) {
                    log.erase(log.begin() + arg.prevLogIndex + 1 - log.front().index, log.end());
                }
                log.insert(log.end(), arg.entries.begin(), arg.entries.end());
                storage->updateLog(log);
            } else {
                log.insert(log.end(), arg.entries.begin(), arg.entries.end());
                if (!storage->appendLog(arg.entries, log.size())) {
                    storage->updateLog(log);
                }
            }
        }
        if (arg.leaderCommit > commitIndex) {
            commitIndex = std::min(arg.leaderCommit, log.back().index);
        }
    }

    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int target, const append_entries_args<command> &arg,
                                                               const append_entries_reply &reply) {
    std::unique_lock<std::mutex> lock(mtx);

    if (reply.term > current_term) {
        set_role_and_term(follower, reply.term);
        return;
    }
    if (role != leader) {
        return;
    }

    if (reply.success) {
        int prev = matchIndex[target];
        matchIndex[target] = std::max(matchIndex[target], (int)(arg.prevLogIndex + arg.entries.size()));
        nextIndex[target] = matchIndex[target] + 1;

        int last = std::max(prev - commitIndex, 0) - 1;
        for (int i = matchIndex[target] - commitIndex - 1; i > last; --i) {
            ++matchCount[i];
            if (matchCount[i] > num_nodes() / 2 && get_term(commitIndex + i + 1) == current_term) {
                commitIndex += i + 1;
                matchCount.erase(matchCount.begin(), matchCount.begin() + i + 1);
                break;
            }
        }
    } else {
        nextIndex[target] = std::min(nextIndex[target], arg.prevLogIndex);
    }

    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args arg, install_snapshot_reply &reply) {
    std::unique_lock<std::mutex> lock(mtx);
    last_time = system_clock::now();
    reply.term = current_term;

    if (arg.term < current_term) {
        return 0;
    }

    if (arg.term > current_term || role == candidate) {
        set_role_and_term(follower, arg.term);
    }

    if (arg.lastIncludedIndex <= log.back().index && arg.lastIncludedTerm == get_term(arg.lastIncludedIndex)) {
        int end_index = arg.lastIncludedIndex;

        if (end_index <= log.back().index) {
            log.erase(log.begin(), log.begin() + end_index - log.front().index);
        } else {
            log.clear();
        }
    } else {
        log.assign(1, log_entry<command>(arg.lastIncludedIndex, arg.lastIncludedTerm));
    }
    snapshot = arg.snapshot;
    state->apply_snapshot(snapshot);

    lastApplied = arg.lastIncludedIndex;
    commitIndex = std::max(commitIndex, arg.lastIncludedIndex);

    storage->updateLog(log);
    storage->updateSnapshot(arg.snapshot);

    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int target, const install_snapshot_args &arg,
                                                                 const install_snapshot_reply &reply) {
    std::unique_lock<std::mutex> lock(mtx);

    if (role != leader) {
        return;
    }
    if (reply.term > current_term) {
        set_role_and_term(follower, reply.term);
        return;
    }

    matchIndex[target] = std::max(matchIndex[target], arg.lastIncludedIndex);
    nextIndex[target] = matchIndex[target] + 1;

    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else{

    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else{

    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else{

    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template <typename state_machine, typename command> void raft<state_machine, command>::run_background_election() {
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    system_clock::time_point current_time;

    while (true) {
        if (is_stopped()){
            return;
        }

        lock.lock();
        current_time = system_clock::now();

        if(role == follower) {
            if (current_time - last_time > follower_timeout) {
                set_role_and_term(candidate, current_term + 1);
                // initial request arguments
                request_vote_args args{};
                args.term = current_term;
                args.candidateId = my_id;
                args.lastLogIndex = log.back().index;
                args.lastLogTerm = log.back().term;

                // send vote request to others
                for (int i = 0; i < num_nodes(); ++i) {
                    if (i == my_id)
                        continue;
                    thread_pool->addObjJob(this, &raft::send_request_vote, i, args);
                }

                // update election timer
                last_time = system_clock::now();
            }
        } else if(role == candidate){
                if (current_time - last_time > candidate_timeout) {
                    set_role_and_term(follower, current_term);
                }
        }
        // do nothing for leader

        lock.unlock();

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    return;
}

template <typename state_machine, typename command> void raft<state_machine, command>::run_background_commit() {
    // Send logs/snapshots to the follower.
    // Only work for the leader.

    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);

    while (true) {
        if (is_stopped()){
            return;
        }

        lock.lock();
        if (role == leader) {
            int last_log_index = this->log.back().index;
            for (int i = 0; i < num_nodes(); ++i) {
                if (i == my_id)
                    continue;
                if (nextIndex[i] <= last_log_index) {
                    if (nextIndex[i] > log.front().index) {
                        int pre_log_index = nextIndex[i] - 1;
                        append_entries_args<command> args(current_term, my_id, commitIndex, pre_log_index, get_term(pre_log_index),get_entries(nextIndex[i], last_log_index + 1));
                        thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
                    } else {
                        install_snapshot_args args(current_term, my_id, log.front().index, log.front().term, snapshot);
                        thread_pool->addObjJob(this, &raft::send_install_snapshot, i, args);
                    }
                }
            }
        }

        lock.unlock();

        std::this_thread::sleep_for(std::chrono::milliseconds(12));
    }

    return;
}

template <typename state_machine, typename command> void raft<state_machine, command>::run_background_apply() {
    // Apply committed logs the state machine
    // Work for all the nodes.

    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    std::vector<log_entry<command>> entries;

    while (true) {
        if (is_stopped())
            return;

        lock.lock();

        if (commitIndex > lastApplied) {
            entries = get_entries(lastApplied + 1, commitIndex + 1);
            for (log_entry<command> &entry : entries) {
                state->apply_log(entry.cmd);
            }
            lastApplied = commitIndex;
        }

        lock.unlock();

        std::this_thread::sleep_for(std::chrono::milliseconds(11));
    }
    return;
}

template <typename state_machine, typename command> void raft<state_machine, command>::run_background_ping() {
    // Send empty append_entries RPC to the followers.
    // Only work for the leader.

    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);

    while (true) {
        if (is_stopped())
            return;

        lock.lock();

        if (role == leader) {
            static append_entries_args<command> args{};
            args.term = current_term;
            args.leaderId = my_id;
            args.leaderCommit = commitIndex;
            for (int i = 0; i < num_nodes(); ++i) {
                if (i == my_id)
                    continue;
                args.prevLogIndex = nextIndex[i] - 1;
                args.prevLogTerm = get_term(args.prevLogIndex);
                thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
            }
        }

        lock.unlock();

        std::this_thread::sleep_for(std::chrono::milliseconds(150)); // Adjust param: ping period
    }
    return;
}

/******************************************************************

                        Other functions

*******************************************************************/
template <typename state_machine, typename command> void raft<state_machine, command>::init_time() {
    static std::random_device rd;
    static std::minstd_rand gen(rd());
    static std::uniform_int_distribution<int> follower_dis(300, 500);   // Adjust param
    static std::uniform_int_distribution<int> candidate_dis(800, 1000); // Adjust param
    follower_timeout = std::chrono::duration_cast<system_clock::duration>(std::chrono::milliseconds(follower_dis(gen)));
    candidate_timeout = std::chrono::duration_cast<system_clock::duration>(std::chrono::milliseconds(candidate_dis(gen)));
}

template <typename state_machine, typename command> inline int raft<state_machine, command>::get_term(int index) {
    return log[index - log.front().index].term;
}
template <typename state_machine, typename command>
inline std::vector<log_entry<command>> raft<state_machine, command>::get_entries(int begin_index, int end_index) {
    std::vector<log_entry<command>> ret;
    if (begin_index < end_index) {
        ret.assign(log.begin() + begin_index - log.front().index, log.begin() + end_index - log.front().index);
    }
    return ret;
}

template <typename state_machine, typename command>
bool  raft<state_machine, command>::judge_vote(int last_log_index, int last_log_term){
    return last_log_term > log.back().term || last_log_index >= log.back().index;
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::judge_append(int pre_log_index, int pre_log_term){
    if(pre_log_index > log.back().index || pre_log_term != get_term(pre_log_index)){
        return false;
    }
    return true;
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::judge_snapshot(int last_included_index, int last_included_term){
    if(last_included_index > log.back().index || last_included_term != get_term(last_included_index)){
        return false;
    }
    return true;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::set_role_and_term(raft_role role, int term){
    this->role = role;
    current_term = term;
    switch (role) {
        case leader:{
            // reinitialize leader volatile states
            nextIndex.assign(num_nodes(), log.back().index + 1);
            matchIndex.assign(num_nodes(), 0);
            matchIndex[my_id] = log.back().index;
            matchCount.assign(log.back().index - commitIndex, 0);
            break;
        }
        case follower:{
            current_term = term;
            vote_for = -1;
            storage->updateMetadata(current_term, vote_for);
            // re-randomize timeouts
            init_time();
            break;
        }
        case candidate:{
            // increment current term
            current_term = term;
            vote_for = my_id;
            vote_count = 1;
            votedNodes.assign(num_nodes(), false);
            votedNodes[my_id] = true;
            // do persist
            storage->updateMetadata(current_term, vote_for);
            // re-randomize timeouts
            init_time();
        }

    }
}

template <typename state_machine, typename command>
void  raft<state_machine, command>::init_state(){
    current_term = 0;
    vote_for = -1;
    log.assign(1, log_entry<command>(0, 0));
}
#endif // raft_h