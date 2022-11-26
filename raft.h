#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>
#include <set>
#include <vector>
#include <iostream>
#include <random>
#include <ctime>
#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

template<typename state_machine, typename command>
class raft {
    static_assert(std::is_base_of<raft_state_machine, state_machine>(),

    "state_machine must inherit from raft_state_machine");

    static_assert(std::is_base_of<raft_command, command>(),

    "command must inherit from raft_command");

    friend class thread_pool;

#define RAFT_LOG(fmt, args...) \
    do {                       \
    } while (0);

//#define RAFT_LOG(fmt, args...)                                                                                   \
//     do {                                                                                                         \
//         auto now =                                                                                               \
//             std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
//                 std::chrono::system_clock::now().time_since_epoch())                                             \
//                 .count();                                                                                        \
//         printf("[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, role,  ##args); \
//     } while (0);

public:
    raft(
            rpcs *rpc_server,
            std::vector<rpcc *> rpc_clients,
            int idx,
            raft_storage<command> *storage,
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

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int leader_id;

    std::thread *background_election;
    std::thread *background_ping;
    std::thread *background_commit;
    std::thread *background_apply;

    int heartbeat_timeout;
    int election_timeout;

    std::chrono::system_clock::time_point last_received_heartbeat_time;
    std::chrono::system_clock::time_point election_start_time;
    /* ----Persistent state on all server----  */
    int vote_for;
    int current_term;

    /* ---- Volatile state on all server----  */
    int commit_idx;
    int last_applied;

    /* ---- Volatile state on leader----  */
    std::vector<int> next_idx;
    std::vector<int> match_idx;
    std::set<int> vote_for_me;

    // Your code here:
    void set_role(raft_role role_, int new_term) {
        std::string str[3] = {"follower", "candidate", "leader"};
        if (role != role_) {
            RAFT_LOG("from %s to %s", str[role].c_str(), str[role_].c_str())
        }
        switch (role_) {
            case leader: {
                next_idx = std::vector<int>(num_nodes(), storage->logs.size() + 1);
                match_idx = std::vector<int>(num_nodes(), 0);
                break;
            }
            case follower: {
                current_term = new_term;
                set_vote_for(-1);
                vote_for_me.clear();
                last_received_heartbeat_time = std::chrono::system_clock::now();
                break;
            }
            case candidate: {
                vote_for = my_id;
                vote_for_me.clear();
                current_term = new_term;
                vote_for_me.insert(my_id);
                election_start_time = std::chrono::system_clock::now();
                break;
            }
        }
        role = role_;
    }

    void set_current_term(int new_term) {
        current_term = new_term;
    }

    void set_vote_for(int vote_for_) {
        this->vote_for = vote_for_;
    }

private:
    // RPC handlers
    int request_vote(const request_vote_args arg, request_vote_reply &reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply &reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply &reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);

    void handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply);

    void send_append_entries(int target, append_entries_args<command> arg);

    void
    handle_append_entries_reply(int target, const append_entries_args<command> &arg, const append_entries_reply &reply);

    void send_install_snapshot(int target, install_snapshot_args arg);

    void
    handle_install_snapshot_reply(int target, const install_snapshot_args &arg, const install_snapshot_reply &reply);

private:
    bool is_stopped();

    int num_nodes() {
        return rpc_clients.size();
    }

    // background workers
    void run_background_ping();

    void run_background_election();

    void run_background_commit();

    void run_background_apply();

    // Your code here:
    int get_commit_index();

    int get_log_term(int index);

    bool check_append(int pre_log_index, int pre_log_term);

    void update_commit_idx_after_get_reply();
};

template<typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs *server, std::vector<rpcc *> clients, int idx, raft_storage<command> *storage,
                                   state_machine *state) :
        stopped(false),
        rpc_server(server),
        rpc_clients(clients),
        my_id(idx),
        storage(storage),
        state(state),
        background_election(nullptr),
        background_ping(nullptr),
        background_commit(nullptr),
        background_apply(nullptr),
        heartbeat_timeout(500),
        vote_for(-1),
        commit_idx(0),
        last_applied(0),
        next_idx(clients.size(), 1),
        match_idx(clients.size(), 0),
        current_term(0),
        role(follower) {
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here:
    // Do the initialization
    vote_for_me.clear();
    commit_idx = 0;
    last_applied = 0;
    // generate seperately between 300 to 500 randomly
    election_timeout = 300 + (200 / rpc_clients.size()) * my_id;
    last_received_heartbeat_time = std::chrono::system_clock::now();
}

template<typename state_machine, typename command>
raft<state_machine, command>::~raft() {
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

template<typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    mtx.lock();
    term = current_term;
    mtx.unlock();
    return role == leader;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Lab3: Your code here
    last_received_heartbeat_time = std::chrono::system_clock::now();
    RAFT_LOG("start");
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Lab3: Your code here
    mtx.lock();
    if (role != leader) {
        mtx.unlock();
        return false;
    }
    term = current_term;
    log_entry<command> entry(cmd, current_term);
    storage->logs.template emplace_back(entry);
    match_idx[my_id] = storage->logs.size();
    index = (int) storage->logs.size();
    mtx.unlock();
    for (int i = 0; i < storage->logs.size(); ++i) {
        std::cout<<storage->logs[i].cmd.value<<" ";
    }
    std::cout<<std::endl;
    return true;
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Lab3: Your code here
    return true;
}

/******************************************************************

                         RPC Related

*******************************************************************/
template<typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply &reply) {
    // Lab3: Your code here
    mtx.lock();
    RAFT_LOG("receive request vote from %d, term %d", args.candidate_id, args.term);
    reply.term = current_term;
    reply.vote_granted = false;
    if (args.term < current_term) {
        RAFT_LOG("receive a append request smaller than itself")
        mtx.unlock();
        return 0;
    }
    if (args.term > current_term && role != follower) {
        set_role(follower, args.term);
    }
    if (vote_for == -1 || vote_for == args.candidate_id) {
        int last_log_index = storage->logs.size();
        int last_log_term = get_log_term(last_log_index);
        if (last_log_term < args.term || last_log_term == args.last_log_term && last_log_index <= args.last_log_index) {
            RAFT_LOG("node %d vote for node %d", my_id, args.candidate_id);
            reply.vote_granted = true;
            vote_for = args.candidate_id;
            // update storage
            storage->update(args.term, args.candidate_id);
        }
    }
    mtx.unlock();
    return 0;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args &arg,
                                                             const request_vote_reply &reply) {
    // Lab3: Your code here
    mtx.lock();
    // TODO: does the sequence matter?
//    RAFT_LOG("node %d handle vote reply, reply vote_granted: %d, term: %d", my_id, reply.vote_granted, reply.term);
    if (role != candidate) {
        mtx.unlock();
        return;
    }
    if (reply.term > current_term) {
        set_role(follower, arg.term);
        mtx.unlock();
        return;
    }
    if (reply.vote_granted) {
        vote_for_me.insert(target);
        if (vote_for_me.size() > num_nodes() / 2) {  // becomes a leader
            set_role(leader, current_term);
        }
    }
    mtx.unlock();
    return;
}

template<typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply) {
    // Lab3: Your code here
    reply.term = current_term;
    reply.success = false;
    mtx.lock();
    // message is unknown
    if (arg.term < current_term) {
        mtx.unlock();
        return 0;
    }
    last_received_heartbeat_time = std::chrono::system_clock::now();  // update heartbeat time
    // append to leader itself
    if (arg.leader_id == my_id) {
        reply.success = true;
        mtx.unlock();
        return 0;
    }
    // receive an append request with higher term
    if (arg.term > current_term) {
        set_role(follower, arg.term);
        // update storage
        storage->update(arg.term, -1);
    }
    // logs not consistent, can't append
//    if (arg.prev_log_index > storage->logs.size() - 1) {
//        mtx.unlock();
//        return 0;
//    }
    if (arg.entries.empty()) {
        set_role(follower, arg.term);
//        RAFT_LOG("receive heart beat pre log index %d prev log term %d", arg.prev_log_index, arg.prev_log_term)
    }
    if (check_append(arg.prev_log_index, arg.prev_log_term)) {
        storage->logs.erase(storage->logs.begin() + arg.prev_log_index, storage->logs.end());
        storage->logs.insert(storage->logs.end(), arg.entries.begin(), arg.entries.end());
        if(arg.entries.size()){
            RAFT_LOG("commit id %d  leader commit %d, current log size %d", commit_idx, arg.leader_commit, storage->logs.size());
        }
        if (arg.leader_commit > commit_idx) {
            commit_idx = arg.leader_commit > commit_idx ? storage->logs.size() : commit_idx;
            RAFT_LOG("node %d change commit idx to %d", my_id, commit_idx);
        }
        reply.success = true;
    }   // not able to append
    mtx.unlock();
    return 0;


}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int node, const append_entries_args<command> &arg,
                                                               const append_entries_reply &reply) {
    // Lab3: Your code here
    if (role != leader || arg.entries.empty()) {
        return;
    }
    mtx.lock();
    if (!reply.success) {
//        RAFT_LOG("append fail on %d", node);
        if (reply.term > current_term) {
            set_role(follower, reply.term);
            // update storage
            storage->current_term = reply.term;
            storage->vote_for = -1;
            storage->flush();
            last_received_heartbeat_time = std::chrono::system_clock::now();
        } else if (next_idx[node] > 1) {
            next_idx[node]--;
        }
    } else {
        match_idx[node] = arg.prev_log_index + arg.entries.size();
        next_idx[node] = match_idx[node] + 1;
        RAFT_LOG("current node %d match_idx %d", node, match_idx[node])
        update_commit_idx_after_get_reply();
//        RAFT_LOG("node %d append successfully until log[%d], current commit idx %d", node, match_idx[node], commit_idx);
    }
    mtx.unlock();

}

template<typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply &reply) {
    // Lab3: Your code here
    return 0;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int node, const install_snapshot_args &arg,
                                                                 const install_snapshot_reply &reply) {
    // Lab3: Your code here
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.
    srand(time(nullptr));
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here
        mtx.lock();
        if (role == follower) {
            int time = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now() - last_received_heartbeat_time).count();
            int random_time = 200 + rand() % 300;
            if (time >= random_time) {  // no legal leader, follower starts election
//                RAFT_LOG("node %d start election, time: %d, last_receive_time: %ld", my_id, time,
//                         last_received_heartbeat_time);
                // RAFT_LOG("%d have log[%ld], last_log_term = %d", my_id, storage->logs.size() - 1, storage->logs[storage->logs.size() - 1].term);
                set_role(candidate, current_term + 1);
                request_vote_args args;
                args.term = current_term;
                args.candidate_id = my_id;
                args.last_log_index = storage->logs.size() - 1;
                args.last_log_term = get_log_term(args.last_log_index);
                // send to every node except itself
                for (int i = 0; i < rpc_clients.size(); ++i) {
                    if (i != my_id) {
                        thread_pool->addObjJob(this, &raft::send_request_vote, i, args);
                    }
                }
            }
        } else if (role == candidate) {
            int time = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now() - last_received_heartbeat_time).count();
            if (time >= election_timeout) { // election failed
//                RAFT_LOG("node %d quit elect", my_id);
                set_role(follower, current_term);
            }
        }
        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here
        if (role != leader) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }
        mtx.lock();
        for (int i = 0; i < rpc_clients.size(); ++i) {
            if (i == my_id) {
                continue;
            }
//            if ((storage->logs.size() < next_idx[i] && (storage->logs.size() - 1) < match_idx[i]) ||
//                !storage->logs.size()) {
//                continue;
//            }
            if(storage->logs.size() < next_idx[i]){
                RAFT_LOG("node %d next_idx %d",i, next_idx[i])
                continue;
            }
            int prev_log_index = next_idx[i] - 1;
            std::vector <log_entry<command>> entries = std::vector<log_entry<command>>(
                    storage->logs.begin() + prev_log_index, storage->logs.end());
            append_entries_args<command> args(current_term, my_id, prev_log_index, get_log_term(prev_log_index),
                                              commit_idx, entries);
//            RAFT_LOG("end commit to node %d, prev_log_index %d prev_log_term %d, next_idx %d match idx %d commit idx %d log size %d ",
//                     i, prev_log_index, get_log_term(prev_log_index), next_idx[i], match_idx[i], commit_idx, storage->logs.size());
            if (!thread_pool->addObjJob(this, &raft::send_append_entries, i, args)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Periodly apply committed logs the state machine

    // Work for all the nodes.
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:
        mtx.lock();
//        RAFT_LOG("commit idx %d, last applied %d", commit_idx, last_applied)
        while (commit_idx > last_applied) {
//            RAFT_LOG("node %d should apply log, last applied: %d, commit_idx, %d", my_id, last_applied, commit_idx)
            state->apply_log(storage->logs[last_applied].cmd);
            RAFT_LOG("apply new log last_applied %d commit_idx %d", last_applied, commit_idx)
            last_applied++;
        }
        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:
        mtx.lock();
        if (role == leader) {
//            RAFT_LOG("leader %d send heartbeat to other nodes", my_id);
            int client_size = rpc_clients.size();
            int pre_log_index;
            for (int i = 0; i < client_size; ++i) {
                if (i == my_id) {
                    continue;
                }
                pre_log_index = next_idx[i] - 1;
                append_entries_args<command> args(current_term, my_id, pre_log_index, get_log_term(pre_log_index),
                                                  commit_idx, std::vector<log_entry<command>>());
//                RAFT_LOG("send heart beat to node %d", i)
                // call RPC asym
                thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
            }
        }
        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return;
}

/******************************************************************

                        Other functions

*******************************************************************/

template<typename state_machine, typename command>
int raft<state_machine, command>::get_commit_index() {
    std::vector<int> copy = match_idx;
    std::sort(copy.begin(), copy.end());
    return copy[copy.size() / 2];
}

template<typename state_machine, typename command>
int raft<state_machine, command>::get_log_term(int index) {
    if (index > storage->logs.size()) {
        return -1;
    }
    return index == 0 ? 0 : storage->logs[index - 1].term;
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::check_append(int pre_log_index, int pre_log_term) {
    // log size should >= leader size and term should be the same
//    RAFT_LOG("node %d check append, logs size %d, log_term %d pre_log_index %d, pre_log_term %d",
//             my_id, storage->logs.size(), get_log_term(pre_log_index), pre_log_index, pre_log_term)
    return (storage->logs.size()) >= pre_log_index && (get_log_term(pre_log_index) == pre_log_term);
}

template<typename state_machine, typename command>
void raft<state_machine, command>::update_commit_idx_after_get_reply() {
    std::vector<int> temp = match_idx;
    std::sort(temp.begin(), temp.end());
    commit_idx = temp[temp.size() / 2];
    RAFT_LOG("new commit idx after get reply %d", commit_idx)
}

#endif // raft_h