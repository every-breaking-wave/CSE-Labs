#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <string>
#include <vector>
#include <mutex>

#include "mr_protocol.h"
#include "rpc.h"

using namespace std;

struct Task {
    int taskType;     // should be either Mapper or Reducer
    bool isAssigned;  // has been assigned to a worker
    bool isCompleted; // has been finised by a worker
    int index;        // index to the file
};

class Coordinator {
public:
    Coordinator(const vector <string> &files, int nReduce);

    mr_protocol::status askTask(int, mr_protocol::AskTaskResponse &reply);

    mr_protocol::status submitTask(int taskType, int index, bool &success);

    bool isFinishedMap();

    bool isFinishedReduce();

    bool Done();

    bool assignTask(Task &newTask);

private:
    vector <string> files;
    vector <Task> mapTasks;
    vector <Task> reduceTasks;

    mutex mtx;

    long completedMapCount;
    long completedReduceCount;
    bool isFinished;

    string getFile(int index);
};


// Your code here -- RPC handlers for the worker to call.

mr_protocol::status Coordinator::askTask(int, mr_protocol::AskTaskResponse &reply) {
    // Lab4 : Your code goes here.
    Task newTask;
    if (assignTask(newTask)) {
        reply.index = newTask.index;
        reply.tasktype = static_cast<mr_tasktype>(newTask.taskType);
        reply.filename = getFile(newTask.index);
        reply.nfiles = files.size();
    } else {
        reply.index = -1;
        reply.tasktype = mr_tasktype::NONE;
        reply.filename = "";
    }
    return mr_protocol::OK;
}

mr_protocol::status Coordinator::submitTask(int taskType, int index, bool &success) {
    // Lab4 : Your code goes here.

    std::unique_lock <std::mutex> lock(mtx);
    if (taskType == mr_tasktype::MAP) {
        mapTasks[index].isAssigned = false;
        mapTasks[index].isCompleted = true;
        completedMapCount++;
    } else if (taskType == mr_tasktype::REDUCE) {
        reduceTasks[index].isAssigned = false;
        reduceTasks[index].isCompleted = true;
        completedReduceCount++;
    }
    // judge if all task are completed
    if (completedMapCount >= (long) mapTasks.size() && completedReduceCount >= (long) reduceTasks.size()) {
        isFinished = true;
    }
    success = true;
    return mr_protocol::OK;
}

string Coordinator::getFile(int index) {
    std::unique_lock <std::mutex> lock(mtx);
    string file = this->files[index];
    return file;
}

bool Coordinator::isFinishedMap() {
    bool isFinished = false;
    std::unique_lock <std::mutex> lock(mtx);
    if (this->completedMapCount >= long(this->mapTasks.size())) {
        isFinished = true;
    }
    return isFinished;
}

bool Coordinator::isFinishedReduce() {
    bool isFinished = false;
    std::unique_lock <std::mutex> lock(mtx);
    if (this->completedReduceCount >= long(this->reduceTasks.size())) {
        isFinished = true;
    }
    return isFinished;
}

bool Coordinator::assignTask(Task &newTask) {
    newTask.taskType = mr_tasktype::NONE;
    std::unique_lock <std::mutex> lock(mtx);
    if (completedMapCount < (long) mapTasks.size()) {
        for (auto &mapTask: mapTasks) {
            if (!mapTask.isAssigned && !mapTask.isCompleted) {
                newTask = mapTask;
                mapTask.isAssigned = true;
                return true;
            }
        }
    } else if (completedReduceCount < (long) reduceTasks.size()) {
        for (auto &reduceTask: reduceTasks) {
            if (!reduceTask.isAssigned && !reduceTask.isCompleted) {
                newTask = reduceTask;
                reduceTask.isAssigned = true;
                return true;
            }
        }
    }
    return false;
}


//
// mr_coordinator calls Done() periodically to find out
// if the entire job has finished.
//
bool Coordinator::Done() {
    bool r = false;
    std::unique_lock <std::mutex> lock(mtx);
    r = this->isFinished;
    return r;
}

//
// create a Coordinator.
// nReduce is the number of reduce tasks to use.
//
Coordinator::Coordinator(const vector <string> &files, int nReduce) {
    this->files = files;
    this->isFinished = false;
    this->completedMapCount = 0;
    this->completedReduceCount = 0;

    int filesize = files.size();
    for (int i = 0; i < filesize; i++) {
        this->mapTasks.push_back(Task{mr_tasktype::MAP, false, false, i});
    }
    for (int i = 0; i < nReduce; i++) {
        this->reduceTasks.push_back(Task{mr_tasktype::REDUCE, false, false, i});
    }
}

int main(int argc, char *argv[]) {
    int count = 0;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s <port-listen> <inputfiles>...\n", argv[0]);
        exit(1);
    }
    char *port_listen = argv[1];

    setvbuf(stdout, NULL, _IONBF, 0);

    char *count_env = getenv("RPC_COUNT");
    if (count_env != NULL) {
        count = atoi(count_env);
    }

    vector <string> files;
    char **p = &argv[2];
    while (*p) {
        files.push_back(string(*p));
        ++p;
    }

    rpcs server(atoi(port_listen), count);

    Coordinator c(files, REDUCER_COUNT);

    //
    // Lab4: Your code here.
    // Hints: Register "askTask" and "submitTask" as RPC handlers here
    //
    server.reg(mr_protocol::asktask, &c, &Coordinator::askTask);
    server.reg(mr_protocol::submittask, &c, &Coordinator::submitTask);

    while (!c.Done()) {
        sleep(1);
    }

    return 0;
}


