#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>

#include <mutex>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include "rpc.h"
#include "mr_protocol.h"

using namespace std;

struct KeyVal {
    KeyVal(string key_, string val_) : key(key_), val(val_){}
    string key;
    string val;
};

int stringToIntHash(const string &keyStr){
    int size = keyStr.size();
    int hash = 1;
    int p = 1999, mod = 101;
    for (int i = 0; i < size - 1; ++i) {
        hash = (hash * p + i + 1) % mod;
    }
    return hash % REDUCER_COUNT;
}

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
vector<KeyVal> Map(const string &filename, const string &content)
{
	// Copy your code from mr_sequential.cc here.
    // Your code goes here
    // Hints: split contents into an array of words.
    vector<KeyVal> word_to_count;
    int begin = 0, end = 0;
    string str = "";
    while (content[end] != '\0'){
        if(isalpha(content[end])){
            end++;
        } else if(end > begin){
            str = content.substr(begin, end - begin);
            word_to_count.emplace_back(KeyVal(str, "1"));
            end ++;
            begin = end;
        } else{    // illegal character or space, skip it
            while (!isalpha(content[end]) && content[end] != '\0'){
                end++;
            }
            begin = end;
        }
    }
    // last string
    if(end > begin){
        str = content.substr(begin, end - begin);
        word_to_count.emplace_back(KeyVal(str, "1"));
    }
    return word_to_count;
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
string Reduce(const string &key, const vector < string > &values)
{
    // Copy your code from mr_sequential.cc here.
    size_t count = 0;
    for(auto value : values){
        count += atoll(value.c_str());
    }
    return to_string(count);
}


typedef vector<KeyVal> (*MAPF)(const string &key, const string &value);
typedef string (*REDUCEF)(const string &key, const vector<string> &values);

class Worker {
public:
	Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf);

	void doWork();

private:
	void doMap(int index, const vector<string> &filenames);
	void doReduce(int index, int nfiles);
	void doSubmit(mr_tasktype taskType, int index);

	mutex mtx;
	int id;
    bool isworking;
	rpcc *cl;
	std::string basedir;
	MAPF mapf;
	REDUCEF reducef;
};


Worker::Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf)
{
	this->basedir = dir;
	this->mapf = mf;
	this->reducef = rf;
    this->isworking = false;

	sockaddr_in dstsock;
	make_sockaddr(dst.c_str(), &dstsock);
	this->cl = new rpcc(dstsock);
	if (this->cl->bind() < 0) {
		printf("mr worker: call bind error\n");
	}
}

void Worker::doMap(int index, const vector<string> &filenames)
{
	// Lab4: Your code goes here.
    isworking = true;
    string intermediatePrefix;
    //this->basedir
    intermediatePrefix = basedir + "mr-" + to_string(index) + "-";
    string content;
    string filename = filenames.front();
    ifstream file(filename);
    ostringstream tmp;
    tmp << file.rdbuf();
    content = tmp.str();

    vector <KeyVal> keyVals = Map(filename, content);

    vector <string> contents(REDUCER_COUNT);
    for (const KeyVal &keyVal : keyVals) {
        int reducerId = stringToIntHash(keyVal.key);
        contents[reducerId] += keyVal.key + ' ' + keyVal.val + '\n';
    }

    for (int i = 0; i < REDUCER_COUNT; ++i) {
        const string &content = contents[i];
        if (!content.empty()) {
            string intermediateFilepath = intermediatePrefix + to_string(i);
            ofstream file(intermediateFilepath, ios::out);
            file << content;
            file.close();
        }
    }

    file.close();

}

void Worker::doReduce(int index, int nfiles)
{
	// Lab4: Your code goes here.
    string filepath;
    unordered_map<string, unsigned long long> wordFreqs;
    for (int i = 0; i < nfiles; ++i) {
        filepath = basedir + "mr-" + to_string(i) + '-' + to_string(index);
        ifstream file(filepath, ios::in);
        if (!file.is_open()) {
            continue;
        }
        string key, value;
        while (file >> key >> value)
            wordFreqs[key] += atoll(value.c_str());
        file.close();
    }

    string content;
    for (const pair<string, unsigned long long> &keyVal : wordFreqs)
        content += keyVal.first + ' ' + to_string(keyVal.second) + '\n';

    ofstream mrOut(basedir + "mr-out", ios::out | ios::app);
    mrOut << content << endl;
    mrOut.close();
    isworking = true;
}

void Worker::doSubmit(mr_tasktype taskType, int index)
{
	bool b;
	mr_protocol::status ret = this->cl->call(mr_protocol::submittask, taskType, index, b);
	if (ret != mr_protocol::OK) {
		fprintf(stderr, "submit task failed\n");
		exit(-1);
	}
    isworking = false;
}

void Worker::doWork()
{
	for (;;) {

		//
		// Lab4: Your code goes here.
		// Hints: send asktask RPC call to coordinator
		// if mr_tasktype::MAP, then doMap and doSubmit
		// if mr_tasktype::REDUCE, then doReduce and doSubmit
		// if mr_tasktype::NONE, meaning currently no work is needed, then sleep
		//
        mr_protocol::AskTaskResponse res;
        if(!this->isworking){
            cl->call(mr_protocol::asktask, id, res);
        }
        switch (res.tasktype) {
            case mr_tasktype::MAP:{
                doMap(res.index, vector<string>({res.filename}));
                doSubmit(mr_tasktype::MAP, res.index);
                break;
            }
            case mr_tasktype::REDUCE:{
                doReduce(res.index, res.nfiles);
                doSubmit(mr_tasktype::REDUCE, res.index);
                break;
            }
            case mr_tasktype::NONE:{
                sleep(1);
            }
        }

	}
}

int main(int argc, char **argv)
{
	if (argc != 3) {
		fprintf(stderr, "Usage: %s <coordinator_listen_port> <intermediate_file_dir> \n", argv[0]);
		exit(1);
	}

	MAPF mf = Map;
	REDUCEF rf = Reduce;

	Worker w(argv[1], argv[2], mf, rf);
	w.doWork();

	return 0;
}

