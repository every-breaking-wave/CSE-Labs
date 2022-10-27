#ifndef persister_h
#define persister_h

#include <fcntl.h>
#include <mutex>
#include <iostream>
#include <fstream>
#include "rpc.h"

#define MAX_LOG_SZ 131072

#define DEBUG
/*
 * Your code here for Lab2A:
 * Implement class chfs_command, you may need to add command types such as
 * 'create', 'put' here to represent different commands a transaction requires. 
 * 
 * Here are some tips:
 * 1. each transaction in ChFS consists of several chfs_commands.
 * 2. each transaction in ChFS MUST contain a BEGIN command and a COMMIT command.
 * 3. each chfs_commands contains transaction ID, command type, and other information.
 * 4. you can treat a chfs_command as a log entry.
 */
class chfs_command {
public:
    typedef unsigned long long txid_t;
    enum cmd_type {
        CMD_BEGIN = 0,
        CMD_COMMIT,
        CMD_CREATE,
        CMD_PUT,
        CMD_REMOVE

    };

    cmd_type type = CMD_BEGIN;
    txid_t id = 0;
    std::string info = "";
    uint64_t inum = 0;

    // constructor
    chfs_command(){};

    chfs_command(cmd_type c_type, txid_t id, uint64_t inum, const std::string& info): type(c_type), id(id), inum(inum), info(info){};

    chfs_command(const chfs_command &cmd): type(cmd.type), id(cmd.id), inum(cmd.inum), info(cmd.info){} ;

    void to_string(char *buf) {
        uint32_t info_size = info.size();
        memcpy(buf, &type, sizeof(cmd_type));
        memcpy(buf + sizeof(cmd_type), &id, sizeof(txid_t));
        memcpy(buf + sizeof(cmd_type) + sizeof(txid_t), &inum, sizeof(uint64_t));
        memcpy(buf + sizeof(cmd_type) + sizeof(txid_t) + sizeof(uint64_t), &info_size, sizeof(uint32_t));
        memcpy(buf + sizeof(cmd_type) + sizeof(txid_t) + sizeof(uint64_t) + sizeof(uint32_t) , info.c_str(), info_size);
    }

    void to_command(const char *buf) {
        uint32_t info_size;
        memcpy(&type, buf, sizeof(cmd_type));
        memcpy(&id, buf + sizeof(cmd_type), sizeof(txid_t));
        memcpy(&inum, buf + sizeof(cmd_type) + sizeof(txid_t), sizeof(uint64_t));
        memcpy(&info_size, buf + sizeof(cmd_type) + sizeof(txid_t) + sizeof(uint64_t), sizeof(uint32_t));
        info.resize(info_size);
        memcpy(&info[0], buf + sizeof(cmd_type) + sizeof(txid_t) + sizeof(uint64_t) + sizeof(uint32_t), info_size);
    }

    uint64_t size() const {
        return sizeof(cmd_type) + sizeof(txid_t) + sizeof(uint64_t)+ sizeof(uint32_t) + info.size();
    }
};


/*
 * Your code here for Lab2A:
 * Implement class persister. A persister directly interacts with log files.
 * Remember it should not contain any transaction logic, its only job is to 
 * persist and recover data.
 * 
 * P.S. When and how to do checkpoint is up to you. Just keep your logfile size
 *      under MAX_LOG_SZ and checkpoint file size under DISK_SIZE.
 */
template<typename command>
class persister {

public:
    persister(const std::string& file_dir);
    ~persister();

    // persist data into solid binary file
    // You may modify parameters in these functions
    void append_log(command& log);
    void checkpoint();

    // restore data from solid binary file
    // You may modify parameters in these functions
    void restore_logdata();
    void restore_checkpoint();
    command form_command_by_params(unsigned long long id, chfs_command::cmd_type type, std::string info);
    std::vector<command> get_log_entry_vector(){
        return log_entries;
    }


private:
    std::mutex mtx;
    std::string file_dir;
    std::string file_path_checkpoint;
    std::string file_path_logfile;

    // restored log data
    std::vector<command> log_entries;
};



template<typename command>
persister<command>::persister(const std::string& dir){
    // DO NOT change the file names here
    file_dir = dir;
    file_path_checkpoint = file_dir + "/checkpoint.bin";
    file_path_logfile = file_dir + "/logdata.bin";
}

template<typename command>
persister<command>::~persister() {
    // Your code here for lab2A

}

template<typename command>
void persister<command>::append_log(command& log) {
    // Your code here for lab2A
    std::fstream out(file_path_logfile, std::ios::binary | std::ios::out | std::ios::app);

#ifdef DEBUG
    printf("id: %ull, type: %d, info: %s \n", log.id, log.type, log.info);
    std::cout<<log.info<<std::endl;
#endif
    uint32_t log_size = log.size();
    char *buf = new char [log_size];
    // serialize log to string type
    log.to_string(buf);
#ifdef DEBUG
    std::cout<<"log size : "<<log_size<<std::endl;
#endif
    out.write((char*)&(log_size), sizeof(uint32_t));
    out.write(buf, log_size);
    out.close();
}

template<typename command>
void persister<command>::checkpoint() {
    // Your code here for lab2A

}

template<typename command>
void persister<command>::restore_logdata() {
    // Your code here for lab2A
    std::fstream in(file_path_logfile , std::ios::binary | std::ios::in);
    if(!in.is_open())
    {
        std::cout<<"can not open file\n";
        return ;
    }
    in.seekg(0, std::ios::end);
    int m = in.tellg();
    in.seekg(0, in.beg);
    int l = in.tellg();
    printf("file size : %d\n", m - l);

    uint32_t log_size;
    log_entries.clear();
    while (in.peek() != EOF) {

        in.read((char*)&(log_size), sizeof(uint32_t));
#ifdef DEBUG
        std::cout<<"log_size"<<log_size<< "\n";
#endif
        char buf[log_size];
        in.read(buf, log_size);
        command cmd;
        cmd.to_command(buf);
        log_entries.template emplace_back(cmd);
#ifdef DEBUG
        std::cout<<cmd.info<<std::endl;
#endif
    }
#ifdef DEBUG
    std::cout<<"entry size : "<<log_entries.size()<<std::endl;
#endif


};

template<typename command>
void persister<command>::restore_checkpoint() {
    // Your code here for lab2A

}

template<typename command>
command persister<command>::form_command_by_params(unsigned long long id, chfs_command::cmd_type type, std::string info) {
    command log;
    log.id = id;
    log.type = type;
    log.info = info;
    return log;
}


using chfs_persister = persister<chfs_command>;

#endif // persister_h