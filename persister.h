#ifndef persister_h
#define persister_h

#include <fcntl.h>
#include <mutex>
#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include <algorithm>
#include <vector>
#include <assert.h>
#include "rpc.h"
#include "extent_server.h"
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
    void checkpoint(std::string & buf);
    size_t get_file_size(std::string filename);
    bool should_check_point();

    // restore data from solid binary file
    // You may modify parameters in these functions
    void restore_logdata();
    void restore_checkpoint();
    uint32_t get_log_size(){return log_size;}
    void set_log_size(uint32_t new_size) {}
    command form_command_by_params(unsigned long long id, chfs_command::cmd_type type, std::string info);
    std::vector<command> get_log_entry_vector(){
        return log_entries;
    }
    std::vector<std::pair<uint32_t , std::pair<uint32_t, std::string> > > get_checkpoint_pair_vec(){
        return checkpoint_pair_vec;
    }


private:
    std::mutex mtx;
    std::string file_dir;
    std::string file_path_checkpoint;
    std::string file_path_logfile;
    // restored log data
    std::vector<command> log_entries;
    std::vector<std::pair<uint32_t , std::pair<uint32_t, std::string> > > checkpoint_pair_vec;

    uint32_t log_size = 0;


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
void persister<command>::checkpoint(std::string &buf) {
    // Your code here for lab2A
    // delete committed logs
    restore_logdata();
    std::vector<chfs_command> logs = get_log_entry_vector();
#ifdef DEBUG
    std::cout<<"begin checkpoint\n";
    for (int i = 0; i < logs.size(); ++i) {
        std::cout<<"tx id: "<<logs[i].id<<"type: "<<logs[i].type<<std::endl;
    }
#endif

    std::vector<chfs_command::txid_t> committed_tx_vector;
    int bef_size = logs.size();
    for(chfs_command log : logs) {
        if(log.type == chfs_command::CMD_COMMIT) {
            committed_tx_vector.template emplace_back(log.id);
        }
    }
    std::vector<chfs_command>::iterator it;
    for (it = logs.begin();  it != logs.end() ; ) {
        if(std::count(committed_tx_vector.begin(), committed_tx_vector.end(), (*it).id)){
            it = logs.erase(it);
        } else {
            ++it;
        }
    }
    // 删除已经commit的log
    printf("bef size %d , now size %d\n", bef_size, logs.size());
    std::fstream out(file_path_logfile, std::ios::out | std::ios::trunc | std::ios::binary);
    for(chfs_command log : logs) {
        uint32_t log_size = log.size();
        char *log_buf = new char [log_size];
        log.to_string(log_buf);
        out.write((char*)&(log_size), sizeof(uint32_t));
        out.write(log_buf, log_size);
    }
    out.close();

    // 对当前文件系统进行 snapshot
    const char * snapshot = buf.c_str();
#ifdef DEBUG
    printf("snapshot size is : %d\n", buf.size());
#endif
    std::fstream fp(file_path_checkpoint, std::ios::out | std::ios::binary);
    if (fp.is_open()){
        fp.write(snapshot, buf.size());
    }
    fp.close();
}

template<typename command>
void persister<command>::restore_logdata() {
    // Your code here for lab2A
    std::fstream in(file_path_logfile , std::ios::binary | std::ios::in);
    if(!in.is_open()){
        std::cout<<"can not open log file\n";
        return ;
    }
    in.seekg(0, std::ios::end);
    int m = in.tellg();
    in.seekg(0, in.beg);
    int l = in.tellg();

    uint32_t log_size;
    log_entries.clear();
    while (in.peek() != EOF) {
        in.read((char*)&(log_size), sizeof(uint32_t));
        char buf[log_size];
        in.read(buf, log_size);
        command cmd;
        cmd.to_command(buf);
        log_entries.template emplace_back(cmd);
    }
#ifdef DEBUG
    std::cout<<"entry size : "<<log_entries.size()<<std::endl;
#endif


}

template<typename command>
void persister<command>::restore_checkpoint() {
    // Your code here for lab2A
    std::fstream in(file_path_checkpoint, std::ios::binary | std::ios::in);
#ifdef DEBUG
    printf("restore checkpoint\n");
#endif
    if(in.is_open()){
        checkpoint_pair_vec.clear();
        char buf[20];
        uint32_t size, type, inum;
        std::string data="";
        while (in.peek() != EOF){
            // read file size
            printf("loop \n");
            in.read(buf, 16);
            size = std::stoi(std::string(buf, buf + 16));
            printf("read size %d\n", size);
            std::pair<uint32_t , std::pair<uint32_t, std::string> > pair;
            if(size) {
                // read file type
                in.read(buf, 16);
                type = std::stoi(std::string(buf, buf + 16));
                printf("read type %d\n", type);

                // read file inum
                in.read(buf, 16);
                inum = std::stoi(std::string(buf, buf + 16));
                printf("read inum %d\n", inum);

                pair.second.first = type;
                pair.first = inum;

                char tem_data[size];
                in.read(tem_data, size);
                data = (std::string(tem_data, tem_data + size));
                pair.second.second = data;
                checkpoint_pair_vec.template emplace_back(pair);
            } else {
                pair.first = 0;
                pair.second.first = 0;
                pair.second.second = "";
                checkpoint_pair_vec.template emplace_back(pair);
            }

#ifdef DEBUG
            printf("type: %d , size : %d  inum %d data : %s\n", type, size,  inum,data);
#endif
        }
    }
    in.close();
}

template<typename command>
command persister<command>::form_command_by_params(unsigned long long id, chfs_command::cmd_type type, std::string info) {
    command log;
    log.id = id;
    log.type = type;
    log.info = info;
    return log;
}

template<typename command>
size_t persister<command>::get_file_size(std::string filename) {
    const char * fileName = filename.c_str();
    if (fileName == NULL) {
        return 0;
    }

    // 这是一个存储文件(夹)信息的结构体，其中有文件大小和创建时间、访问时间、修改时间等
    struct stat statbuf;

    // 提供文件名字符串，获得文件属性结构体
    stat(fileName, &statbuf);

    // 获取文件大小
    size_t filesize = statbuf.st_size;

    return filesize;
}

template<typename command>
bool persister<command>::should_check_point() {
    return get_file_size(file_path_logfile) > MAX_LOG_SZ * 0.8;
}


using chfs_persister = persister<chfs_command>;



#endif // persister_h