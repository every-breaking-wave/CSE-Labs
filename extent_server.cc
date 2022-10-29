// the extent server implementation

#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#define  DEBUG

#include "extent_server.h"
#include "persister.h"

extent_server::extent_server()
{
  im = new inode_manager();
  _persister = new chfs_persister("log"); // DO NOT change the dir name here
  tx_id = 0;
  // Your code here for Lab2A: recover data on startup
  _persister->restore_logdata();
  _persister->restore_checkpoint();
  std::vector<chfs_command> logs = _persister->get_log_entry_vector();
  auto checkpoint_pairs = _persister->get_checkpoint_pair_vec();

  // 恢复到 checkpoint 的 snapshot
  if(checkpoint_pairs.size()) {
      for(auto pair : checkpoint_pairs){
          if(pair.second.second.size()) {
              im->alloc_inode_by_inum(pair.second.first, pair.first);
              im->write_file(pair.first, pair.second.second.c_str(), pair.second.second.size());
          }
      }
  }

    // redo not committed tx
    if(logs.size() == 0) {
      return;
    }
    int beg_pos = 0, size = logs.size();
    std::vector<chfs_command> tem_log_vector;
    while (beg_pos < size){
        if (logs[beg_pos].type == chfs_command::CMD_BEGIN) {
            beg_pos++;
            for (int i = beg_pos; i < size; ++i) {
                switch (logs[i].type) {
                    case chfs_command::CMD_COMMIT:   // 该 tx 已提交
                        for (int j = beg_pos; j < i; ++j) {
                            tem_log_vector.emplace_back(logs[j]);
                        }
                        beg_pos++;
                        break;
                    case chfs_command::CMD_BEGIN:  // 说明这之前的 tx 都没有被提交, 更新 beg_pos
                        beg_pos = i;
                    default:                      // 除此之外的 command 直接略过
                        continue;
                }
            }
        } else {
            break;
        }
    }

#ifdef DEBUG
    printf("begin to recover, log size is %d\n",tem_log_vector.size());
#endif
    for(chfs_command log : tem_log_vector) {
        this->tx_id = log.id;
        switch (log.type) {
            case chfs_command::CMD_CREATE:
                im->alloc_inode(std::stoi(log.info));
                break;
            case chfs_command::CMD_PUT:
                im->write_file(log.inum, log.info.c_str(), log.info.size());
                break;
            case chfs_command::CMD_REMOVE:
                im->remove_file(log.inum);
            default:
                break;
        }
    }

}

int extent_server::begin(uint64_t &tx_id) {
    this->tx_id += 1;
    tx_id = this->tx_id;
#ifdef DEBUG
    printf("a new tx is created\n");
#endif
    _persister->append_log(*(new chfs_command(chfs_command::CMD_BEGIN, tx_id, 0, "")));
    return extent_protocol::OK;
}

int extent_server::commit(uint64_t &tx_id) {
    _persister->append_log(*(new chfs_command(chfs_command::CMD_COMMIT, tx_id, 0, "")));
    if(_persister->should_check_point()){
        std::string buf = "";
        get_all_file(buf);
        _persister->checkpoint(buf);
    }
    return extent_protocol::OK;
}

int extent_server::create(uint32_t type, extent_protocol::extentid_t &id)
{
    // alloc a new inode and return inum
    id = im->alloc_inode(type);
    char info[sizeof (uint32_t) + sizeof (extent_protocol::extentid_t)];
//    memcpy(info, (char*)&type, sizeof (uint32_t));
    // type is required
    _persister->append_log(*(new chfs_command(chfs_command::CMD_CREATE, this->tx_id, 0, std::to_string(type))));

#ifdef DEBUG
    std::cout<<"info: "<<info<<std::endl;
#endif
    return extent_protocol::OK;
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
    id &= 0x7fffffff;

    const char * cbuf = buf.c_str();
    int size = buf.size();
#ifdef DEBUG
    printf("in extent server , size : %d\n", size);
#endif
    // buf is required
    _persister->append_log(*(new chfs_command(chfs_command::CMD_PUT, this->tx_id, id, buf)));
    im->write_file(id, cbuf, size);
    return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  printf("extent_server: get %lld\n", id);

    id &= 0x7fffffff;

    int size = 0;
    char *cbuf = NULL;

    im->read_file(id, &cbuf, &size);
    if (size == 0)
        buf = "";
    else {
        buf.assign(cbuf, size);
        free(cbuf);
    }

    return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
#ifdef DEBUG
    printf("extent_server: getattr %lld\n", id);
#endif

    id &= 0x7fffffff;

    extent_protocol::attr attr;
    memset(&attr, 0, sizeof(attr));
    im->get_attr(id, attr);
    a = attr;

    return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{

#ifdef DEBUG
    printf("extent_server: create inode\n");
#endif
    id &= 0x7fffffff;

    _persister->append_log(*(new chfs_command(chfs_command::CMD_REMOVE, this->tx_id, id, "")));
    im->remove_file(id);

    return extent_protocol::OK;
}

int extent_server::get_all_file(std::string &buf) {
    auto convert = [](int num)->std::string {
        std::string  str = std::to_string(num);
        while (str.size() < 16) {
            str = "0" + str;
        }
        return str;
    };


    std::string data = "";
    extent_protocol::attr attr;
    // 读取整个文件系统所有inode对应的文件内容，以及对应的文件type
    for (int i = 1; i < INODE_NUM; ++i) {
        this->get(i, data);
        this->getattr(i, attr);
        uint32_t size = data.size();
        buf.append(convert(size));

        // 若inode未分配， 只记录size = 0，作为填位符，读出时直接跳过这个inode
        if(size == 0){
            continue;
        }
        buf.append(convert(attr.type));
        buf.append(convert(i));
        std::cout<<"attr type is :" + convert(attr.type)<<std::endl;
        buf.append(data);
    }
    printf("get all file size is %d\n", buf.size());


    //  test code
#ifdef DEBUG

#endif
    return extent_protocol::OK;
}
