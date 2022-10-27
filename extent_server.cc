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
  std::vector<chfs_command> logs = _persister->get_log_entry_vector();
  if(logs.size() == 0)
      return;
  // redo not committed tx
    int beg_pos = 0, size = logs.size();
    std::vector<chfs_command> tem_log_vector;
    while (true){
        if (logs[beg_pos].type == chfs_command::CMD_BEGIN) {
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
    tx_id = ++(this->tx_id);
#ifdef DEBUG
    printf("a new tx is created\n");
#endif
    _persister->append_log(*(new chfs_command(chfs_command::CMD_BEGIN, tx_id, 0, "")));
    return extent_protocol::OK;
}

int extent_server::commit(uint64_t &tx_id) {
    _persister->append_log(*(new chfs_command(chfs_command::CMD_COMMIT, tx_id, 0, "")));
    return extent_protocol::OK;
}

int extent_server::create(uint32_t type, extent_protocol::extentid_t &id)
{
    // alloc a new inode and return inum
    id = im->alloc_inode(type);
    char info[sizeof (uint32_t) + sizeof (extent_protocol::extentid_t)];
//    memcpy(info, (char*)&type, sizeof (uint32_t));
    // type is required
    _persister->append_log(*(new chfs_command(chfs_command::CMD_CREATE, 1, 0, std::to_string(type))));

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
    _persister->append_log(*(new chfs_command(chfs_command::CMD_PUT, 1, id, buf)));
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

    _persister->append_log(*(new chfs_command(chfs_command::CMD_REMOVE, 1, id, "")));
    im->remove_file(id);

    return extent_protocol::OK;
}
