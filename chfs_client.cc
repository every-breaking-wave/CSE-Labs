// chfs client.  implements FS operations using extent and lock server
#include "chfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <list>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

//#define DEBUG

chfs_client::chfs_client(std::string extent_dst) {
    ec = new extent_client(extent_dst);
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

//chfs_client::chfs_client(std::string extent_dst, std::string lock_dst) {
//    ec = new extent_client();
//    if (ec->put(1, "") != extent_protocol::OK)
//        printf("error init root dir\n"); // XYB: init root dir
//}

chfs_client::inum
chfs_client::n2i(std::string n) {
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum) {
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
chfs_client::isfile(inum inum) {
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        return false;
    }
    return a.type == extent_protocol::T_FILE;
}

/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 *
 * */

bool
chfs_client::isdir(inum inum) {
    // Oops! is this still correct when you implement symlink?
    // well, sure it should be corrected
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        return false;
    }
    return a.type == extent_protocol::T_DIR;
}

bool
chfs_client::issymlink(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }
    return a.type == extent_protocol::T_SYMLINK;


}

int
chfs_client::getfile(inum inum, fileinfo &fin) {
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

    release:
    return r;
}

int
chfs_client::getdir(inum inum, dirinfo &din) {
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

    release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
chfs_client::setattr(inum ino, size_t size) {
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    // 获取当前目录文件内容
    std::string buf;
    if (ec->get(ino, buf) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    buf.resize(size);
    if (ec->put(ino, buf) != extent_protocol::OK) {
        r = IOERR;
    }
    release:
    return r;
}

int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out) {
    int r = OK;
#ifdef DEBUG
    printf("\tfuse:im create, inum:%llu, name:%s\n", parent, name);
#endif
    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    std::string buf, append_str;
    bool if_exist;

    // 判断改文件名是否已经存在
    if (lookup(parent, name, if_exist, ino_out) != OK) {
        r = EXIST;
        goto release;
    }
    // 在当前目录下创建新文件
    if (ec->create(extent_protocol::types::T_FILE, ino_out) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    // 获取当前目录文件内容
    if (ec->get(parent, buf) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    // 将新建文件的entry添加到原目录
    append_str = std::string(name) + '/' + std::to_string(ino_out) + '&';
    buf.append(append_str);
    if (ec->put(parent, buf) != extent_protocol::OK) {
        r = IOERR;
    }

    release:
    return r;
}

int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out) {
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    std::string buf, append_str;
    bool if_exist;

    // 判断改文件名是否已经存在
    if (lookup(parent, name, if_exist, ino_out) != OK) {
        r = EXIST;
        goto release;
    }
    // 在当前目录下创建新目录
    if (ec->create(extent_protocol::types::T_DIR, ino_out) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    // 获取当前目录文件内容
    if (ec->get(parent, buf) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    // 将新建文件的entry添加到原目录
    append_str =  std::string(name) + '/' + std::to_string(ino_out) +'&';
    buf.append(append_str);
    if (ec->put(parent, buf) != extent_protocol::OK) {
        r = IOERR;
    }

    release:
    return r;
}

int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out) {
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
#ifdef DEBUG
    printf("look up inum: %lld, name: %s\n:", parent, name);
#endif
    std::list <dirent> dirent_list;

    this->readdir(parent, dirent_list);

    if(dirent_list.empty()) {
        found = false;
        goto release;
    }
    for (auto entry: dirent_list) {
        if (!entry.name.compare(name)) {
            found = true;
            ino_out = entry.inum;
            goto release;
        }
    }
    found = false;
    release:
    return r;
}

int
chfs_client::readdir(inum dir, std::list <dirent> &list) {
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    // my format:  name/inum&name/inum.....
    std::string data;
    int pos_begin = 0;
    int pos_end = 0;
    std::string name, inum;
    struct dirent dir_entry;
    if(!isdir(dir)){
        r = NOENT;
        goto release;
    }
    if (ec->get(dir, data) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    pos_end = data.find('/', pos_begin);
    while (std::string::npos != pos_end) {
        pos_end = data.find('/', pos_begin);
        name = data.substr(pos_begin, pos_end - pos_begin);
        pos_begin = pos_end + 1;
        pos_end = data.find('&', pos_begin);
        inum = data.substr(pos_begin, pos_end - pos_begin);
        dir_entry.inum = n2i(inum);
        dir_entry.name = name;
        list.push_back(dir_entry);
        pos_begin = pos_end + 1;
        pos_end = data.find('/', pos_begin);
    }
    release:
    return r;
}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data) {
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    if (ec->get(ino, data) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    if (off > data.size()) {
        data = "";
        goto release;
    }
    if (off + size > data.size()) {
        data = data.substr(off);
        goto release;
    }
    data = data.substr(off, size);

    release:
    return r;
}

int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
                   size_t &bytes_written) {
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

    std::string originData;
    if (ec->get(ino, originData) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    originData.resize(std::max(originData.size(), off + size));
    for (int i = 0; i < size; ++i) {
        originData[off + i] = data[i];
    }
    bytes_written = size;
    if (ec->put(ino, originData) != extent_protocol::OK) {
        r = IOERR;
    }
    release:
    return r;
}


int chfs_client::unlink(inum parent, const char *name) {
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

    bool found = false;
    inum inum;
    lookup(parent, name, found, inum);

    // can't unlink directory
    if(isdir(inum)) {
        return r;
    }
    ec->remove(inum);

    std::string buf;
    if(ec->get(parent, buf) != extent_protocol::OK){
        r = IOERR;
        return r;
    }
    int erase_start = buf.find(name);
    int erase_after = buf.find('&', erase_start);
    buf.erase(erase_start, erase_after - erase_start + 1);
    if(ec->put(parent, buf) != extent_protocol::OK) {
        r = IOERR;
    }
    return r;

}

int chfs_client::link(inum parent, const char *name, const char* link_path, inum & ino_out) {
    int r = OK;
    bool found = false;
    inum inum;
    lookup(parent, name, found, inum);
    std::string buf, append_str;

    if (found){
        r = EXIST;
        goto release;
    }

    if(ec->create(extent_protocol::T_SYMLINK, ino_out) != extent_protocol::OK){
        r = IOERR;
        goto release;
    }
    if(ec->put(ino_out, std::string(link_path)) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    if(ec->get(parent, buf) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    // 将新建文件的entry添加到原目录
    append_str =  std::string(name) + '/' + std::to_string(ino_out) +'&';
    buf.append(append_str);
    if (ec->put(parent, buf) != extent_protocol::OK) {
        r = IOERR;
    }

    release:
    return r;
}

int chfs_client::readlink(inum ino, std::string &data) {
    int r = OK;
    if(ec->get(ino, data) != extent_protocol::OK){
        r = IOERR;
    }
    return r;
}