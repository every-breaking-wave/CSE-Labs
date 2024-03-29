// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#define  DEBUG

extent_server::extent_server()
{
    im = new inode_manager();
}

int extent_server::create(uint32_t type, extent_protocol::extentid_t &id)
{
    // alloc a new inode and return inum
#ifdef DEBUG
    printf("extent_server: create inode\n");
#endif
    id = im->alloc_inode(type);

    return extent_protocol::OK;
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
    id &= 0x7fffffff;

    const char * cbuf = buf.c_str();
    int size = buf.size();
//    printf("id : %lld size : %d\n", id, size);

//    printf("id : %lld cbuf: %s size : %d\n", id, cbuf, size);
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
    im->remove_file(id);

    return extent_protocol::OK;
}

