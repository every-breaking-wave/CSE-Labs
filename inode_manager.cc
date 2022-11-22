#include "inode_manager.h"
#include <time.h>
#include <assert.h>
#include <iostream>
#include <string>


#define MIN(a, b) ((a)<(b) ? (a) : (b))
#define MAX(a, b) ((a)>(b) ? (a) : (b))
//#define DEBUG
// disk layer -----------------------------------------

disk::disk() {
    // 初始化disk 全部置0
    bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf) {

//#ifdef DEBUG
//    printf("%s blockID\n", blocks[id]);
//#endif
#ifdef DEBUG
    printf("\tim read_block, blockid %d\n", id);
#endif
    memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf) {
#ifdef DEBUG
    printf("\tim write block, blockid %d\n", id);
#endif
    bzero(blocks[id], BLOCK_SIZE);
    memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block() {
    /*
     * your code goes here.
     * note: you should mark the corresponding bit in block bitmap when alloc.
     * you need to think about which block you can start to be allocated.
     */


    // find a free block
    if(!lastAllocBlock) {
        lastAllocBlock = DATABLOCK;
    }
    blockid_t blockid = this->lastAllocBlock;
#ifdef DEBUG
    printf("\tim alloc_block, block_id %d\n", blockid);
#endif
    for (int i = 0; i < BLOCK_NUM - DATABLOCK; ++i) {
        if (this->using_blocks[blockid] == 0) {
            using_blocks[blockid] = 1;
            lastAllocBlock = blockid + 1;
            return blockid;
        }
        blockid = (blockid + 1) > BLOCK_NUM ? (DATABLOCK) : blockid + 1;
    }
    // no free block
    return -1;
}

void
block_manager::free_block(uint32_t id) {

#ifdef DEBUG
    printf("\tim free_block, blockid %d\n", id);
#endif
    /*
     * your code goes here.
     * note: you should unmark the corresponding bit in the block bitmap when free.
     */
    using_blocks[id] = 0;
}


// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager() {
    d = new disk();

    // format the disk

    for (int i = 0; i < DATABLOCK - 1; ++i) {
        this->using_blocks[i] = 1;
    }
    sb.size = BLOCK_SIZE * BLOCK_NUM;
    sb.nblocks = BLOCK_NUM;
    sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf) {
    d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf) {
    d->write_block(id, buf);
}

bool
block_manager::is_free_block(blockid_t blockid) {
    return !using_blocks[blockid];
}

// inode layer -----------------------------------------

inode_manager::inode_manager() {
    bm = new block_manager();
    uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
    if (root_dir != 1) {
        printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
        exit(0);
    }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type) {
    /*
     * your code goes here.
     * note: the normal inode block should begin from the 2nd inode block.
     * the 1st is used for root_dir, see inode_manager::inode_manager().
     */

#ifdef DEBUG
    printf("\tim alloc inode, type: %d\n", type);
#endif
    int inum = last_fit_inum;
    inode_t *inode;
    int count = 0;
    while (count < INODE_NUM) {
        inode = get_inode(inum);
        if (inode != NULL) {
            inum = (inum + 1) % INODE_NUM;
            free(inode);
            continue;
        } else {
            inode_t *ino;
            ino = (inode_t *) malloc(sizeof(inode_t));
            bzero(ino, sizeof(inode_t));
            ino->type = type;
            ino->size = 0;
            unsigned int t = time(NULL);
            ino->atime = t;
            ino->mtime = t;
            ino->ctime = t;
            put_inode(inum, ino);
            free(ino);
            last_fit_inum = inum;
            return inum;
        }
    }
    // if count == INODE_NUM then alloc failed
    assert(count != INODE_NUM);
    return 1;
}

void
inode_manager::free_inode(uint32_t inum) {
    /*
     * your code goes here.
     * note: you need to check if the inode is already a freed one;
     * if not, clear it, and remember to write back to disk.
     */
#ifdef DEBUG
    printf("\tim free_inode, inum: %d\n", inum);
#endif
    inode_t *inode = get_inode(inum);
    if (!inode) {
        return;
    }
    inode->type = 0;
    inode->size = 0;
    uint32_t t = time(NULL);
    inode->atime = t;
    inode->mtime = t;

    put_inode(inum, inode);
    free(inode);
    return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode *
inode_manager::get_inode(uint32_t inum) {
    inode_t *ino;
    /*
     * your code goes here.
     */
#ifdef DEBUG
    printf("\tim get_inode, inum: %d\n", inum);
#endif
    // boundary check
    if (inum < 0 || inum >= INODE_NUM) {
        return NULL;
    }

    // block is the smallest unit can be read from inode File System
    char buf[BLOCK_SIZE];
    this->bm->read_block(IBLOCK(inum, this->bm->sb.nblocks), buf);
    inode_t *inode = (inode_t *) buf + inum % IPB;
    ino = (inode_t *) malloc(sizeof(inode_t));
    *ino = *inode;
    return ino->type == 0 ? NULL : ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino) {
#ifdef DEBUG
    printf("\tim put_inode, inum: %d\n", inum);
#endif
    char buf[BLOCK_SIZE];
    struct inode *ino_disk;
    if (ino == NULL)
        return;

    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
    ino_disk = (struct inode *) buf + inum % IPB;
    *ino_disk = *ino;
    bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}


/**
 * @param inode : 需要分配新的 block 的 inode
 * @param index : inode 新增加的 block 对应的序号
 */

void
inode_manager::alloc_block_by_index(inode_t *inode, uint32_t index) {
    blockid_t blockid = this->bm->alloc_block();

    if (blockid != -1) {
        if (index < NDIRECT) {
            inode->blocks[index] = blockid;
            return;
        } else {
            if (inode->blocks[NDIRECT] == 0) {
                inode->blocks[NDIRECT] = this->bm->alloc_block();
            }
            char blockid_buf[BLOCK_SIZE];
            this->bm->read_block(inode->blocks[NDIRECT], blockid_buf);
            ((blockid_t *) blockid_buf)[index - NDIRECT] = this->bm->alloc_block();
            this->bm->write_block(inode->blocks[NDIRECT], blockid_buf);
        }
    }
}

/**
 * @param inode : 需要释放 block 的 inode
 * @param index : inode 需要释放的 block 对应的序号
 */
void
inode_manager::free_block_by_index(inode_t *inode, uint32_t index) {
    if (index < NDIRECT) {
        this->bm->free_block(inode->blocks[index]);
        return;
    }
    this->bm->free_block(get_blockid_by_index(inode, index));
}

blockid_t
inode_manager::get_blockid_by_index(inode_t *inode, uint32_t index) {
    if (index < NDIRECT) {
        return inode->blocks[index];
    } else {
        if (inode->blocks[NDIRECT] == 0) {
            return 0;
        }
        char buf[BLOCK_SIZE];
        this->bm->read_block(inode->blocks[NDIRECT], buf);
        return ((blockid_t *) buf)[index - NDIRECT];
    }
}


/* Get all the data of a file by inum.
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size) {
    /*
     * your code goes here.
     * note: read blocks related to inode number inum,
     * and copy them to buf_out
     */
#ifdef DEBUG
    printf("\tim read_file, inum: %d\n", inum);
#endif
    inode_t *inode = this->get_inode(inum);
    if (!inode)
        return;
    char buf[BLOCK_SIZE];
    *size = inode->size;
    *buf_out = (char *) malloc(*size);
    int block_count = inode->size == 0 ? 0: (inode->size - 1) / BLOCK_SIZE + 1;
    for (int i = 0; i < block_count; ++i) {
        this->bm->read_block(this->get_blockid_by_index(inode, i), buf);
        if (i == block_count - 1 && (inode->size) % BLOCK_SIZE) {  // 对最后一个特殊处理
            memcpy(*buf_out + BLOCK_SIZE * i, buf, (inode->size) % BLOCK_SIZE);
        } else {
            memcpy(*buf_out + BLOCK_SIZE * i, buf, BLOCK_SIZE);
        }
    }
    return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size) {
    /*
     * your code goes here.
     * note: write buf to blocks of inode inum.
     * you need to consider the situation when the size of buf
     * is larger or smaller than the size of original inode
     */
#ifdef DEBUG
    printf("\tim write_file inum: %d\n", inum);
#endif
    inode_t *inode = this->get_inode(inum);
    if (!inode)
        return;
    int block_size_before = inode->size == 0 ? 0 : ((inode->size - 1) / BLOCK_SIZE + 1);
    int block_size_after = size == 0 ? 0 : ((size - 1) / BLOCK_SIZE + 1);
    int min_block = MIN(block_size_before, block_size_after);
    int max_block = MAX(block_size_after, block_size_before);

#ifdef DEBUG
    printf("bef: %d, after: %d\n", block_size_before, block_size_after);
#endif
    if (block_size_after > block_size_before) {
        // 需要分配新的block
        for (int i = min_block; i < max_block; ++i) {
            this->alloc_block_by_index(inode, i);
        }
    } else {
        // 需要 free 多出的block
        for (int i = min_block; i < max_block; ++i) {
            this->free_block_by_index(inode, i);
        }
    }
    char temp[BLOCK_SIZE];
    for (int i = 0; i < block_size_after; ++i) {
        if (i == block_size_after - 1 && size % BLOCK_SIZE) {
            memcpy(temp, buf + i * BLOCK_SIZE, size % BLOCK_SIZE);
//#ifdef DEBUG
//            printf("%d ", this->get_blockid_by_index(inode, i));
//#endif
            this->bm->write_block(this->get_blockid_by_index(inode, i), temp);
        } else {
//#ifdef DEBUG
//            printf("%d ", this->get_blockid_by_index(inode, i));
//#endif
            this->bm->write_block(this->get_blockid_by_index(inode, i), buf + i * BLOCK_SIZE);
        }
    }

    inode->size = size;
    uint32_t t = (unsigned int) time(NULL);
    inode->atime = t;
    inode->ctime = t;
    inode->mtime = t;
    this->put_inode(inum, inode);
    free(inode);
}

void
inode_manager::get_attr(uint32_t inum, extent_protocol::attr &a) {
    /*
     * your code goes here.
     * note: get the attributes of inode inum.
     * you can refer to "struct attr" in extent_protocol.h
     */

#ifdef DEBUG
    printf("\tim get_attr inum: %d\n", inum);
#endif
    inode_t *inode = this->get_inode(inum);
    if(!inode) {
        return;
    }
    a.type = inode->type;
    a.ctime = inode->ctime;
    a.mtime = inode->mtime;
    a.atime = inode->atime;
    a.size = inode->size;
}

void
inode_manager::remove_file(uint32_t inum) {
    /*
     * your code goes here
     * note: you need to consider about both the data block and inode of the file
     */
#ifdef DEBUG
    printf("\tim remove_file  inum: %d\n", inum);
#endif
    inode_t *inode = get_inode(inum);
    if (!inode) {
        printf("this inode is not allocated yet\n");
        return;
    }
    int count_of_block = inode->size == 0 ? 0 : (inode->size - 1) / BLOCK_SIZE + 1;
    for (int i = 0; i < count_of_block; ++i) {
        free_block_by_index(inode, i);
    }
    this->bm->free_block(inode->blocks[NDIRECT]);
    free(inode);
    free_inode(inum);

    return;
}
