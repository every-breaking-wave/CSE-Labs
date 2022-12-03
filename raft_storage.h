#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

template <typename command> class raft_storage {
public:
    raft_storage(const std::string &file_dir);
    ~raft_storage();

    bool updateMetadata(int term, int vote);
    bool updateSnapshot(const std::vector<char> &snapshot);
    bool updateLog(const std::vector<log_entry<command>> &log);

    bool appendLog(const log_entry<command> &log, int new_size);
    bool appendLog(const std::vector<log_entry<command>> &log, int new_size);

    bool updateTotal(int term, int vote, const std::vector<log_entry<command>> &log, const std::vector<char> &snapshot);
    bool restore(int &term, int &vote, std::vector<log_entry<command>> &log, std::vector<char> &snapshot);

private:
    std::mutex mtx;
    std::string m_metadata;
    std::string m_log;
    std::string m_snapshot;

    char *buf;
    int buf_size;
};

template <typename command> raft_storage<command>::raft_storage(const std::string &dir) {
    m_metadata = dir + "/metadata";
    m_log = dir + "/log";
    m_snapshot = dir + "/snapshot";
    buf_size = 16;
    buf = new char[buf_size];
}

template <typename command> raft_storage<command>::~raft_storage() { delete[] buf; }

template <typename command> bool raft_storage<command>::updateMetadata(int term, int vote) {
    std::unique_lock<std::mutex> lock(mtx);

    std::fstream fs(m_metadata, std::ios::out | std::ios::trunc | std::ios::binary);
    if (fs.fail()) {
        return false;
    }

    fs.write((const char *)&term, sizeof(int));
    fs.write((const char *)&vote, sizeof(int));

    fs.close();

    return true;
}

template <typename command> bool raft_storage<command>::updateLog(const std::vector<log_entry<command>> &log) {
    std::unique_lock<std::mutex> lock(mtx);

    std::fstream fs(m_log, std::ios::out | std::ios::trunc | std::ios::binary);
    if (fs.fail()) {
        return false;
    }

    int size = log.size();
    fs.write((const char *)&size, sizeof(int));

    for (const log_entry<command> &entry : log) {
        fs.write((const char *)&entry.index, sizeof(int));
        fs.write((const char *)&entry.term, sizeof(int));

        size = entry.cmd.size();
        fs.write((const char *)&size, sizeof(int));
        if (size > buf_size) {
            delete[] buf;
            buf_size = std::max(size, 2 * buf_size);
            buf = new char[buf_size];
        }

        entry.cmd.serialize(buf, size);
        fs.write(buf, size);
    }

    fs.close();

    return true;
}

template <typename command> bool raft_storage<command>::appendLog(const log_entry<command> &entry, int new_size) {
    std::unique_lock<std::mutex> lock(mtx);

    std::fstream fs(m_log, std::ios::out | std::ios::in | std::ios::binary);
    if (fs.fail()) {
        return false;
    }

    int size = 0;
    fs.seekp(0, std::ios::end);
    fs.write((const char *)&entry.index, sizeof(int));
    fs.write((const char *)&entry.term, sizeof(int));

    size = entry.cmd.size();
    fs.write((const char *)&size, sizeof(int));
    if (size > buf_size) {
        delete[] buf;
        buf_size = std::max(size, 2 * buf_size);
        buf = new char[buf_size];
    }

    entry.cmd.serialize(buf, size);
    fs.write(buf, size);

    fs.seekp(0, std::ios::beg);
    fs.write((const char *)&new_size, sizeof(int));

    fs.close();

    return true;
}

template <typename command>
bool raft_storage<command>::appendLog(const std::vector<log_entry<command>> &log, int new_size) {
    std::unique_lock<std::mutex> lock(mtx);

    std::fstream fs(m_log, std::ios::out | std::ios::in | std::ios::binary);
    if (fs.fail()) {
        return false;
    }

    int size = 0;
    fs.seekp(0, std::ios::end);
    for (const log_entry<command> &entry : log) {
        fs.write((const char *)&entry.index, sizeof(int));
        fs.write((const char *)&entry.term, sizeof(int));

        size = entry.cmd.size();
        fs.write((const char *)&size, sizeof(int));
        if (size > buf_size) {
            delete[] buf;
            buf_size = std::max(size, 2 * buf_size);
            buf = new char[buf_size];
        }

        entry.cmd.serialize(buf, size);
        fs.write(buf, size);
    }

    fs.seekp(0, std::ios::beg);
    fs.write((const char *)&new_size, sizeof(int));

    fs.close();

    return true;
}

template <typename command> bool raft_storage<command>::updateSnapshot(const std::vector<char> &snapshot) {
    std::unique_lock<std::mutex> lock(mtx);

    std::fstream fs(m_snapshot, std::ios::out | std::ios::trunc | std::ios::binary);
    if (fs.fail()) {
        return false;
    }

    int size = snapshot.size();
    fs.write((const char *)&size, sizeof(int));
    fs.write(snapshot.data(), size);

    fs.close();

    return true;
}

template <typename command>
bool raft_storage<command>::updateTotal(int term, int vote, const std::vector<log_entry<command>> &log,
                                        const std::vector<char> &snapshot) {
    if (!updateMetadata(term, vote)) {
        return false;
    }
    if (!updateLog(log)) {
        return false;
    }
    if (!updateSnapshot(snapshot)) {
        return false;
    }

    return true;
}

template <typename command>
bool raft_storage<command>::restore(int &term, int &vote, std::vector<log_entry<command>> &log,
                                    std::vector<char> &snapshot) {
    std::unique_lock<std::mutex> lock(mtx);

    std::fstream fs;
    fs.open(m_metadata, std::ios::in | std::ios::binary);
    if (fs.fail() || fs.eof()) { // no file or empty file
        return false;
    }

    fs.read((char *)&term, sizeof(int));
    fs.read((char *)&vote, sizeof(int));

    fs.close();
    fs.open(m_log, std::ios::in | std::ios::binary);
    if (fs.fail() || fs.eof()) { // no file or empty file
        return false;
    }

    int size = 0;
    fs.read((char *)&size, sizeof(int));
    log.resize(size);

    for (log_entry<command> &entry : log) {
        fs.read((char *)&entry.index, sizeof(int));
        fs.read((char *)&entry.term, sizeof(int));

        fs.read((char *)&size, sizeof(int));
        if (size > buf_size) {
            delete[] buf;
            buf_size = std::max(size, 2 * buf_size);
            buf = new char[buf_size];
        }

        fs.read(buf, size);
        entry.cmd.deserialize(buf, size);
    }

    fs.close();
    fs.open(m_snapshot, std::ios::in | std::ios::binary);
    if (fs.fail() || fs.eof()) { // no file or empty file
        return false;
    }

    fs.read((char *)&size, sizeof(int));
    snapshot.resize(size);

    for (char &c : snapshot) {
        fs.read(&c, sizeof(char));
    }

    fs.close();

    return true;
}

#endif // raft_storage_h