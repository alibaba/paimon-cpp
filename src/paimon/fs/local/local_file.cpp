/*
 * Copyright 2024-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/fs/local/local_file.h"

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/fs/local/local_file_status.h"

namespace paimon {

// TODO(yonghao.fyh): move io_hook.h to test/test_util and add a HookLocalFileSystem only for test
#define CHECK_HOOK()                             \
    if (hook_) {                                 \
        PAIMON_RETURN_NOT_OK(hook_->Try(path_)); \
    }

LocalFile::LocalFile(const std::string& path) : path_(path), hook_(IOHook::GetInstance()) {}

Result<bool> LocalFile::Exists() const {
    CHECK_HOOK();
    if (access(path_.c_str(), F_OK) == 0) {
        return true;
    } else if (errno == ENOENT) {
        return false;
    }
    int32_t cur_errno = errno;
    return Status::IOError(
        fmt::format("path '{}' check exists fail, ec: {}", path_, std::strerror(cur_errno)));
}

Result<bool> LocalFile::IsFile() const {
    CHECK_HOOK();
    bool is_file = false;
    struct stat buf;
    if (stat(path_.c_str(), &buf) < 0) {
        return Status::IOError(
            fmt::format("path '{}' check isFile fail, ec: {}", path_, std::strerror(errno)));
    }
    if (S_ISREG(buf.st_mode)) {
        is_file = true;
    }
    return is_file;
}

Result<bool> LocalFile::IsDir() const {
    CHECK_HOOK();
    PAIMON_ASSIGN_OR_RAISE(auto file_status, GetFileStatus());
    return file_status->IsDir();
}

Status LocalFile::List(std::vector<std::string>* file_list) const {
    CHECK_HOOK();
    file_list->clear();
    DIR* dp;
    struct dirent* ep;
    dp = opendir(path_.c_str());
    if (dp == nullptr) {
        int32_t cur_errno = errno;
        return Status::IOError(
            fmt::format("list path '{}' fail, ec: {}", path_, std::strerror(cur_errno)));
    }

    while ((ep = readdir(dp)) != nullptr) {
        if (strcmp(ep->d_name, ".") == 0 || strcmp(ep->d_name, "..") == 0) {
            continue;
        }
        file_list->push_back(ep->d_name);
    }
    if (closedir(dp) < 0) {
        file_list->clear();
        int32_t cur_errno = errno;
        return Status::IOError(
            fmt::format("list path '{}' fail, ec: {}", path_, std::strerror(cur_errno)));
    }
    return Status::OK();
}

Status LocalFile::ListFiles(std::vector<LocalFile>* file_list) const {
    CHECK_HOOK();
    file_list->clear();
    std::vector<std::string> file_names;
    PAIMON_RETURN_NOT_OK(List(&file_names));
    for (const auto& file_name : file_names) {
        file_list->emplace_back(PathUtil::JoinPath(path_, file_name));
    }
    return Status::OK();
}

Status LocalFile::Delete() const {
    CHECK_HOOK();
    PAIMON_ASSIGN_OR_RAISE(bool is_exist, Exists());
    if (is_exist) {
        if (::remove(path_.c_str()) != 0) {
            if (errno != ENOENT) {
                return Status::IOError(
                    fmt::format("delete path '{}' fail, ec: {}", path_, std::strerror(errno)));
            }
        }
    }
    return Status::OK();
}

Result<bool> LocalFile::Mkdir() const {
    CHECK_HOOK();
    return mkdir(path_.c_str(), 0755) == 0;
}

Result<std::unique_ptr<LocalFileStatus>> LocalFile::GetFileStatus() const {
    CHECK_HOOK();
    struct stat buf;
    if (stat(path_.c_str(), &buf) < 0) {
        int32_t cur_errno = errno;
        return Status::IOError(
            fmt::format("get file '{}' status failed, ec: {}", path_, std::strerror(cur_errno)));
    }
    return std::make_unique<LocalFileStatus>(path_, buf.st_size, buf.st_mtime * 1000,
                                             S_ISDIR(buf.st_mode));
}

Result<uint64_t> LocalFile::Length() const {
    CHECK_HOOK();
    PAIMON_ASSIGN_OR_RAISE(auto file_status, GetFileStatus());
    return file_status->GetLen();
}

Result<int64_t> LocalFile::LastModifiedTimeMs() const {
    CHECK_HOOK();
    PAIMON_ASSIGN_OR_RAISE(auto file_status, GetFileStatus());
    return file_status->GetModificationTime();
}

LocalFile LocalFile::GetParentFile() const {
    size_t pos = path_.rfind('/');
    if (pos == std::string::npos) {
        return LocalFile("");
    } else {
        std::string parent_dir = path_.substr(0, pos);
        return LocalFile(parent_dir);
    }
}

const std::string& LocalFile::GetAbsolutePath() const {
    return path_;
}

Result<int32_t> LocalFile::Read(char* buffer, uint32_t length, uint64_t offset) {
    if (file_) {
        CHECK_HOOK();
        int32_t fd = fileno(file_);
        auto more = static_cast<int32_t>(length);
        if (more < 0) {
            return Status::IOError(fmt::format(
                "pread file '{}' fail, length overflow int32_t, ec: EC_BADARGS", path_));
        }

        uint64_t off = 0;
        int32_t ret = 0;
        while (more > 0) {
            ret = ::pread(fd, buffer + off, more, offset + off);
            if (ret == -1) {
                return Status::IOError(
                    fmt::format("pread file '{}' fail at off {}, with error {}, ec: {}", path_, off,
                                strerror(errno), std::strerror(errno)));
            }
            if (ret == 0) {
                break;
            }
            more -= ret;
            off += ret;
        }
        return off;
    }
    return Status::IOError(fmt::format(
        "read file '{}' fail, can not read file which is opened fail, ec: EBADF", path_));
}

Result<int32_t> LocalFile::Read(char* buffer, uint32_t length) {
    if (file_) {
        CHECK_HOOK();
        auto more = static_cast<int32_t>(length);
        if (more < 0) {
            return Status::IOError(
                fmt::format("fileName '{}', length '{}', ec: EC_BADARGS", path_, length));
        }

        int32_t ret = 0;
        uint64_t off = 0;
        while (more > 0) {
            ret = fread(buffer + off, 1, more, file_);
            if (ferror(file_) != 0) {
                return Status::IOError(
                    fmt::format("read file '{}' fail at off {}, with error {}, ec: {}", path_, off,
                                strerror(errno), std::strerror(errno)));
            }
            more -= ret;
            off += ret;
            if (feof(file_)) {
                break;
            }
        }
        return off;
    }

    return Status::IOError(fmt::format(
        "read file '{}' fail, can not read file which is opened fail, ec: EBADF", path_));
}

Result<int32_t> LocalFile::Write(const char* buffer, uint32_t length) {
    if (file_) {
        CHECK_HOOK();
        auto more = static_cast<int32_t>(length);
        if (more < 0) {
            return Status::IOError(fmt::format(
                "write file '{}' fail, length overflow int32_t, ec: EC_BADARGS", path_));
        }

        int32_t ret = 0;
        uint64_t off = 0;
        while (more > 0) {
            ret = fwrite(buffer + off, 1, more, file_);
            if (ferror(file_) != 0) {
                return Status::IOError(fmt::format("write file '{}' fail, with error {}, ec: {}",
                                                   path_, off, strerror(errno),
                                                   std::strerror(errno)));
            }
            more -= ret;
            off += ret;
        }
        return off;
    }

    return Status::IOError(
        fmt::format("write file '{}' fail, can not write file which not opened, ec: EBADF", path_));
}

Status LocalFile::Flush() {
    if (file_) {
        CHECK_HOOK();
        int32_t ret = fflush(file_);
        if (0 == ret) {
            CHECK_HOOK();
            int32_t fd = fileno(file_);
            ret |= fsync(fd);
        }
        if (0 != ret) {
            return Status::IOError(
                fmt::format("flush '{}' fail, ec: {}", path_, std::strerror(errno)));
        }
        return Status::OK();
    }
    return Status::IOError(
        fmt::format("flush '{}' fail, can not flush file which not opened, ec: EBADF", path_));
}

Status LocalFile::OpenFile(bool is_read_file) {
    if (is_read_file) {
        PAIMON_ASSIGN_OR_RAISE(bool is_exist, Exists());
        if (!is_exist) {
            return Status::IOError(
                fmt::format("direct openFile '{}' fail, file not exist, ec: ENOENT", path_));
        }
        PAIMON_ASSIGN_OR_RAISE(bool is_dir, IsDir());
        if (is_dir) {
            return Status::IOError(fmt::format(
                "direct openFile '{}' fail, cannot open a directory, ec: EISDIR", path_));
        }
        CHECK_HOOK();
        file_ = fopen(path_.c_str(), "r");
    } else {
        CHECK_HOOK();
        file_ = fopen(path_.c_str(), "w");
    }
    if (file_ == nullptr) {
        return Status::IOError(fmt::format("open '{}' fail, ec: {}", path_, std::strerror(errno)));
    }
    return Status::OK();
}

Status LocalFile::Close() {
    if (file_) {
        CHECK_HOOK();
        if (fclose(file_) != 0) {
            file_ = nullptr;
            return Status::IOError(
                fmt::format("close '{}' fail, ec: {}", path_, std::strerror(errno)));
        }
        file_ = nullptr;
    }
    return Status::OK();
}

Status LocalFile::Seek(int64_t offset, int32_t seek_origin) {
    if (file_) {
        CHECK_HOOK();
        int32_t ret = 0;
        ret = fseek(file_, offset, seek_origin);
        if (ret != 0) {
            return Status::IOError(
                fmt::format("seek '{}' fail, ec: {}", path_, std::strerror(errno)));
        }
        return Status::OK();
    }
    return Status::IOError(
        fmt::format("seek '{}' fail, can not read file which not opened, ec: EBADF", path_));
}

Result<int64_t> LocalFile::Tell() const {
    if (file_) {
        CHECK_HOOK();
        int64_t ret = ftell(file_);
        if (ret < 0) {
            return Status::IOError(
                fmt::format("tell '{}' fail, ec: {}", path_, std::strerror(errno)));
        }
        return ret;
    }
    return Status::IOError(
        fmt::format("tell '{}' fail, can not read file which not opened, ec: EBADF", path_));
}

}  // namespace paimon
