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

#include "paimon/fs/jindo/jindo_file_system.h"

#include <cassert>
#include <utility>

#include "JdoFileInfo.hpp"    // NOLINT(build/include_subdir)
#include "JdoFileSystem.hpp"  // NOLINT(build/include_subdir)
#include "JdoListResult.hpp"  // NOLINT(build/include_subdir)
#include "JdoStatus.hpp"      // NOLINT(build/include_subdir)
#include "fmt/format.h"
#include "jdo_error.h"  // NOLINT(build/include_subdir)
#include "paimon/fs/jindo/jindo_file_status.h"
#include "paimon/fs/jindo/jindo_utils.h"

namespace paimon::jindo {

class JindoFileSystemImpl {
 public:
    explicit JindoFileSystemImpl(std::unique_ptr<JdoFileSystem>&& fs) : fs_(std::move(fs)) {
        assert(fs_);
    }
    ~JindoFileSystemImpl() {
        if (fs_) {
            [[maybe_unused]] auto status = fs_->destroy();
            assert(status.ok());
            fs_.reset();
        }
    }
    JdoFileSystem* GetFileSystem() {
        assert(fs_);
        return fs_.get();
    }

 private:
    std::unique_ptr<JdoFileSystem> fs_;
};

JindoFileSystem::JindoFileSystem(std::unique_ptr<JdoFileSystem>&& fs)
    : impl_(std::make_shared<JindoFileSystemImpl>(std::move(fs))) {}

Result<std::unique_ptr<InputStream>> JindoFileSystem::Open(const std::string& path) const {
    std::unique_ptr<JdoReader> reader;
    PAIMON_RETURN_NOT_OK_FROM_JINDO(impl_->GetFileSystem()->openReader(path, &reader));
    return std::make_unique<JindoInputStream>(impl_, std::move(reader));
}

Result<std::unique_ptr<OutputStream>> JindoFileSystem::Create(const std::string& path,
                                                              bool overwrite) const {
    PAIMON_ASSIGN_OR_RAISE(bool exist, Exists(path));
    if (exist && !overwrite) {
        return Status::Invalid(
            fmt::format("do not allow overwrite, but the file {} already exists", path));
    }
    std::unique_ptr<JdoWriter> writer;
    PAIMON_RETURN_NOT_OK_FROM_JINDO(impl_->GetFileSystem()->openWriter(path, &writer));
    return std::make_unique<JindoOutputStream>(impl_, std::move(writer));
}

Status JindoFileSystem::Mkdirs(const std::string& path) const {
    PAIMON_RETURN_NOT_OK_FROM_JINDO(impl_->GetFileSystem()->mkdir(path, /*recursive=*/true));
    return Status::OK();
}

Status JindoFileSystem::Rename(const std::string& src, const std::string& dst) const {
    PAIMON_ASSIGN_OR_RAISE(bool is_src_exist, Exists(src));
    if (!is_src_exist) {
        return Status::NotExist(
            fmt::format("rename {} to {} failed, because: src file not exist", src, dst));
    }
    PAIMON_ASSIGN_OR_RAISE(bool is_dst_exist, Exists(dst));
    if (is_dst_exist) {
        return Status::Invalid(
            fmt::format("rename {} to {} failed, because: dst file already exist", src, dst));
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStatus> file_status, GetFileStatus(src));
    if (!file_status->IsDir() && dst.back() == '/') {
        return Status::Invalid(
            fmt::format("rename {} to {} failed, because: src file is not a dir", src, dst));
    }
    PAIMON_RETURN_NOT_OK_FROM_JINDO(impl_->GetFileSystem()->rename(src, dst));
    return Status::OK();
}

Result<bool> JindoFileSystem::Exists(const std::string& path) const {
    auto status = impl_->GetFileSystem()->exists(path);
    if (status.ok()) {
        return true;
    } else if (status.getErrCode() == JDO_FILE_NOT_FOUND_ERROR) {
        return false;
    }
    return Status::IOError(status.errMsg());
}

Status JindoFileSystem::Delete(const std::string& path, bool recursive) const {
    PAIMON_RETURN_NOT_OK_FROM_JINDO(impl_->GetFileSystem()->remove(path, recursive));
    return Status::OK();
}

Result<std::unique_ptr<FileStatus>> JindoFileSystem::GetFileStatus(const std::string& path) const {
    JdoFileInfo file_info;
    PAIMON_RETURN_NOT_OK_FROM_JINDO(impl_->GetFileSystem()->getFileInfo(path, &file_info));
    return std::make_unique<JindoFileStatus>(std::move(file_info));
}

Status JindoFileSystem::ListDir(
    const std::string& directory,
    std::vector<std::unique_ptr<BasicFileStatus>>* file_status_list) const {
    PAIMON_ASSIGN_OR_RAISE(bool exist, Exists(directory));
    if (!exist) {
        return Status::OK();
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStatus> file_status, GetFileStatus(directory));
    if (!file_status->IsDir()) {
        return Status::Invalid(
            fmt::format("file {} already exists and is not a directory", file_status->GetPath()));
    }
    JdoListResult list_result;
    while (true) {
        PAIMON_RETURN_NOT_OK_FROM_JINDO(
            impl_->GetFileSystem()->listDir(directory, /*recursive=*/false, &list_result));
        auto file_infos = list_result.getFileInfos();
        file_status_list->reserve(file_status_list->size() + file_infos.size());
        for (auto& file_info : file_infos) {
            file_status_list->push_back(
                std::make_unique<JindoBasicFileStatus>(std::move(file_info)));
        }
        if (!list_result.isTruncated()) {
            break;
        }
        // TODO(xinyu.lxy): test truncated
        list_result.setFileInfos({});
        list_result.setTruncated(false);
    }
    return Status::OK();
}

Status JindoFileSystem::ListFileStatus(
    const std::string& path, std::vector<std::unique_ptr<FileStatus>>* file_status_list) const {
    PAIMON_ASSIGN_OR_RAISE(bool exist, Exists(path));
    if (!exist) {
        return Status::OK();
    }

    JdoListResult list_result;
    while (true) {
        PAIMON_RETURN_NOT_OK_FROM_JINDO(
            impl_->GetFileSystem()->listDir(path, /*recursive=*/false, &list_result));
        auto file_infos = list_result.getFileInfos();
        file_status_list->reserve(file_status_list->size() + file_infos.size());
        for (auto& file_info : file_infos) {
            // oss not support list FileStatus, only return BasicFileStatus
            // call GetFileStatus to return FileStatus
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStatus> file_status,
                                   GetFileStatus(file_info.getPath()));
            file_status_list->push_back(std::move(file_status));
        }
        if (!list_result.isTruncated()) {
            break;
        }
        list_result.setFileInfos({});
        list_result.setTruncated(false);
    }
    return Status::OK();
}

JindoInputStream::JindoInputStream(const std::shared_ptr<JindoFileSystemImpl>& fs,
                                   std::unique_ptr<JdoReader>&& reader)
    : fs_(fs), reader_(std::move(reader)) {}

Status JindoInputStream::Seek(int64_t offset, SeekOrigin origin) {
    if (origin == FS_SEEK_SET) {
        PAIMON_RETURN_NOT_OK_FROM_JINDO(reader_->seek(offset));
    } else if (origin == FS_SEEK_CUR) {
        PAIMON_ASSIGN_OR_RAISE(int64_t pos, GetPos());
        PAIMON_RETURN_NOT_OK_FROM_JINDO(reader_->seek(offset + pos));
    } else if (origin == FS_SEEK_END) {
        PAIMON_ASSIGN_OR_RAISE(uint64_t len, Length());
        PAIMON_RETURN_NOT_OK_FROM_JINDO(reader_->seek(len + offset));
    } else {
        return Status::Invalid("unsupported seek origin");
    }
    return Status::OK();
}

Result<int64_t> JindoInputStream::GetPos() const {
    int64_t pos = -1;
    PAIMON_RETURN_NOT_OK_FROM_JINDO(reader_->tell(pos));
    return pos;
}

Result<uint64_t> JindoInputStream::Length() const {
    int64_t len = -1;
    PAIMON_RETURN_NOT_OK_FROM_JINDO(reader_->getFileLength(len));
    return len;
}

Result<int32_t> JindoInputStream::Read(char* buffer, uint32_t size) {
    PAIMON_RETURN_NOT_OK_FROM_JINDO(reader_->read(size, &result_, buffer));
    return result_.length();
}

Result<int32_t> JindoInputStream::Read(char* buffer, uint32_t size, uint64_t offset) {
    PAIMON_RETURN_NOT_OK_FROM_JINDO(reader_->pread(offset, size, &result_, buffer));
    return result_.length();
}

void JindoInputStream::ReadAsync(char* buffer, uint32_t size, uint64_t offset,
                                 std::function<void(Status)>&& callback) {
    auto outer_callback = [=](JdoStatus status) {
        callback(status.ok() ? Status::OK() : Status::IOError(status.errMsg()));
    };
    auto task = reader_->preadAsync(offset, size, &result_, buffer, outer_callback);
    assert(task);
    auto status = task->perform();
}

Status JindoInputStream::Close() {
    PAIMON_RETURN_NOT_OK_FROM_JINDO(reader_->close());
    return Status::OK();
}

Result<std::string> JindoInputStream::GetUri() const {
    std::string_view uri;
    PAIMON_RETURN_NOT_OK_FROM_JINDO(reader_->name(&uri));
    return std::string(uri);
}

JindoOutputStream::JindoOutputStream(const std::shared_ptr<JindoFileSystemImpl>& fs,
                                     std::unique_ptr<JdoWriter>&& writer)
    : fs_(fs), writer_(std::move(writer)) {}

Result<int64_t> JindoOutputStream::GetPos() const {
    int64_t pos = -1;
    PAIMON_RETURN_NOT_OK_FROM_JINDO(writer_->tell(pos));
    return pos;
}

Result<int32_t> JindoOutputStream::Write(const char* buffer, uint32_t size) {
    std::string_view data(buffer, size);
    PAIMON_RETURN_NOT_OK_FROM_JINDO(writer_->write(data));
    return size;
}

Status JindoOutputStream::Flush() {
    PAIMON_RETURN_NOT_OK_FROM_JINDO(writer_->flush());
    return Status::OK();
}

Status JindoOutputStream::Close() {
    PAIMON_RETURN_NOT_OK_FROM_JINDO(writer_->close());
    return Status::OK();
}

Result<std::string> JindoOutputStream::GetUri() const {
    std::string_view name;
    PAIMON_RETURN_NOT_OK_FROM_JINDO(writer_->name(&name));
    return std::string(name);
}

}  // namespace paimon::jindo
