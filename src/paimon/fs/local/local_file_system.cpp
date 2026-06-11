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

#include "paimon/fs/local/local_file_system.h"

#include <fcntl.h>

#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/fs/local/local_file_status.h"

namespace paimon {

Result<bool> LocalFileSystem::Exists(const std::string& path) const {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<LocalFile> file, LocalFile::Create(path));
    return file->Exists();
}

Result<std::unique_ptr<InputStream>> LocalFileSystem::Open(const std::string& path) const {
    PAIMON_ASSIGN_OR_RAISE(bool is_exist, Exists(path));
    if (!is_exist) {
        return Status::NotExist(fmt::format("File '{}' not exists", path));
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<LocalFile> file, LocalFile::Create(path));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<LocalInputStream> in,
                           LocalInputStream::Create(std::move(file)));
    return in;
}

Result<std::unique_ptr<OutputStream>> LocalFileSystem::Create(const std::string& path,
                                                              bool overwrite) const {
    PAIMON_ASSIGN_OR_RAISE(bool is_exist, Exists(path));
    if (is_exist && !overwrite) {
        return Status::Invalid(
            fmt::format("do not allow overwrite, but the file {} already exists", path));
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<LocalFile> file, LocalFile::Create(path));
    std::unique_ptr<LocalFile> parent = file->GetParentFile();
    PAIMON_RETURN_NOT_OK(Mkdirs(parent->GetPath()));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<LocalOutputStream> out,
                           LocalOutputStream::Create(std::move(file)));
    return out;
}

Status LocalFileSystem::Mkdirs(const std::string& path) const {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<LocalFile> file, LocalFile::Create(path));
    return MkdirsInternal(std::move(file));
}

Status LocalFileSystem::MkdirsInternal(std::unique_ptr<LocalFile>&& file) const {
    // Important: The 'Exists()' check above must come before the 'IsDir()'
    // check to be safe when multiple parallel instances try to create the directory
    PAIMON_ASSIGN_OR_RAISE(bool is_exist, file->Exists());
    if (is_exist) {
        PAIMON_ASSIGN_OR_RAISE(bool is_dir, file->IsDir());
        if (is_dir) {
            return Status::OK();
        } else {
            // exists and is not a directory -> is a regular file
            return Status::IOError(
                fmt::format("file {} already exists and is not a directory", file->GetPath()));
        }
    }

    std::unique_ptr<LocalFile> parent = file->GetParentFile();
    if (!parent->IsEmpty()) {
        PAIMON_RETURN_NOT_OK(MkdirsInternal(std::move(parent)));
    }
    PAIMON_ASSIGN_OR_RAISE(bool success, file->Mkdir());
    if (!success) {
        PAIMON_ASSIGN_OR_RAISE(bool is_dir, file->IsDir());
        if (is_dir) {
            return Status::OK();
        } else {
            return Status::IOError(fmt::format("create directory '{}' failed", file->GetPath()));
        }
    }
    return Status::OK();
}

Result<std::unique_ptr<FileStatus>> LocalFileSystem::GetFileStatus(const std::string& path) const {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<LocalFile> file, LocalFile::Create(path));
    PAIMON_ASSIGN_OR_RAISE(bool is_exist, file->Exists());
    if (is_exist) {
        return file->GetFileStatus();
    } else {
        return Status::NotExist(
            fmt::format("File {} does not exist or the user running "
                        "Paimon has insufficient permissions to access it.",
                        file->GetPath()));
    }
}

Status LocalFileSystem::ListDir(
    const std::string& directory,
    std::vector<std::unique_ptr<BasicFileStatus>>* file_status_list) const {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<LocalFile> file, LocalFile::Create(directory));
    PAIMON_ASSIGN_OR_RAISE(bool is_exist, file->Exists());
    if (!is_exist) {
        return Status::OK();
    }
    PAIMON_ASSIGN_OR_RAISE(bool is_file, file->IsFile());
    if (is_file) {
        return Status::IOError(
            fmt::format("file {} already exists and is not a directory", file->GetPath()));
    } else {
        std::vector<std::string> file_list;
        PAIMON_RETURN_NOT_OK(file->List(&file_list));
        file_status_list->reserve(file_status_list->size() + file_list.size());
        for (const auto& f : file_list) {
            Result<std::unique_ptr<FileStatus>> file_status =
                GetFileStatus(PathUtil::JoinPath(directory, f));
            if (!file_status.ok() && !file_status.status().IsNotExist()) {
                return file_status.status();
            } else if (file_status.ok()) {
                file_status_list->emplace_back(std::make_unique<LocalBasicFileStatus>(
                    file_status.value()->GetPath(), file_status.value()->IsDir()));
            }
        }
        return Status::OK();
    }
}

Status LocalFileSystem::ListFileStatus(
    const std::string& path, std::vector<std::unique_ptr<FileStatus>>* file_status_list) const {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<LocalFile> file, LocalFile::Create(path));
    PAIMON_ASSIGN_OR_RAISE(bool is_exist, file->Exists());
    if (!is_exist) {
        return Status::OK();
    }
    PAIMON_ASSIGN_OR_RAISE(bool is_file, file->IsFile());
    if (is_file) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStatus> file_status, file->GetFileStatus());
        file_status_list->emplace_back(std::move(file_status));
    } else {
        std::vector<std::string> file_list;
        PAIMON_RETURN_NOT_OK(file->List(&file_list));
        file_status_list->reserve(file_status_list->size() + file_list.size());
        for (const auto& f : file_list) {
            Result<std::unique_ptr<FileStatus>> file_status =
                GetFileStatus(PathUtil::JoinPath(path, f));
            if (!file_status.ok() && !file_status.status().IsNotExist()) {
                return file_status.status();
            } else if (file_status.ok()) {
                file_status_list->emplace_back(std::move(file_status).value());
            }
        }
    }
    return Status::OK();
}

Status LocalFileSystem::Delete(const std::string& path, bool recursive) const {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<LocalFile> file, LocalFile::Create(path));
    PAIMON_ASSIGN_OR_RAISE(bool is_file, file->IsFile());
    if (is_file) {
        return file->Delete();
    }
    return Delete(std::move(file), recursive);
}

Status LocalFileSystem::Delete(std::unique_ptr<LocalFile>&& file, bool recursive) const {
    PAIMON_ASSIGN_OR_RAISE(bool is_dir, file->IsDir());
    if (is_dir) {
        std::vector<std::unique_ptr<LocalFile>> files;
        PAIMON_RETURN_NOT_OK(file->ListFiles(&files));
        if (recursive == false && !files.empty()) {
            return Status::IOError(
                fmt::format("cannot delete {}, directory is not empty", file->GetPath()));
        }
        for (auto& child : files) {
            PAIMON_RETURN_NOT_OK(Delete(std::move(child)));
        }
    }
    // Now directory is empty
    return file->Delete();
}

Status LocalFileSystem::Rename(const std::string& src, const std::string& dst) const {
    std::string err_msg = fmt::format("rename '{}' to '{}' failed, because: ", src, dst);
    PAIMON_ASSIGN_OR_RAISE(bool is_src_exist, Exists(src));
    if (!is_src_exist) {
        return Status::NotExist(err_msg, "src file not exist");
    }
    PAIMON_ASSIGN_OR_RAISE(bool is_dst_exist, Exists(dst));
    if (is_dst_exist) {
        return Status::Invalid(err_msg, "dst file already exist");
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<LocalFile> src_file, LocalFile::Create(src));
    PAIMON_ASSIGN_OR_RAISE(bool is_file, src_file->IsFile());
    std::string new_file_name = dst;

    if (is_file && new_file_name[new_file_name.length() - 1] == '/') {
        return Status::Invalid(err_msg, "src file is not a dir");
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<LocalFile> dst_file, LocalFile::Create(dst));
    std::unique_ptr<LocalFile> parent = dst_file->GetParentFile();
    PAIMON_RETURN_NOT_OK(Mkdirs(parent->GetPath()));
    if (::rename(src_file->GetPath().c_str(), dst_file->GetPath().c_str()) != 0) {
        int32_t cur_errno = errno;
        return Status::IOError(err_msg, std::strerror(cur_errno));
    }
    return Status::OK();
}

// input stream
Result<std::unique_ptr<LocalInputStream>> LocalInputStream::Create(
    std::unique_ptr<LocalFile> file) {
    PAIMON_RETURN_NOT_OK(file->OpenFile(/*is_read_file=*/true));
    return std::unique_ptr<LocalInputStream>(new LocalInputStream(std::move(file)));
}

LocalInputStream::LocalInputStream(std::unique_ptr<LocalFile>&& file) : file_(std::move(file)) {}

Status LocalInputStream::Seek(int64_t offset, SeekOrigin origin) {
    if (origin != FS_SEEK_SET && origin != FS_SEEK_CUR && origin != FS_SEEK_END) {
        return Status::Invalid(
            "invalid SeekOrigin, only support FS_SEEK_SET, FS_SEEK_CUR, and FS_SEEK_END");
    }
    auto convert_origin = [](SeekOrigin origin) -> int32_t {
        switch (origin) {
            case FS_SEEK_SET:
                return SEEK_SET;
            case FS_SEEK_CUR:
                return SEEK_CUR;
            case FS_SEEK_END:
                return SEEK_END;
            default:
                return SEEK_SET;
        }
    };
    return file_->Seek(offset, convert_origin(origin));
}

Result<int64_t> LocalInputStream::GetPos() const {
    return file_->Tell();
}

Result<int64_t> LocalInputStream::Read(char* buffer, int64_t size) {
    PAIMON_ASSIGN_OR_RAISE(int64_t read_length, file_->Read(buffer, size));
    if (read_length != size) {
        return Status::IOError(fmt::format("file '{}' read size {} != expected {}",
                                           file_->GetPath(), read_length, size));
    }
    return read_length;
}

Result<int64_t> LocalInputStream::Read(char* buffer, int64_t size, int64_t offset) {
    PAIMON_ASSIGN_OR_RAISE(int64_t read_length, file_->Read(buffer, size, offset));
    if (read_length != size) {
        return Status::IOError(fmt::format("file '{}' read size {} != expected {}",
                                           file_->GetPath(), read_length, size));
    }
    return read_length;
}

void LocalInputStream::ReadAsync(char* buffer, int64_t size, int64_t offset,
                                 std::function<void(Status)>&& callback) {
    Result<int64_t> read_size = Read(buffer, size, offset);
    Status status = Status::OK();
    if (!read_size.ok()) {
        status = read_size.status();
    } else {
        assert(read_size.value() == size);
    }
    callback(status);
}

Result<int64_t> LocalInputStream::Length() const {
    return file_->Length();
}

Status LocalInputStream::Close() {
    return file_->Close();
}

// output stream
Result<std::unique_ptr<LocalOutputStream>> LocalOutputStream::Create(
    std::unique_ptr<LocalFile> file) {
    PAIMON_RETURN_NOT_OK(file->OpenFile(/*is_read_file=*/false));
    return std::unique_ptr<LocalOutputStream>(new LocalOutputStream(std::move(file)));
}

LocalOutputStream::LocalOutputStream(std::unique_ptr<LocalFile>&& file) : file_(std::move(file)) {}

Result<int64_t> LocalOutputStream::GetPos() const {
    return file_->Tell();
}
Result<int64_t> LocalOutputStream::Write(const char* buffer, int64_t size) {
    return file_->Write(buffer, size);
}
Status LocalOutputStream::Flush() {
    return file_->Flush();
}
Status LocalOutputStream::Close() {
    return file_->Close();
}

}  // namespace paimon
