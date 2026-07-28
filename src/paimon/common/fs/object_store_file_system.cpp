/*
 * Copyright 2026-present Alibaba Inc.
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

#include "paimon/common/fs/object_store_file_system.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <limits>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/utils/math.h"
#include "paimon/common/utils/path_util.h"

namespace paimon {
namespace {

constexpr int64_t kMaxReadAheadMemory = 64LL * 1024LL * 1024LL;
constexpr int64_t kInitialReadAheadSize = 64LL * 1024LL;
constexpr int64_t kMaxReadAheadSize = 8LL * 1024LL * 1024LL;

std::string NormalizeDirectoryPrefix(const std::string& key) {
    if (key.empty() || key.back() == '/') {
        return key;
    }
    return key + "/";
}

class ObjectStoreBasicFileStatus : public BasicFileStatus {
 public:
    ObjectStoreBasicFileStatus(std::string path, bool is_dir)
        : path_(std::move(path)), is_dir_(is_dir) {}
    bool IsDir() const override {
        return is_dir_;
    }
    std::string GetPath() const override {
        return path_;
    }

 private:
    std::string path_;
    bool is_dir_;
};

class ObjectStoreFileStatus : public FileStatus {
 public:
    ObjectStoreFileStatus(std::string path, int64_t size, int64_t modification_time, bool is_dir)
        : path_(std::move(path)),
          size_(size),
          modification_time_(modification_time),
          is_dir_(is_dir) {}
    int64_t GetLen() const override {
        return size_;
    }
    bool IsDir() const override {
        return is_dir_;
    }
    std::string GetPath() const override {
        return path_;
    }
    int64_t GetModificationTime() const override {
        return modification_time_;
    }

 private:
    std::string path_;
    int64_t size_;
    int64_t modification_time_;
    bool is_dir_;
};

class ObjectStoreInputStream : public InputStream {
 public:
    ObjectStoreInputStream(std::shared_ptr<ObjectStoreClient> client,
                           std::shared_ptr<ReadAheadMemoryLimiter> limiter, ObjectStorePath path,
                           std::string uri, int64_t length)
        : client_(std::move(client)),
          limiter_(std::move(limiter)),
          path_(std::move(path)),
          uri_(std::move(uri)),
          length_(length) {}

    ~ObjectStoreInputStream() override {
        CloseInternal();
    }

    Status Seek(int64_t offset, SeekOrigin origin) override {
        std::scoped_lock lock(mutex_);
        if (closed_) {
            return Status::IOError(fmt::format("{} is closed", uri_));
        }
        int64_t base;
        if (origin == FS_SEEK_SET) {
            base = 0;
        } else if (origin == FS_SEEK_CUR) {
            base = position_;
        } else if (origin == FS_SEEK_END) {
            base = length_;
        } else {
            return Status::Invalid("unsupported seek origin");
        }
        if ((offset > 0 && base > std::numeric_limits<int64_t>::max() - offset) ||
            (offset < 0 && base < std::numeric_limits<int64_t>::min() - offset)) {
            return Status::Invalid("object store input position overflows");
        }
        int64_t position = base + offset;
        PAIMON_RETURN_NOT_OK(ValidateValueNonNegative(position, "object store input position"));
        position_ = position;
        return Status::OK();
    }

    Result<int64_t> GetPos() const override {
        std::scoped_lock lock(mutex_);
        if (closed_) {
            return Status::IOError(fmt::format("{} is closed", uri_));
        }
        return position_;
    }

    Result<int64_t> Read(char* buffer, int64_t size) override {
        std::scoped_lock read_lock(read_mutex_);
        int64_t offset;
        {
            std::scoped_lock lock(mutex_);
            if (closed_) {
                return Status::IOError(fmt::format("{} is closed", uri_));
            }
            offset = position_;
        }
        PAIMON_ASSIGN_OR_RAISE(int64_t bytes_read, ReadWithReadAhead(buffer, size, offset));
        {
            std::scoped_lock lock(mutex_);
            position_ += bytes_read;
        }
        return bytes_read;
    }

    Result<int64_t> Read(char* buffer, int64_t size, int64_t offset) override {
        std::scoped_lock read_lock(read_mutex_);
        return ReadWithReadAhead(buffer, size, offset);
    }

    void ReadAsync(char* buffer, int64_t size, int64_t offset,
                   std::function<void(Status)>&& callback) override {
        Status status = ValidateRead(buffer, size, offset);
        if (!status.ok()) {
            callback(std::move(status));
            return;
        }
        if (offset > length_ || size > length_ - offset) {
            callback(Status::Invalid(
                fmt::format("object store async read size {} at offset {} exceeds length {}", size,
                            offset, length_)));
            return;
        }
        if (size == 0) {
            callback(Status::OK());
            return;
        }
        client_->GetObjectRangeAsync(path_, offset, size, buffer,
                                     [callback = std::move(callback)](Status status) mutable {
                                         if (!status.ok()) {
                                             callback(std::move(status));
                                             return;
                                         }
                                         callback(Status::OK());
                                     });
    }

    Status Close() override {
        CloseInternal();
        return Status::OK();
    }
    Result<std::string> GetUri() const override {
        return uri_;
    }
    Result<int64_t> Length() const override {
        return length_;
    }

 private:
    Status ValidateRead(char* buffer, int64_t size, int64_t offset) const {
        PAIMON_RETURN_NOT_OK(ValidateValueNonNegative(size, "read length"));
        PAIMON_RETURN_NOT_OK(ValidateValueNonNegative(offset, "read offset"));
        {
            std::scoped_lock lock(mutex_);
            if (closed_) {
                return Status::IOError(fmt::format("{} is closed", uri_));
            }
        }
        if (size > 0 && buffer == nullptr) {
            return Status::Invalid("read buffer is null");
        }
        return Status::OK();
    }

    void EvictReadAhead() {
        int64_t reserved = 0;
        {
            std::scoped_lock lock(mutex_);
            std::vector<char>().swap(read_ahead_buffer_);
            reserved = reserved_read_ahead_size_;
            reserved_read_ahead_size_ = 0;
        }
        limiter_->Release(reserved);
    }

    Result<int64_t> ReadWithReadAhead(char* buffer, int64_t size, int64_t offset) {
        PAIMON_RETURN_NOT_OK(ValidateRead(buffer, size, offset));
        if (size == 0 || offset >= length_) {
            return 0;
        }
        int64_t read_size = std::min(size, length_ - offset);
        bool continues_previous = false;
        {
            std::scoped_lock lock(mutex_);
            int64_t relative = offset - read_ahead_offset_;
            if (relative >= 0 && relative <= static_cast<int64_t>(read_ahead_buffer_.size()) &&
                read_size <= static_cast<int64_t>(read_ahead_buffer_.size()) - relative) {
                std::memcpy(buffer, read_ahead_buffer_.data() + relative,
                            static_cast<size_t>(read_size));
                return read_size;
            }
            continues_previous =
                !read_ahead_buffer_.empty() &&
                offset == read_ahead_offset_ + static_cast<int64_t>(read_ahead_buffer_.size());
        }
        EvictReadAhead();
        if (read_size >= kMaxReadAheadSize) {
            next_read_ahead_size_ = kInitialReadAheadSize;
            return client_->GetObjectRange(path_, offset, read_size, buffer);
        }
        next_read_ahead_size_ = continues_previous
                                    ? std::min(kMaxReadAheadSize, next_read_ahead_size_ * 2)
                                    : kInitialReadAheadSize;
        int64_t fetch_size = std::min(std::max(read_size, next_read_ahead_size_), length_ - offset);
        fetch_size = limiter_->ReserveUpTo(read_size, fetch_size);
        if (fetch_size == 0) {
            return client_->GetObjectRange(path_, offset, read_size, buffer);
        }
        std::vector<char> fetched(static_cast<size_t>(fetch_size));
        Result<int64_t> result = client_->GetObjectRange(path_, offset, fetch_size, fetched.data());
        if (!result.ok()) {
            limiter_->Release(fetch_size);
            return result.status();
        }
        int64_t bytes_read = std::move(result).value();
        if (bytes_read != fetch_size) {
            limiter_->Release(fetch_size);
            return Status::IOError(fmt::format("range read for {} returned {} bytes, expected {}",
                                               uri_, bytes_read, fetch_size));
        }
        {
            std::scoped_lock lock(mutex_);
            if (closed_) {
                limiter_->Release(fetch_size);
                return Status::IOError(fmt::format("{} is closed", uri_));
            }
            read_ahead_offset_ = offset;
            read_ahead_buffer_ = std::move(fetched);
            reserved_read_ahead_size_ = fetch_size;
            std::memcpy(buffer, read_ahead_buffer_.data(), static_cast<size_t>(read_size));
        }
        return read_size;
    }

    void CloseInternal() {
        int64_t reserved = 0;
        {
            std::scoped_lock lock(mutex_);
            closed_ = true;
            std::vector<char>().swap(read_ahead_buffer_);
            reserved = reserved_read_ahead_size_;
            reserved_read_ahead_size_ = 0;
        }
        limiter_->Release(reserved);
    }

    std::shared_ptr<ObjectStoreClient> client_;
    std::shared_ptr<ReadAheadMemoryLimiter> limiter_;
    ObjectStorePath path_;
    std::string uri_;
    int64_t length_;
    int64_t position_ = 0;
    int64_t read_ahead_offset_ = 0;
    int64_t next_read_ahead_size_ = kInitialReadAheadSize;
    int64_t reserved_read_ahead_size_ = 0;
    std::vector<char> read_ahead_buffer_;
    bool closed_ = false;
    mutable std::mutex read_mutex_;
    mutable std::mutex mutex_;
};

}  // namespace

ReadAheadMemoryLimiter::ReadAheadMemoryLimiter(int64_t limit) : limit_(limit) {}

int64_t ReadAheadMemoryLimiter::ReserveUpTo(int64_t min_size, int64_t max_size) {
    std::scoped_lock lock(mutex_);
    int64_t available = limit_ - used_;
    if (available < min_size) {
        return 0;
    }
    int64_t size = std::min(max_size, available);
    used_ += size;
    return size;
}

void ReadAheadMemoryLimiter::Release(int64_t size) {
    std::scoped_lock lock(mutex_);
    assert(size >= 0 && size <= used_);
    used_ -= std::clamp(size, int64_t{0}, used_);
}

ObjectStoreFileSystem::ObjectStoreFileSystem(std::string scheme,
                                             std::shared_ptr<ObjectStoreClient> client)
    : ObjectStoreFileSystem(std::move(scheme), std::move(client), kMaxReadAheadMemory) {}

ObjectStoreFileSystem::ObjectStoreFileSystem(std::string scheme,
                                             std::shared_ptr<ObjectStoreClient> client,
                                             int64_t read_ahead_memory_limit)
    : scheme_(std::move(scheme)),
      client_(std::move(client)),
      read_ahead_limiter_(std::make_shared<ReadAheadMemoryLimiter>(read_ahead_memory_limit)) {}

Result<ObjectStorePath> ObjectStoreFileSystem::ParsePath(const std::string& path) const {
    PAIMON_ASSIGN_OR_RAISE(Path parsed, PathUtil::ToPath(path));
    if (parsed.scheme != scheme_) {
        return Status::Invalid(fmt::format("path must use {} scheme: {}", scheme_, path));
    }
    if (parsed.authority.empty()) {
        return Status::Invalid(fmt::format("{} path must include bucket: {}", scheme_, path));
    }
    std::string key = parsed.path;
    key.erase(0, key.find_first_not_of('/'));
    return ObjectStorePath{parsed.authority, key};
}

std::string ObjectStoreFileSystem::ToUri(const ObjectStorePath& path, bool is_dir) const {
    std::string uri = scheme_ + "://" + path.bucket + "/";
    uri += path.key;
    if (is_dir && uri.back() != '/') {
        uri += "/";
    }
    return uri;
}

Result<bool> ObjectStoreFileSystem::DirectoryExists(const ObjectStorePath& path) const {
    ObjectStorePath directory{path.bucket, NormalizeDirectoryPrefix(path.key)};
    PAIMON_ASSIGN_OR_RAISE(ListObjectsResult result, client_->ListObjects(directory, "", 1));
    return path.key.empty() || !result.objects.empty() || !result.common_prefixes.empty();
}

Result<std::unique_ptr<InputStream>> ObjectStoreFileSystem::Open(const std::string& path) const {
    PAIMON_ASSIGN_OR_RAISE(ObjectStorePath object_path, ParsePath(path));
    if (object_path.key.empty()) {
        return Status::Invalid(fmt::format("{} is a directory", path));
    }
    Result<ObjectMetadata> metadata = client_->HeadObject(object_path);
    if (!metadata.ok()) {
        if (!metadata.status().IsNotExist()) {
            return metadata.status();
        }
        PAIMON_ASSIGN_OR_RAISE(bool is_dir, DirectoryExists(object_path));
        if (is_dir) {
            return Status::Invalid(fmt::format("{} is a directory", path));
        }
        return metadata.status();
    }
    return std::make_unique<ObjectStoreInputStream>(client_, read_ahead_limiter_, object_path,
                                                    ToUri(object_path), metadata.value().size);
}

Result<std::unique_ptr<FileStatus>> ObjectStoreFileSystem::GetFileStatus(
    const std::string& path) const {
    PAIMON_ASSIGN_OR_RAISE(ObjectStorePath object_path, ParsePath(path));
    if (!object_path.key.empty()) {
        Result<ObjectMetadata> metadata = client_->HeadObject(object_path);
        if (metadata.ok()) {
            return std::make_unique<ObjectStoreFileStatus>(
                ToUri(object_path), metadata.value().size, metadata.value().modification_time,
                false);
        }
        if (!metadata.status().IsNotExist()) {
            return metadata.status();
        }
    }
    PAIMON_ASSIGN_OR_RAISE(bool exists, DirectoryExists(object_path));
    if (!exists) {
        return Status::NotExist(fmt::format("{} does not exist", path));
    }
    return std::make_unique<ObjectStoreFileStatus>(ToUri(object_path, true), 0, 0, true);
}

Status ObjectStoreFileSystem::ListDirectory(
    const ObjectStorePath& path, std::vector<std::unique_ptr<BasicFileStatus>>* basic_statuses,
    std::vector<std::unique_ptr<FileStatus>>* statuses) const {
    if (basic_statuses == nullptr && statuses == nullptr) {
        return Status::Invalid("a destination status list is required");
    }
    ObjectStorePath directory{path.bucket, NormalizeDirectoryPrefix(path.key)};
    std::string token;
    do {
        PAIMON_ASSIGN_OR_RAISE(ListObjectsResult result, client_->ListObjects(directory, token, 0));
        if (result.is_truncated && result.continuation_token.empty()) {
            return Status::IOError(
                fmt::format("truncated listing for {} did not include a continuation token",
                            ToUri(directory, true)));
        }
        for (const auto& object : result.objects) {
            if (object.key == directory.key) {
                continue;
            }
            ObjectStorePath child{path.bucket, object.key};
            if (basic_statuses) {
                basic_statuses->push_back(
                    std::make_unique<ObjectStoreBasicFileStatus>(ToUri(child), false));
            } else {
                statuses->push_back(std::make_unique<ObjectStoreFileStatus>(
                    ToUri(child), object.size, object.modification_time, false));
            }
        }
        for (const auto& prefix : result.common_prefixes) {
            ObjectStorePath child{path.bucket, prefix};
            if (basic_statuses) {
                basic_statuses->push_back(
                    std::make_unique<ObjectStoreBasicFileStatus>(ToUri(child, true), true));
            } else {
                statuses->push_back(
                    std::make_unique<ObjectStoreFileStatus>(ToUri(child, true), 0, 0, true));
            }
        }
        token = result.continuation_token;
        if (!result.is_truncated) {
            break;
        }
    } while (true);
    return Status::OK();
}

Status ObjectStoreFileSystem::ListDir(
    const std::string& directory,
    std::vector<std::unique_ptr<BasicFileStatus>>* file_status_list) const {
    PAIMON_ASSIGN_OR_RAISE(ObjectStorePath path, ParsePath(directory));
    if (!path.key.empty() && path.key.back() != '/') {
        Result<ObjectMetadata> metadata = client_->HeadObject(path);
        if (metadata.ok()) {
            return Status::Invalid(fmt::format("file {} exists and is not a directory", directory));
        }
        if (!metadata.status().IsNotExist()) {
            return metadata.status();
        }
    }
    return ListDirectory(path, file_status_list, nullptr);
}

Status ObjectStoreFileSystem::ListFileStatus(
    const std::string& path, std::vector<std::unique_ptr<FileStatus>>* file_status_list) const {
    PAIMON_ASSIGN_OR_RAISE(ObjectStorePath object_path, ParsePath(path));
    if (!object_path.key.empty() && object_path.key.back() != '/') {
        Result<ObjectMetadata> metadata = client_->HeadObject(object_path);
        if (metadata.ok()) {
            file_status_list->push_back(
                std::make_unique<ObjectStoreFileStatus>(ToUri(object_path), metadata.value().size,
                                                        metadata.value().modification_time, false));
            return Status::OK();
        }
        if (!metadata.status().IsNotExist()) {
            return metadata.status();
        }
    }
    return ListDirectory(object_path, nullptr, file_status_list);
}

Result<bool> ObjectStoreFileSystem::Exists(const std::string& path) const {
    PAIMON_ASSIGN_OR_RAISE(ObjectStorePath object_path, ParsePath(path));
    if (!object_path.key.empty()) {
        Result<ObjectMetadata> metadata = client_->HeadObject(object_path);
        if (metadata.ok()) {
            return true;
        }
        if (!metadata.status().IsNotExist()) {
            return metadata.status();
        }
    }
    return DirectoryExists(object_path);
}

Status ObjectStoreFileSystem::ReadOnlyStatus() const {
    return Status::NotImplemented(fmt::format("{} object store file system is read-only", scheme_));
}

Result<std::unique_ptr<OutputStream>> ObjectStoreFileSystem::Create(const std::string&,
                                                                    bool) const {
    return ReadOnlyStatus();
}

Status ObjectStoreFileSystem::Mkdirs(const std::string&) const {
    return ReadOnlyStatus();
}

Status ObjectStoreFileSystem::Rename(const std::string&, const std::string&) const {
    return ReadOnlyStatus();
}

Status ObjectStoreFileSystem::Delete(const std::string&, bool) const {
    return ReadOnlyStatus();
}

Status ObjectStoreFileSystem::WriteFile(const std::string&, const std::string&, bool) {
    return ReadOnlyStatus();
}

Status ObjectStoreFileSystem::AtomicStore(const std::string&, const std::string&) {
    return ReadOnlyStatus();
}

}  // namespace paimon
