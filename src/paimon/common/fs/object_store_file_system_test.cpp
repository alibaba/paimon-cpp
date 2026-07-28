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

#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <limits>
#include <map>
#include <utility>
#include <vector>

#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

using Range = std::pair<int64_t, int64_t>;

class MockObjectStoreClient : public ObjectStoreClient {
 public:
    Result<ObjectMetadata> HeadObject(const ObjectStorePath& path) const override {
        ++head_calls_;
        if (!head_error_.ok()) {
            return head_error_;
        }
        auto iter = objects_.find(path.key);
        if (iter == objects_.end()) {
            return Status::NotExist("not found");
        }
        return ObjectMetadata{path.key, static_cast<int64_t>(iter->second.size()), 1};
    }

    Result<ListObjectsResult> ListObjects(const ObjectStorePath& path, const std::string& token,
                                          int32_t) const override {
        ++list_calls_;
        if (!list_error_.ok()) {
            return list_error_;
        }
        if (!pages_.empty()) {
            return pages_.at(token);
        }
        ListObjectsResult result;
        for (const auto& [key, value] : objects_) {
            if (key.rfind(path.key, 0) == 0) {
                result.objects.push_back(
                    ObjectMetadata{key, static_cast<int64_t>(value.size()), 1});
            }
        }
        return result;
    }

    Result<int64_t> GetObjectRange(const ObjectStorePath& path, int64_t offset, int64_t size,
                                   char* buffer) const override {
        ranges_.emplace_back(offset, size);
        const std::string& value = objects_.at(path.key);
        int64_t available = std::min(size, static_cast<int64_t>(value.size()) - offset);
        if (short_read_) {
            --available;
        }
        std::memcpy(buffer, value.data() + offset, available);
        return available;
    }

    void GetObjectRangeAsync(const ObjectStorePath& path, int64_t offset, int64_t size,
                             char* buffer, std::function<void(Status)>&& callback) const override {
        Result<int64_t> result = GetObjectRange(path, offset, size, buffer);
        callback(result.ok() && result.value() == size ? Status::OK()
                                                       : Status::IOError("short read"));
    }

    std::map<std::string, std::string> objects_;
    std::map<std::string, ListObjectsResult> pages_;
    Status head_error_ = Status::OK();
    Status list_error_ = Status::OK();
    mutable int32_t list_calls_ = 0;
    mutable int32_t head_calls_ = 0;
    mutable std::vector<Range> ranges_;
    bool short_read_ = false;
};

TEST(ObjectStoreFileSystemTest, TestObjectWinsOverPrefix) {
    auto client = std::make_shared<MockObjectStoreClient>();
    client->objects_["foo"] = "file";
    client->objects_["foo/bar"] = "child";
    ObjectStoreFileSystem fs("s3", client);
    ASSERT_OK_AND_ASSIGN(auto status, fs.GetFileStatus("s3://bucket/foo"));
    ASSERT_FALSE(status->IsDir());
    std::vector<std::unique_ptr<FileStatus>> statuses;
    ASSERT_OK(fs.ListFileStatus("s3://bucket/foo", &statuses));
    ASSERT_EQ(statuses.size(), 1);
    ASSERT_FALSE(statuses[0]->IsDir());
}

TEST(ObjectStoreFileSystemTest, TestHeadErrorIsNotMasked) {
    auto client = std::make_shared<MockObjectStoreClient>();
    client->head_error_ = Status::IOError("access denied");
    client->objects_["foo/bar"] = "child";
    ObjectStoreFileSystem fs("s3", client);
    auto status = fs.GetFileStatus("s3://bucket/foo");
    ASSERT_TRUE(status.status().IsIOError());
    ASSERT_EQ(client->list_calls_, 0);
}

TEST(ObjectStoreFileSystemTest, TestPaginationAndDirectoryMarker) {
    auto client = std::make_shared<MockObjectStoreClient>();
    ListObjectsResult first;
    first.objects.push_back({"dir/", 0, 0});
    first.objects.push_back({"dir/a", 1, 1});
    first.is_truncated = true;
    first.continuation_token = "next";
    ListObjectsResult second;
    second.common_prefixes.push_back("dir/sub/");
    client->pages_[""] = first;
    client->pages_["next"] = second;
    ObjectStoreFileSystem fs("s3", client);
    std::vector<std::unique_ptr<FileStatus>> statuses;
    ASSERT_OK(fs.ListFileStatus("s3://bucket/dir/", &statuses));
    ASSERT_EQ(statuses.size(), 2);
    ASSERT_EQ(statuses[0]->GetPath(), "s3://bucket/dir/a");
    ASSERT_TRUE(statuses[1]->IsDir());
}

TEST(ObjectStoreFileSystemTest, TestTruncatedPageRequiresContinuationToken) {
    auto client = std::make_shared<MockObjectStoreClient>();
    ListObjectsResult page;
    page.objects.push_back({"dir/a", 1, 1});
    page.is_truncated = true;
    client->pages_[""] = page;
    ObjectStoreFileSystem fs("s3", client);
    std::vector<std::unique_ptr<FileStatus>> statuses;
    ASSERT_TRUE(fs.ListFileStatus("s3://bucket/dir/", &statuses).IsIOError());
    ASSERT_TRUE(statuses.empty());
}

TEST(ObjectStoreFileSystemTest, TestOpenBucketRootIsDirectory) {
    auto client = std::make_shared<MockObjectStoreClient>();
    ObjectStoreFileSystem fs("s3", client);
    ASSERT_TRUE(fs.Open("s3://bucket/").status().IsInvalid());
    ASSERT_EQ(client->head_calls_, 0);
    ASSERT_EQ(client->list_calls_, 0);
}

TEST(ObjectStoreFileSystemTest, TestPathWithLeadingSlashes) {
    auto client = std::make_shared<MockObjectStoreClient>();
    client->objects_["file"] = "data";
    ObjectStoreFileSystem fs("s3", client);
    ASSERT_OK_AND_ASSIGN(auto status, fs.GetFileStatus("s3://bucket///file"));
    ASSERT_EQ(status->GetPath(), "s3://bucket/file");
}

TEST(ObjectStoreInputStreamTest, TestBoundsCloseAndSeekOverflow) {
    auto client = std::make_shared<MockObjectStoreClient>();
    client->objects_["file"] = std::string(128 * 1024, 'x');
    ObjectStoreFileSystem fs("s3", client, 64 * 1024);
    ASSERT_OK_AND_ASSIGN(auto stream, fs.Open("s3://bucket/file"));
    char data[4];
    bool called = false;
    stream->ReadAsync(data, 4, 128 * 1024 - 2, [&called](Status status) {
        called = true;
        ASSERT_TRUE(status.IsInvalid());
    });
    ASSERT_TRUE(called);
    ASSERT_TRUE(stream->Seek(std::numeric_limits<int64_t>::max(), FS_SEEK_END).IsInvalid());
    ASSERT_OK(stream->Close());
    called = false;
    stream->ReadAsync(data, 1, 0, [&called, &stream](Status status) {
        called = true;
        ASSERT_TRUE(status.IsIOError());
        ASSERT_TRUE(stream->GetPos().status().IsIOError());
    });
    ASSERT_TRUE(called);
}

TEST(ObjectStoreInputStreamTest, TestShortReadFails) {
    auto client = std::make_shared<MockObjectStoreClient>();
    client->objects_["file"] = std::string(128 * 1024, 'x');
    client->short_read_ = true;
    ObjectStoreFileSystem fs("s3", client);
    ASSERT_OK_AND_ASSIGN(auto stream, fs.Open("s3://bucket/file"));
    char data[4];
    ASSERT_TRUE(stream->Read(data, sizeof(data)).status().IsIOError());
}

TEST(ObjectStoreInputStreamTest, TestSequentialReadAheadGrows) {
    auto client = std::make_shared<MockObjectStoreClient>();
    client->objects_["file"] = std::string(1024 * 1024, 'x');
    ObjectStoreFileSystem fs("s3", client);
    ASSERT_OK_AND_ASSIGN(auto stream, fs.Open("s3://bucket/file"));
    std::array<char, 4096> buffer{};
    ASSERT_OK(stream->Read(buffer.data(), buffer.size()));
    ASSERT_EQ(client->ranges_.back(), Range(0, 64 * 1024));
    ASSERT_OK(stream->Seek(64 * 1024, FS_SEEK_SET));
    ASSERT_OK(stream->Read(buffer.data(), buffer.size()));
    ASSERT_EQ(client->ranges_.back(), Range(64 * 1024, 128 * 1024));
}

TEST(ObjectStoreInputStreamTest, TestConsumedBufferReleasesBudget) {
    auto client = std::make_shared<MockObjectStoreClient>();
    client->objects_["file"] = std::string(1024 * 1024, 'x');
    ObjectStoreFileSystem fs("s3", client, 64 * 1024);
    ASSERT_OK_AND_ASSIGN(auto stream, fs.Open("s3://bucket/file"));
    std::array<char, 4096> buffer{};
    for (int32_t i = 0; i < 17; ++i) {
        ASSERT_OK(stream->Read(buffer.data(), buffer.size()));
    }
    ASSERT_EQ(client->ranges_.size(), 2);
    ASSERT_EQ(client->ranges_.back(), Range(64 * 1024, 64 * 1024));
}

TEST(ObjectStoreInputStreamTest, TestBackwardSeekReleasesBudget) {
    auto client = std::make_shared<MockObjectStoreClient>();
    client->objects_["file"] = std::string(1024 * 1024, 'x');
    ObjectStoreFileSystem fs("s3", client, 64 * 1024);
    ASSERT_OK_AND_ASSIGN(auto stream, fs.Open("s3://bucket/file"));
    std::array<char, 4096> buffer{};
    ASSERT_OK(stream->Seek(64 * 1024, FS_SEEK_SET));
    ASSERT_OK(stream->Read(buffer.data(), buffer.size()));
    ASSERT_OK(stream->Seek(0, FS_SEEK_SET));
    ASSERT_OK(stream->Read(buffer.data(), buffer.size()));
    ASSERT_EQ(client->ranges_.back(), Range(0, 64 * 1024));
}

TEST(ObjectStoreInputStreamTest, TestLargeDirectReadReleasesBudget) {
    auto client = std::make_shared<MockObjectStoreClient>();
    client->objects_["file"] = std::string(16 * 1024 * 1024, 'x');
    ObjectStoreFileSystem fs("s3", client, 64 * 1024);
    ASSERT_OK_AND_ASSIGN(auto stream, fs.Open("s3://bucket/file"));
    std::array<char, 4096> small_buffer{};
    ASSERT_OK(stream->Seek(64 * 1024, FS_SEEK_SET));
    ASSERT_OK(stream->Read(small_buffer.data(), small_buffer.size()));
    std::vector<char> large_buffer(8 * 1024 * 1024);
    ASSERT_OK(stream->Seek(1024 * 1024, FS_SEEK_SET));
    ASSERT_OK(stream->Read(large_buffer.data(), large_buffer.size()));
    ASSERT_OK(stream->Seek(0, FS_SEEK_SET));
    ASSERT_OK(stream->Read(small_buffer.data(), small_buffer.size()));
    ASSERT_EQ(client->ranges_.back(), Range(0, 64 * 1024));
}

TEST(ObjectStoreInputStreamTest, TestCompetingStreamsShareBudget) {
    auto client = std::make_shared<MockObjectStoreClient>();
    client->objects_["first"] = std::string(1024 * 1024, 'x');
    client->objects_["second"] = std::string(1024 * 1024, 'x');
    ObjectStoreFileSystem fs("s3", client, 64 * 1024);
    ASSERT_OK_AND_ASSIGN(auto first, fs.Open("s3://bucket/first"));
    ASSERT_OK_AND_ASSIGN(auto second, fs.Open("s3://bucket/second"));
    std::array<char, 4096> buffer{};
    ASSERT_OK(first->Read(buffer.data(), buffer.size()));
    ASSERT_OK(second->Read(buffer.data(), buffer.size()));
    ASSERT_EQ(client->ranges_.back(), Range(0, 4096));
    ASSERT_OK(first->Close());
    ASSERT_OK(second->Seek(100000, FS_SEEK_SET));
    ASSERT_OK(second->Read(buffer.data(), buffer.size()));
    ASSERT_EQ(client->ranges_.back(), Range(100000, 64 * 1024));
}

}  // namespace
}  // namespace paimon::test
