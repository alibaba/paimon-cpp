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

#include "paimon/core/manifest/manifest_committable.h"

#include <cstddef>

#include "gtest/gtest.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class ManifestCommittableTest : public testing::Test {
 private:
    bool IsEqualMap(const std::map<int32_t, int64_t>& actual_map,
                    const std::map<int32_t, int64_t>& expected_map) {
        if (expected_map.size() != actual_map.size()) {
            return false;
        }
        for (const auto& kv : expected_map) {
            const auto& key = kv.first;
            const auto& value = kv.second;
            auto iter = actual_map.find(key);
            if (iter != actual_map.end()) {
                if (iter->second == value) {
                    continue;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    std::vector<std::shared_ptr<CommitMessage>> GetCommitMessages(const std::string& path,
                                                                  int32_t version) const {
        auto file_system = std::make_shared<LocalFileSystem>();
        auto buffer_length = file_system->GetFileStatus(path).value()->GetLen();
        std::vector<uint8_t> buffer(buffer_length, 0);

        EXPECT_OK_AND_ASSIGN(auto in_stream, file_system->Open(path));
        EXPECT_OK_AND_ASSIGN(
            [[maybe_unused]] int32_t read_bytes,
            in_stream->Read(reinterpret_cast<char*>(buffer.data()), buffer.size()));
        EXPECT_OK(in_stream->Close());

        auto pool = GetDefaultPool();
        EXPECT_OK_AND_ASSIGN(
            auto ret, CommitMessage::DeserializeList(
                          version, reinterpret_cast<char*>(buffer.data()), buffer.size(), pool));
        return ret;
    }

    bool IsEqualMsgs(const std::vector<std::shared_ptr<CommitMessage>>& expected_msgs,
                     const std::vector<std::shared_ptr<CommitMessage>>& actual_msgs) {
        if (expected_msgs.size() != actual_msgs.size()) {
            return false;
        }
        for (size_t i = 0; i < expected_msgs.size(); i++) {
            auto actual = std::dynamic_pointer_cast<CommitMessageImpl>(actual_msgs[i]);
            auto expected = std::dynamic_pointer_cast<CommitMessageImpl>(expected_msgs[i]);
            if (*actual == *expected) {
                continue;
            } else {
                return false;
            }
        }
        return true;
    }
};

TEST_F(ManifestCommittableTest, TestSimple) {
    {
        ManifestCommittable committable(123);
        ASSERT_EQ(committable.Identifier(), 123);
    }
    {
        ManifestCommittable committable(/*identifier=*/123, /*watermark=*/456);
        ASSERT_EQ(committable.Identifier(), 123);
        ASSERT_EQ(committable.Watermark().value(), 456);
    }
    {
        std::map<int32_t, int64_t> log_offsets = {{123, 444}, {234, 555}};
        std::map<std::string, std::string> properties = {};
        std::vector<std::shared_ptr<CommitMessage>> msgs =
            GetCommitMessages(paimon::test::GetDataDir() +
                                  "/orc/append_09.db/append_09/commit_messages/commit_messages-01",
                              /*version=*/3);
        ManifestCommittable committable(/*identifier=*/123, /*watermark=*/456, log_offsets,
                                        properties, msgs);
        ASSERT_TRUE(IsEqualMap(committable.LogOffsets(), log_offsets));
        ASSERT_TRUE(IsEqualMsgs(msgs, committable.FileCommittables()));
    }
    {
        std::map<int32_t, int64_t> log_offsets = {};
        std::map<std::string, std::string> properties = {{"key1", "value1"}, {"key2", "value2"}};
        std::vector<std::shared_ptr<CommitMessage>> msgs =
            GetCommitMessages(paimon::test::GetDataDir() +
                                  "/orc/append_09.db/append_09/commit_messages/commit_messages-01",
                              /*version=*/3);
        ManifestCommittable committable(/*identifier=*/123, /*watermark=*/456,
                                        /*log_offsets=*/{}, properties, msgs);
        ASSERT_EQ(committable.Properties(), properties);
        ASSERT_TRUE(IsEqualMsgs(msgs, committable.FileCommittables()));
    }
}

}  // namespace paimon::test
