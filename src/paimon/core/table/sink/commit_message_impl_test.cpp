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

#include "paimon/core/table/sink/commit_message_impl.h"

#include <memory>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(CommitMessageImplTest, TestToString) {
    std::string data_path = paimon::test::GetDataDir() +
                            "/orc/append_10_external_path.db/append_10_external_path/"
                            "commit_messages/commit_messages-01";
    auto file_system = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStatus> file_status,
                         file_system->GetFileStatus(data_path));
    auto buffer_length = file_status->GetLen();
    std::vector<uint8_t> buffer(buffer_length, 0);

    auto in_stream = file_system->Open(data_path).value_or(nullptr);
    ASSERT_TRUE(in_stream);
    ASSERT_OK(in_stream->Read(reinterpret_cast<char*>(buffer.data()), buffer.size()));
    ASSERT_OK(in_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(
        auto commit_messages,
        CommitMessage::DeserializeList(/*version=*/6, reinterpret_cast<char*>(buffer.data()),
                                       buffer.size(), pool));
    ASSERT_EQ(1, commit_messages.size());
    auto msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(commit_messages[0]);
    ASSERT_TRUE(msg_impl);
    std::string expect =
        "FileCommittable {partition = BinaryRow@9c67b85d, bucket = 0, totalBuckets = null, "
        "newFilesIncrement = "
        "DataIncrement {newFiles = data-64d93fc3-eaf2-4253-9cff-a9faa701e207-0.orc, deletedFiles = "
        ", changelogFiles = , newIndexFiles = , deletedIndexFiles = }, compactIncrement = "
        "CompactIncrement {compactBefore = , compactAfter = , changelogFiles = , newIndexFiles = "
        ", deletedIndexFiles = }}";
    ASSERT_EQ(expect, msg_impl->ToString());
}

}  // namespace paimon::test
