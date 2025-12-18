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

#include "paimon/commit_message.h"

#include <utility>

#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/table/sink/commit_message_serializer.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

int32_t CommitMessage::CurrentVersion() {
    return CommitMessageSerializer::CURRENT_VERSION;
}

CommitMessage::~CommitMessage() = default;

Result<std::string> CommitMessage::Serialize(const std::shared_ptr<CommitMessage>& commit_message,
                                             const std::shared_ptr<MemoryPool>& pool) {
    CommitMessageSerializer serializer(pool);
    MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    PAIMON_RETURN_NOT_OK(serializer.Serialize(commit_message, &out));
    PAIMON_UNIQUE_PTR<Bytes> bytes =
        MemorySegmentUtils::CopyToBytes(out.Segments(), 0, out.CurrentSize(), pool.get());
    return std::string(bytes->data(), bytes->size());
}

Result<std::string> CommitMessage::SerializeList(
    const std::vector<std::shared_ptr<CommitMessage>>& commit_messages,
    const std::shared_ptr<MemoryPool>& pool) {
    CommitMessageSerializer serializer(pool);
    MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    PAIMON_RETURN_NOT_OK(serializer.SerializeList(commit_messages, &out));
    PAIMON_UNIQUE_PTR<Bytes> bytes =
        MemorySegmentUtils::CopyToBytes(out.Segments(), 0, out.CurrentSize(), pool.get());
    return std::string(bytes->data(), bytes->size());
}

Result<std::shared_ptr<CommitMessage>> CommitMessage::Deserialize(
    int32_t version, const char* buffer, int32_t length, const std::shared_ptr<MemoryPool>& pool) {
    if (buffer == nullptr) {
        return Status::Invalid("buffer is null pointer");
    }
    if (length <= 0) {
        return Status::Invalid("length is equal or less than zero");
    }
    CommitMessageSerializer serializer(pool);
    auto input_stream = std::make_shared<ByteArrayInputStream>(buffer, length);
    DataInputStream in(input_stream);
    return serializer.Deserialize(version, &in);
}

Result<std::vector<std::shared_ptr<CommitMessage>>> CommitMessage::DeserializeList(
    int32_t version, const char* buffer, int32_t length, const std::shared_ptr<MemoryPool>& pool) {
    if (buffer == nullptr) {
        return Status::Invalid("buffer is null pointer");
    }
    if (length <= 0) {
        return Status::Invalid("length is equal or less than zero");
    }
    CommitMessageSerializer serializer(pool);
    auto input_stream = std::make_shared<ByteArrayInputStream>(buffer, length);
    DataInputStream in(input_stream);
    return serializer.DeserializeList(version, &in);
}

Result<std::string> CommitMessage::ToDebugString(
    const std::shared_ptr<CommitMessage>& commit_message) {
    auto message = std::dynamic_pointer_cast<CommitMessageImpl>(commit_message);
    if (message == nullptr) {
        return Status::Invalid("failed to cast commit message to commit message impl");
    }
    return message->ToString();
}

}  // namespace paimon
