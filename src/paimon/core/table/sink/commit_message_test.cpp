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

#include <cstddef>
#include <map>
#include <utility>
#include <variant>

#include "arrow/array/array_base.h"
#include "arrow/array/builder_binary.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/utils/linked_hash_map.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/index/deletion_vector_meta.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/table/sink/commit_message_serializer.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/file_store_write.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/record_batch.h"
#include "paimon/status.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"

namespace paimon::test {

TEST(CommitMessageTest, TestCurrentVersion) {
    ASSERT_EQ(CommitMessageSerializer::CURRENT_VERSION, CommitMessage::CurrentVersion());
}

TEST(CommitMessageTest, TestCompatibleWithVersion11) {
    // index file meta: add global index meta
    int32_t version = 11;
    std::string data_path =
        paimon::test::GetDataDir() +
        "orc/append_with_global_index_with_partition.db/append_with_global_index_with_partition/"
        "commit_messages/commit_messages-01";
    auto file_system = std::make_shared<LocalFileSystem>();
    auto buffer_length = file_system->GetFileStatus(data_path).value()->GetLen();

    std::vector<char> buffer(buffer_length, 0);
    ASSERT_OK_AND_ASSIGN(auto in_stream, file_system->Open(data_path));
    ASSERT_OK(in_stream->Read(reinterpret_cast<char*>(buffer.data()), buffer.size()));
    ASSERT_OK(in_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> ret,
                         CommitMessage::DeserializeList(
                             version, reinterpret_cast<char*>(buffer.data()), buffer.size(), pool));
    std::vector<CommitMessageImpl> res_msgs;
    for (const auto& msg : ret) {
        auto msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(msg);
        res_msgs.emplace_back(*msg_impl);
    }
    ASSERT_EQ(res_msgs.size(), 1);

    std::vector<CommitMessageImpl> expected_msgs;
    auto index_meta = std::make_shared<IndexFileMeta>(
        "bitmap", "bitmap-global-index-6f974a9b-07bb-4a06-9696-6646020d8139.index",
        /*file_size=*/120, /*row_count=*/5, /*dv_ranges=*/std::nullopt,
        /*external_path=*/std::nullopt,
        GlobalIndexMeta(/*row_range_start=*/0, /*row_range_end=*/4, /*index_field_id=*/0,
                        /*extra_field_ids=*/std::nullopt, /*index_meta=*/nullptr));

    expected_msgs.emplace_back(
        /*partition=*/BinaryRowGenerator::GenerateRow({10}, pool.get()),
        /*bucket=*/0, /*total_bucket=*/std::nullopt, DataIncrement({index_meta}),
        CompactIncrement({}, {}, {}));

    // check result
    ASSERT_EQ(res_msgs, expected_msgs);
    ASSERT_OK_AND_ASSIGN(std::string serialized_bytes, CommitMessage::SerializeList(ret, pool));
    ASSERT_EQ(serialized_bytes, std::string(reinterpret_cast<char*>(buffer.data()), buffer.size()));
}

TEST(CommitMessageTest, TestCompatibleWithVersion10) {
    // test with commit message version 10
    // move IndexFileMeta into DataIncrement & CompactIncrement
    int32_t version = 10;
    std::string data_path = paimon::test::GetDataDir() +
                            "orc/pk_dv_index_with_commit_message_version10.db/"
                            "pk_dv_index_with_commit_message_version10/"
                            "commit_messages/commit_messages-01";
    auto file_system = std::make_shared<LocalFileSystem>();
    auto buffer_length = file_system->GetFileStatus(data_path).value()->GetLen();

    std::vector<char> buffer(buffer_length, 0);
    ASSERT_OK_AND_ASSIGN(auto in_stream, file_system->Open(data_path));
    ASSERT_OK(in_stream->Read(reinterpret_cast<char*>(buffer.data()), buffer.size()));
    ASSERT_OK(in_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> ret,
                         CommitMessage::DeserializeList(
                             version, reinterpret_cast<char*>(buffer.data()), buffer.size(), pool));
    std::vector<CommitMessageImpl> res_msgs;
    for (const auto& msg : ret) {
        auto msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(msg);
        res_msgs.emplace_back(*msg_impl);
    }
    ASSERT_EQ(res_msgs.size(), 1);

    std::vector<CommitMessageImpl> expected_msgs;
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-a19eec15-e0e3-4a30-85e2-01d23d9945be-1.orc", /*file_size=*/872, /*row_count=*/1,
        /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("Tony"), 0}, pool.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("Tony"), 0}, pool.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Tony"), 0}, {std::string("Tony"), 0},
                                          {0, 0}, pool.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Tony"), 10, 0, 14.1},
                                          {std::string("Tony"), 10, 0, 14.1}, {0, 0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/5, /*max_sequence_number=*/5, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1761242383412ll, 0),
        /*delete_row_count=*/1, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);

    LinkedHashMap<std::string, DeletionVectorMeta> dv_ranges;
    dv_ranges.insert_or_assign(
        "data-a19eec15-e0e3-4a30-85e2-01d23d9945be-0.orc",
        DeletionVectorMeta("data-a19eec15-e0e3-4a30-85e2-01d23d9945be-0.orc", /*offset=*/1,
                           /*length=*/22, /*cardinality=*/1));
    expected_msgs.emplace_back(
        /*partition=*/BinaryRowGenerator::GenerateRow({10}, pool.get()),
        /*bucket=*/1, /*total_bucket=*/2, DataIncrement({file_meta}, {}, {}, {}, {}),
        CompactIncrement({file_meta}, {}, {},
                         {std::make_shared<IndexFileMeta>(
                             "DELETION_VECTORS", "index-9c24b2fc-40db-4f58-9a13-55b52ae8880c-1", 31,
                             1, dv_ranges, std::nullopt)},
                         {}));

    // check result
    ASSERT_EQ(res_msgs, expected_msgs);
}

TEST(CommitMessageTest, TestCompatibleWithVersion9) {
    // test with commit message version 9
    // add write_cols in DataFileMeta and external path in IndexFileMeta
    int32_t version = 9;
    std::string data_path =
        paimon::test::GetDataDir() +
        "orc/pk_dv_index_not_in_data_no_external.db/pk_dv_index_not_in_data_no_external/"
        "commit_messages/commit_messages-01";
    auto file_system = std::make_shared<LocalFileSystem>();
    auto buffer_length = file_system->GetFileStatus(data_path).value()->GetLen();

    std::vector<char> buffer(buffer_length, 0);
    ASSERT_OK_AND_ASSIGN(auto in_stream, file_system->Open(data_path));
    ASSERT_OK(in_stream->Read(reinterpret_cast<char*>(buffer.data()), buffer.size()));
    ASSERT_OK(in_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> ret,
                         CommitMessage::DeserializeList(
                             version, reinterpret_cast<char*>(buffer.data()), buffer.size(), pool));
    std::vector<CommitMessageImpl> res_msgs;
    for (const auto& msg : ret) {
        auto msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(msg);
        res_msgs.emplace_back(*msg_impl);
    }
    ASSERT_EQ(res_msgs.size(), 1);

    std::vector<CommitMessageImpl> expected_msgs;
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-aa87291d-2a90-4846-b106-1bb4c76d74db-1.orc", /*file_size=*/872, /*row_count=*/1,
        /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("Tony"), 0}, pool.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("Tony"), 0}, pool.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Tony"), 0}, {std::string("Tony"), 0},
                                          {0, 0}, pool.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Tony"), 10, 0, 14.1},
                                          {std::string("Tony"), 10, 0, 14.1}, {0, 0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/5, /*max_sequence_number=*/5, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1757349273600ll, 0),
        /*delete_row_count=*/1, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);

    LinkedHashMap<std::string, DeletionVectorMeta> dv_ranges;
    dv_ranges.insert_or_assign(
        "data-aa87291d-2a90-4846-b106-1bb4c76d74db-0.orc",
        DeletionVectorMeta("data-aa87291d-2a90-4846-b106-1bb4c76d74db-0.orc", /*offset=*/1,
                           /*length=*/22, /*cardinality=*/1));
    expected_msgs.emplace_back(
        /*partition=*/BinaryRowGenerator::GenerateRow({10}, pool.get()),
        /*bucket=*/1, /*total_bucket=*/2, DataIncrement({file_meta}, {}, {}, {}, {}),
        CompactIncrement({file_meta}, {}, {},
                         {std::make_shared<IndexFileMeta>(
                             "DELETION_VECTORS", "index-aa60193d-d7cd-434f-bc1a-c1adb210e1f7-1", 31,
                             1, dv_ranges, std::nullopt)},
                         {}));

    // check result
    ASSERT_EQ(res_msgs, expected_msgs);
    ASSERT_OK(CommitMessage::SerializeList(ret, pool));
}

TEST(CommitMessageTest, TestCompatibleWithVersion9WithExternalPathForIndex) {
    // test with commit message version 9
    // add write_cols in DataFileMeta and external path in IndexFileMeta
    int32_t version = 9;
    std::string data_path =
        paimon::test::GetDataDir() +
        "orc/pk_dv_index_in_data_with_external.db/pk_dv_index_in_data_with_external/"
        "commit_messages/commit_messages-01";
    auto file_system = std::make_shared<LocalFileSystem>();
    auto buffer_length = file_system->GetFileStatus(data_path).value()->GetLen();

    std::vector<char> buffer(buffer_length, 0);
    ASSERT_OK_AND_ASSIGN(auto in_stream, file_system->Open(data_path));
    ASSERT_OK(in_stream->Read(reinterpret_cast<char*>(buffer.data()), buffer.size()));
    ASSERT_OK(in_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> ret,
                         CommitMessage::DeserializeList(
                             version, reinterpret_cast<char*>(buffer.data()), buffer.size(), pool));
    std::vector<CommitMessageImpl> res_msgs;
    for (const auto& msg : ret) {
        auto msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(msg);
        res_msgs.emplace_back(*msg_impl);
    }
    ASSERT_EQ(res_msgs.size(), 1);

    std::vector<CommitMessageImpl> expected_msgs;
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-72b62a5f-d698-4db5-b51a-04c0dc027702-1.orc", /*file_size=*/872, /*row_count=*/1,
        /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("Tony"), 0}, pool.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("Tony"), 0}, pool.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Tony"), 0}, {std::string("Tony"), 0},
                                          {0, 0}, pool.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Tony"), 10, 0, 14.1},
                                          {std::string("Tony"), 10, 0, 14.1}, {0, 0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/5, /*max_sequence_number=*/5, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1757354416117ll, 0),
        /*delete_row_count=*/1, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/
        "FILE:/tmp/external/f1=10/bucket-1/data-72b62a5f-d698-4db5-b51a-04c0dc027702-1.orc",
        /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);

    LinkedHashMap<std::string, DeletionVectorMeta> dv_ranges;
    dv_ranges.insert_or_assign(
        "data-72b62a5f-d698-4db5-b51a-04c0dc027702-0.orc",
        DeletionVectorMeta("data-72b62a5f-d698-4db5-b51a-04c0dc027702-0.orc", /*offset=*/1,
                           /*length=*/22, /*cardinality=*/1));
    expected_msgs.emplace_back(
        /*partition=*/BinaryRowGenerator::GenerateRow({10}, pool.get()),
        /*bucket=*/1, /*total_bucket=*/2, DataIncrement({file_meta}, {}, {}, {}, {}),
        CompactIncrement(
            {file_meta}, {}, {},
            {std::make_shared<IndexFileMeta>(
                "DELETION_VECTORS", "index-419e7c6b-9cad-49e8-9cd2-6187471df954-1", 31, 1,
                dv_ranges,
                "FILE:/tmp/external/f1=10/bucket-1/index-419e7c6b-9cad-49e8-9cd2-6187471df954-1")},
            {}));

    // check result
    ASSERT_EQ(res_msgs, expected_msgs);
    ASSERT_OK(CommitMessage::SerializeList(ret, pool));
}

TEST(CommitMessageTest, TestCompatibleWithVersion8) {
    // test with commit message version 8
    // commit msg is generated by java paimon 1.3
    int32_t version = 8;
    std::string data_path = paimon::test::GetDataDir() +
                            "/orc/append_table_with_first_row_id.db/append_table_with_first_row_id/"
                            "commit_messages/commit_messages-01";
    auto file_system = std::make_shared<LocalFileSystem>();
    auto buffer_length = file_system->GetFileStatus(data_path).value()->GetLen();

    std::vector<uint8_t> buffer(buffer_length, 0);
    ASSERT_OK_AND_ASSIGN(auto in_stream, file_system->Open(data_path));
    ASSERT_OK(in_stream->Read(reinterpret_cast<char*>(buffer.data()), buffer.size()));
    ASSERT_OK(in_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> ret,
                         CommitMessage::DeserializeList(
                             version, reinterpret_cast<char*>(buffer.data()), buffer.size(), pool));
    std::vector<CommitMessageImpl> res_msgs;
    for (const auto& msg : ret) {
        auto msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(msg);
        res_msgs.emplace_back(*msg_impl);
    }

    std::vector<CommitMessageImpl> expected_msgs;
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-16bd83f7-282a-479a-9968-0868436516b0-0.orc", /*file_size=*/567, /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Alice"), 10, 1, 11.1},
                                          {std::string("Alice"), 10, 1, 11.1}, {0, 0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1754068646844ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    expected_msgs.emplace_back(/*partition=*/BinaryRowGenerator::GenerateRow({10}, pool.get()),
                               /*bucket=*/0, /*total_bucket=*/2, DataIncrement({file_meta}, {}, {}),
                               CompactIncrement({}, {}, {}));

    auto file_meta2 = std::make_shared<DataFileMeta>(
        "data-f33d2740-1205-49db-8ca4-d4fc2bddc99f-0.orc", /*file_size=*/620, /*row_count=*/4,
        /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Alex"), 10, 0, 12.1},
                                          {std::string("Tony"), 10, 0, 16.1}, {0, 0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/3, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1754068646864ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    expected_msgs.emplace_back(/*partition=*/BinaryRowGenerator::GenerateRow({10}, pool.get()),
                               /*bucket=*/1, /*total_bucket=*/2,
                               DataIncrement({file_meta2}, {}, {}), CompactIncrement({}, {}, {}));
    // check result
    ASSERT_EQ(res_msgs, expected_msgs);
    ASSERT_OK(CommitMessage::SerializeList(ret, pool));
}

TEST(CommitMessageTest, TestCompatibleWithVersion7) {
    // test with commit message version 7
    // commit msg is generated by java paimon 1.1
    int32_t version = 7;
    std::string data_path = paimon::test::GetDataDir() +
                            "/orc/pk_table_with_total_buckets.db/pk_table_with_total_buckets/"
                            "commit_messages/commit_messages-01";
    auto file_system = std::make_shared<LocalFileSystem>();
    auto buffer_length = file_system->GetFileStatus(data_path).value()->GetLen();
    std::vector<uint8_t> buffer(buffer_length, 0);
    ASSERT_OK_AND_ASSIGN(auto in_stream, file_system->Open(data_path));
    ASSERT_OK(in_stream->Read(reinterpret_cast<char*>(buffer.data()), buffer.size()));
    ASSERT_OK(in_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> ret,
                         CommitMessage::DeserializeList(
                             version, reinterpret_cast<char*>(buffer.data()), buffer.size(), pool));
    std::vector<CommitMessageImpl> res_msgs;
    for (const auto& msg : ret) {
        auto msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(msg);
        res_msgs.emplace_back(*msg_impl);
    }

    std::vector<CommitMessageImpl> expected_msgs;
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-d7725088-6bd4-4e70-9ce6-714ae93b47cc-0.orc", /*file_size=*/863, /*row_count=*/1,
        /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("Alice"), 1}, pool.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("Alice"), 1}, pool.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Alice"), 1}, {std::string("Alice"), 1},
                                          {0, 0}, pool.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Alice"), 10, 1, 11.1},
                                          {std::string("Alice"), 10, 1, 11.1}, {0, 0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1743525392885ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    expected_msgs.emplace_back(/*partition=*/BinaryRowGenerator::GenerateRow({10}, pool.get()),
                               /*bucket=*/0, /*total_bucket=*/2, DataIncrement({file_meta}, {}, {}),
                               CompactIncrement({}, {}, {}));

    auto file_meta2 = std::make_shared<DataFileMeta>(
        "data-5858a84b-7081-4618-b828-ae3918c5e1f6-0.orc", /*file_size=*/943, /*row_count=*/4,
        /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("Alex"), 0}, pool.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("Tony"), 0}, pool.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Alex"), 0}, {std::string("Tony"), 0},
                                          {0, 0}, pool.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Alex"), 10, 0, 12.1},
                                          {std::string("Tony"), 10, 0, 16.1}, {0, 0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/3, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1743525392921ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    expected_msgs.emplace_back(/*partition=*/BinaryRowGenerator::GenerateRow({10}, pool.get()),
                               /*bucket=*/1, /*total_bucket=*/2,
                               DataIncrement({file_meta2}, {}, {}), CompactIncrement({}, {}, {}));
    // check result
    ASSERT_EQ(res_msgs, expected_msgs);
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::string serialize_ret,
                         CommitMessage::SerializeList(ret, pool));
}

TEST(CommitMessageTest, TestCompatibleWithVersion6) {
    // test with commit message version 6
    // commit msg is generated by java paimon 1.0
    int32_t version = 6;
    std::string data_path = paimon::test::GetDataDir() +
                            "/orc/append_10_external_path.db/append_10_external_path/"
                            "commit_messages/commit_messages-01";
    auto file_system = std::make_shared<LocalFileSystem>();
    auto buffer_length = file_system->GetFileStatus(data_path).value()->GetLen();
    std::vector<uint8_t> buffer(buffer_length, 0);
    ASSERT_OK_AND_ASSIGN(auto in_stream, file_system->Open(data_path));
    ASSERT_OK(in_stream->Read(reinterpret_cast<char*>(buffer.data()), buffer.size()));
    ASSERT_OK(in_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> ret,
                         CommitMessage::DeserializeList(
                             version, reinterpret_cast<char*>(buffer.data()), buffer.size(), pool));
    std::vector<CommitMessageImpl> res_msgs;
    for (const auto& msg : ret) {
        auto msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(msg);
        res_msgs.emplace_back(*msg_impl);
    }

    std::vector<CommitMessageImpl> expected_msgs;
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-64d93fc3-eaf2-4253-9cff-a9faa701e207-0.orc", /*file_size=*/645, /*row_count=*/5,
        BinaryRow::EmptyRow(), BinaryRow::EmptyRow(), SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Alice"), 10, 0, 11.1},
                                          {std::string("Tony"), 20, 1, 14.1}, {0, 0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/4, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1737052260143ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    expected_msgs.emplace_back(BinaryRow::EmptyRow(), /*bucket=*/0, /*total_bucket=*/std::nullopt,
                               DataIncrement({file_meta}, {}, {}), CompactIncrement({}, {}, {}));
    // check result
    ASSERT_EQ(res_msgs, expected_msgs);
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::string serialize_ret,
                         CommitMessage::SerializeList(ret, pool));
}

TEST(CommitMessageTest, TestCompatibleWithVersion5) {
    // test with commit message version 5
    // commit msg is generated by java paimon 1.0
    int32_t version = 5;
    std::string data_path = paimon::test::GetDataDir() +
                            "/orc/pk_table_with_dv_cardinality.db/pk_table_with_dv_cardinality/"
                            "commit_messages/commit_messages-01";
    auto file_system = std::make_shared<LocalFileSystem>();
    auto buffer_length = file_system->GetFileStatus(data_path).value()->GetLen();
    std::vector<uint8_t> buffer(buffer_length, 0);
    ASSERT_OK_AND_ASSIGN(auto in_stream, file_system->Open(data_path));
    ASSERT_OK(in_stream->Read(reinterpret_cast<char*>(buffer.data()), buffer.size()));
    ASSERT_OK(in_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> ret,
                         CommitMessage::DeserializeList(
                             version, reinterpret_cast<char*>(buffer.data()), buffer.size(), pool));
    std::vector<CommitMessageImpl> res_msgs;
    for (const auto& msg : ret) {
        auto msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(msg);
        res_msgs.emplace_back(*msg_impl);
    }

    std::vector<CommitMessageImpl> expected_msgs;
    auto file_meta1 = std::make_shared<DataFileMeta>(
        "data-0d0f29cc-63c6-4fab-a594-71bd7d06fcde-1.orc", /*file_size=*/859, /*row_count=*/1,
        BinaryRowGenerator::GenerateRow({std::string("Alice"), 1}, pool.get()),
        BinaryRowGenerator::GenerateRow({std::string("Alice"), 1}, pool.get()),
        BinaryRowGenerator::GenerateStats({std::string("Alice"), 1}, {std::string("Alice"), 1},
                                          {0, 0}, pool.get()),
        BinaryRowGenerator::GenerateStats({std::string("Alice"), 10, 1},
                                          {std::string("Alice"), 10, 1}, {0, 0, 0}, pool.get()),
        /*min_sequence_number=*/2, /*max_sequence_number=*/2, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1734707236040ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::optional<std::vector<std::string>>({"f0", "f1", "f2"}),
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    auto file_meta1_after_compact = std::make_shared<DataFileMeta>(
        "data-0d0f29cc-63c6-4fab-a594-71bd7d06fcde-1.orc", /*file_size=*/859, /*row_count=*/1,
        BinaryRowGenerator::GenerateRow({std::string("Alice"), 1}, pool.get()),
        BinaryRowGenerator::GenerateRow({std::string("Alice"), 1}, pool.get()),
        BinaryRowGenerator::GenerateStats({std::string("Alice"), 1}, {std::string("Alice"), 1},
                                          {0, 0}, pool.get()),
        BinaryRowGenerator::GenerateStats({std::string("Alice"), 10, 1},
                                          {std::string("Alice"), 10, 1}, {0, 0, 0}, pool.get()),
        /*min_sequence_number=*/2, /*max_sequence_number=*/2, /*schema_id=*/0,
        /*level=*/4, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1734707236040ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::optional<std::vector<std::string>>({"f0", "f1", "f2"}),
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);

    DataIncrement data_increment1({file_meta1}, {}, {}, {}, {});
    LinkedHashMap<std::string, DeletionVectorMeta> dv_metas1;
    dv_metas1.insert_or_assign(
        "data-0d0f29cc-63c6-4fab-a594-71bd7d06fcde-0.orc",
        DeletionVectorMeta("data-0d0f29cc-63c6-4fab-a594-71bd7d06fcde-0.orc", /*offset=*/1,
                           /*length=*/22, /*cardinality=*/1));
    auto index_file_meta1 = std::make_shared<IndexFileMeta>(
        "DELETION_VECTORS", "index-86356766-3238-46e6-990b-656cd7409eaa-0", /*file_size=*/31,
        /*row_count=*/1, dv_metas1, /*external_path=*/std::nullopt);
    expected_msgs.emplace_back(
        BinaryRowGenerator::GenerateRow({10}, pool.get()), /*bucket=*/0,
        /*total_bucket=*/std::nullopt, data_increment1,
        CompactIncrement({file_meta1}, {file_meta1_after_compact}, {}, {index_file_meta1}, {}));

    auto file_meta2 = std::make_shared<DataFileMeta>(
        "data-2ffe7ae9-2cf7-41e9-944b-2065585cde31-1.orc", /*file_size=*/922, /*row_count=*/2,
        BinaryRowGenerator::GenerateRow({std::string("Lily"), 0}, pool.get()),
        BinaryRowGenerator::GenerateRow({std::string("Tony"), 0}, pool.get()),
        BinaryRowGenerator::GenerateStats({std::string("Lily"), 0}, {std::string("Tony"), 0},
                                          {0, 0}, pool.get()),
        BinaryRowGenerator::GenerateStats({std::string("Lily"), 10, 0},
                                          {std::string("Tony"), 10, 0}, {0, 0, 0}, pool.get()),
        /*min_sequence_number=*/7, /*max_sequence_number=*/8, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1734707236109ll, 0),
        /*delete_row_count=*/2, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::optional<std::vector<std::string>>({"f0", "f1", "f2"}),
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    DataIncrement data_increment2({file_meta2}, {}, {}, {}, {});
    LinkedHashMap<std::string, DeletionVectorMeta> dv_metas2;
    dv_metas2.insert_or_assign(
        "data-2ffe7ae9-2cf7-41e9-944b-2065585cde31-0.orc",
        DeletionVectorMeta("data-2ffe7ae9-2cf7-41e9-944b-2065585cde31-0.orc", /*offset=*/1,
                           /*length=*/24, /*cardinality=*/2));
    auto index_file_meta2 = std::make_shared<IndexFileMeta>(
        "DELETION_VECTORS", "index-86356766-3238-46e6-990b-656cd7409eaa-1", /*file_size=*/33,
        /*row_count=*/1, dv_metas2, /*external_path=*/std::nullopt);
    expected_msgs.emplace_back(BinaryRowGenerator::GenerateRow({10}, pool.get()), /*bucket=*/1,
                               /*total_bucket=*/std::nullopt, data_increment2,
                               CompactIncrement({file_meta2}, {}, {}, {index_file_meta2}, {}));

    // check result
    ASSERT_EQ(res_msgs, expected_msgs);
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::string serialize_ret,
                         CommitMessage::SerializeList(ret, pool));
}

TEST(CommitMessageTest, TestCompatibleWithVersion4) {
    // test with commit message version 4
    // commit msg is generated by java paimon 1.0
    int32_t version = 4;
    std::string data_path = paimon::test::GetDataDir() +
                            "/orc/append_10.db/append_10/commit_messages/commit_messages-01";
    auto file_system = std::make_shared<LocalFileSystem>();
    auto buffer_length = file_system->GetFileStatus(data_path).value()->GetLen();
    std::vector<uint8_t> buffer(buffer_length, 0);

    ASSERT_OK_AND_ASSIGN(auto in_stream, file_system->Open(data_path));
    ASSERT_OK(in_stream->Read(reinterpret_cast<char*>(buffer.data()), buffer.size()));
    ASSERT_OK(in_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> ret,
                         CommitMessage::DeserializeList(
                             version, reinterpret_cast<char*>(buffer.data()), buffer.size(), pool));
    std::vector<CommitMessageImpl> res_msgs;
    for (const auto& msg : ret) {
        auto msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(msg);
        res_msgs.emplace_back(*msg_impl);
    }

    std::vector<CommitMessageImpl> expected_msgs;
    auto file_meta1 = std::make_shared<DataFileMeta>(
        "data-e54f10e3-60ec-4a2a-be29-32f2b6183884-0.orc", /*file_size=*/543, /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Alice"), 10, 1, 11.1},
                                          {std::string("Alice"), 10, 1, 11.1}, {0, 0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1731404403175ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataIncrement data_increment1({file_meta1}, {}, {});
    expected_msgs.emplace_back(BinaryRowGenerator::GenerateRow({10}, pool.get()), /*bucket=*/0,
                               /*total_bucket=*/std::nullopt, data_increment1,
                               CompactIncrement({}, {}, {}));

    auto file_meta2 = std::make_shared<DataFileMeta>(
        "data-de4e972f-5cc8-49b1-844e-374191534c68-0.orc", /*file_size=*/575, /*row_count=*/3,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Bob"), 10, 0, 12.1},
                                          {std::string("Tony"), 10, 0, 14.1}, {0, 0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/2, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1731404403198ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataIncrement data_increment2({file_meta2}, {}, {});
    expected_msgs.emplace_back(BinaryRowGenerator::GenerateRow({10}, pool.get()), /*bucket=*/1,
                               /*total_bucket=*/std::nullopt, data_increment2,
                               CompactIncrement({}, {}, {}));

    auto file_meta3 = std::make_shared<DataFileMeta>(
        "data-8ab054d3-0480-4268-84f7-bf2759632f76-0.orc", /*file_size=*/541, /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Lucy"), 20, 1, 14.1},
                                          {std::string("Lucy"), 20, 1, 14.1}, {0, 0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1731404403214ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataIncrement data_increment3({file_meta3}, {}, {});
    expected_msgs.emplace_back(BinaryRowGenerator::GenerateRow({20}, pool.get()), /*bucket=*/0,
                               /*total_bucket=*/std::nullopt, data_increment3,
                               CompactIncrement({}, {}, {}));

    // check result
    ASSERT_EQ(res_msgs, expected_msgs);
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::string serialize_ret,
                         CommitMessage::SerializeList(ret, pool));
}

TEST(CommitMessageTest, TestCompatibleWithJavaPaimon10WithStatsDenseStore) {
    // test with commit message version 4
    // commit msg is generated by java paimon 1.0
    int32_t version = 4;
    std::string data_path =
        paimon::test::GetDataDir() +
        "/orc/append_10_stats_dense_store.db/append_10_stats_dense_store/commit_messages/"
        "commit_messages-01";
    auto file_system = std::make_shared<LocalFileSystem>();
    auto buffer_length = file_system->GetFileStatus(data_path).value()->GetLen();
    std::vector<uint8_t> buffer(buffer_length, 0);

    ASSERT_OK_AND_ASSIGN(auto in_stream, file_system->Open(data_path));
    ASSERT_OK(in_stream->Read(reinterpret_cast<char*>(buffer.data()), buffer.size()));
    ASSERT_OK(in_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> ret,
                         CommitMessage::DeserializeList(
                             version, reinterpret_cast<char*>(buffer.data()), buffer.size(), pool));
    std::vector<CommitMessageImpl> res_msgs;
    for (const auto& msg : ret) {
        auto msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(msg);
        res_msgs.emplace_back(*msg_impl);
    }

    std::vector<CommitMessageImpl> expected_msgs;
    auto file_meta1 = std::make_shared<DataFileMeta>(
        "data-cdb38c8a-31c1-4824-a024-9abd3fbb466f-0.orc", /*file_size=*/543, /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Alice"), 10, 1},
                                          {std::string("Alice"), 10, 1}, {0, 0, 0}, pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1731412938869ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::optional<std::vector<std::string>>({"f0", "f1", "f2"}),
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    DataIncrement data_increment1({file_meta1}, {}, {});
    expected_msgs.emplace_back(BinaryRowGenerator::GenerateRow({10}, pool.get()), /*bucket=*/0,
                               /*total_bucket=*/std::nullopt, data_increment1,
                               CompactIncrement({}, {}, {}));

    auto file_meta2 = std::make_shared<DataFileMeta>(
        "data-c2613568-0412-4cd9-a0c4-1eae8e4ca89b-0.orc", /*file_size=*/575, /*row_count=*/3,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Bob"), 10, 0}, {std::string("Tony"), 10, 0},
                                          {0, 0, 0}, pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/2, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1731412938891ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::optional<std::vector<std::string>>({"f0", "f1", "f2"}),
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    DataIncrement data_increment2({file_meta2}, {}, {});
    expected_msgs.emplace_back(BinaryRowGenerator::GenerateRow({10}, pool.get()), /*bucket=*/1,
                               /*total_bucket=*/std::nullopt, data_increment2,
                               CompactIncrement({}, {}, {}));

    auto file_meta3 = std::make_shared<DataFileMeta>(
        "data-a6d1261a-f798-4fbd-a251-6d6c7d8060dd-0.orc", /*file_size=*/541, /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Lucy"), 20, 1},
                                          {std::string("Lucy"), 20, 1}, {0, 0, 0}, pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1731412938908ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::optional<std::vector<std::string>>({"f0", "f1", "f2"}),
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    DataIncrement data_increment3({file_meta3}, {}, {});
    expected_msgs.emplace_back(BinaryRowGenerator::GenerateRow({20}, pool.get()), /*bucket=*/0,
                               /*total_bucket=*/std::nullopt, data_increment3,
                               CompactIncrement({}, {}, {}));

    // check result
    ASSERT_EQ(res_msgs, expected_msgs);

    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::string serialize_ret,
                         CommitMessage::SerializeList(ret, pool));
}

TEST(CommitMessageTest, TestCompatibleWith09JavaPaimon1) {
    // test with commit message version 3
    int32_t version = 3;
    std::string data_path = paimon::test::GetDataDir() +
                            "/orc/append_09.db/append_09/commit_messages/commit_messages-01";
    auto file_system = std::make_shared<LocalFileSystem>();
    auto buffer_length = file_system->GetFileStatus(data_path).value()->GetLen();
    std::vector<uint8_t> buffer(buffer_length, 0);

    ASSERT_OK_AND_ASSIGN(auto in_stream, file_system->Open(data_path));
    ASSERT_OK(in_stream->Read(reinterpret_cast<char*>(buffer.data()), buffer.size()));
    ASSERT_OK(in_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> ret,
                         CommitMessage::DeserializeList(
                             version, reinterpret_cast<char*>(buffer.data()), buffer.size(), pool));
    std::vector<CommitMessageImpl> res_msgs;
    for (const auto& msg : ret) {
        auto msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(msg);
        res_msgs.emplace_back(*msg_impl);
    }

    std::vector<CommitMessageImpl> expected_msgs;
    auto file_meta1 = std::make_shared<DataFileMeta>(
        "data-51a45441-6037-4af3-b67b-5cefd75dc6f2-0.orc", /*file_size=*/543, /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Alice"), 10, 1, 11.1},
                                          {std::string("Alice"), 10, 1, 11.1}, {0, 0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataIncrement data_increment1({file_meta1}, {}, {});
    expected_msgs.emplace_back(BinaryRowGenerator::GenerateRow({10}, pool.get()), /*bucket=*/0,
                               /*total_bucket=*/std::nullopt, data_increment1,
                               CompactIncrement({}, {}, {}));

    auto file_meta2 = std::make_shared<DataFileMeta>(
        "data-6828284c-e707-49b5-af6b-69be79af120c-0.orc", /*file_size=*/575, /*row_count=*/3,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Bob"), 10, 0, 12.1},
                                          {std::string("Tony"), 10, 0, 14.1}, {0, 0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/2, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888727ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataIncrement data_increment2({file_meta2}, {}, {});
    expected_msgs.emplace_back(BinaryRowGenerator::GenerateRow({10}, pool.get()), /*bucket=*/1,
                               /*total_bucket=*/std::nullopt, data_increment2,
                               CompactIncrement({}, {}, {}));

    auto file_meta3 = std::make_shared<DataFileMeta>(
        "data-8dc7f04c-3c98-48b2-9d56-834d746c4a40-0.orc", /*file_size=*/541, /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Lucy"), 20, 1, 14.1},
                                          {std::string("Lucy"), 20, 1, 14.1}, {0, 0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888743ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataIncrement data_increment3({file_meta3}, {}, {});
    expected_msgs.emplace_back(BinaryRowGenerator::GenerateRow({20}, pool.get()), /*bucket=*/0,
                               /*total_bucket=*/std::nullopt, data_increment3,
                               CompactIncrement({}, {}, {}));

    // check result
    ASSERT_EQ(res_msgs, expected_msgs);

    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::string serialize_ret,
                         CommitMessage::SerializeList(ret, pool));
}

TEST(CommitMessageTest, TestCompatibleWith09JavaPaimon2) {
    // test with commit message version 3
    int32_t version = 3;
    std::string data_path = paimon::test::GetDataDir() +
                            "/orc/append_09.db/append_09/commit_messages/commit_messages-02";
    auto file_system = std::make_shared<LocalFileSystem>();
    auto buffer_length = file_system->GetFileStatus(data_path).value()->GetLen();
    std::vector<uint8_t> buffer(buffer_length, 0);

    ASSERT_OK_AND_ASSIGN(auto in_stream, file_system->Open(data_path));
    ASSERT_OK(in_stream->Read(reinterpret_cast<char*>(buffer.data()), buffer.size()));
    ASSERT_OK(in_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> ret,
                         CommitMessage::DeserializeList(
                             version, reinterpret_cast<char*>(buffer.data()), buffer.size(), pool));
    std::vector<CommitMessageImpl> res_msgs;
    for (const auto& msg : ret) {
        auto msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(msg);
        res_msgs.emplace_back(*msg_impl);
    }

    std::vector<CommitMessageImpl> expected_msgs;
    auto file_meta1 = std::make_shared<DataFileMeta>(
        "data-fd1d2255-43f2-4534-b4cc-08b29e662940-0.orc", /*file_size=*/589, /*row_count=*/3,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Alex"), 10, 0, 12.1},
                                          {std::string("Emily"), 10, 0, 16.1}, {0, 0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/3, /*max_sequence_number=*/5, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724091050427ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataIncrement data_increment1({file_meta1}, {}, {});
    expected_msgs.emplace_back(BinaryRowGenerator::GenerateRow({10}, pool.get()), /*bucket=*/1,
                               /*total_bucket=*/std::nullopt, data_increment1,
                               CompactIncrement({}, {}, {}));
    // handle null value
    auto simple_stats = BinaryRowGenerator::GenerateStats(
        {std::string("Paul"), 20, 1, 0}, {std::string("Paul"), 20, 1, 0}, {0, 0, 0, 1}, pool.get());
    simple_stats.min_values_.SetNullAt(3);
    simple_stats.max_values_.SetNullAt(3);
    auto file_meta2 = std::make_shared<DataFileMeta>(
        "data-7b3f4cc7-116b-4d2f-9c62-5dadc1f11bcb-0.orc", /*file_size=*/506, /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(), simple_stats,
        /*min_sequence_number=*/1, /*max_sequence_number=*/1, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724091050445ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataIncrement data_increment2({file_meta2}, {}, {});
    expected_msgs.emplace_back(BinaryRowGenerator::GenerateRow({20}, pool.get()), /*bucket=*/0,
                               /*total_bucket=*/std::nullopt, data_increment2,
                               CompactIncrement({}, {}, {}));

    // check result
    ASSERT_EQ(res_msgs, expected_msgs);

    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::string serialize_ret,
                         CommitMessage::SerializeList(ret, pool));
}

TEST(CommitMessageTest, TestCompatibleWith09JavaPaimon3) {
    // test with commit message version 3
    int32_t version = 3;
    std::string data_path = paimon::test::GetDataDir() +
                            "/orc/append_09.db/append_09/commit_messages/commit_messages-03";
    auto file_system = std::make_shared<LocalFileSystem>();
    auto buffer_length = file_system->GetFileStatus(data_path).value()->GetLen();
    std::vector<uint8_t> buffer(buffer_length, 0);

    ASSERT_OK_AND_ASSIGN(auto in_stream, file_system->Open(data_path));
    ASSERT_OK(in_stream->Read(reinterpret_cast<char*>(buffer.data()), buffer.size()));
    ASSERT_OK(in_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> ret,
                         CommitMessage::DeserializeList(
                             version, reinterpret_cast<char*>(buffer.data()), buffer.size(), pool));
    std::vector<CommitMessageImpl> res_msgs;
    for (const auto& msg : ret) {
        auto msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(msg);
        res_msgs.emplace_back(*msg_impl);
    }

    std::vector<CommitMessageImpl> expected_msgs;
    auto file_meta1 = std::make_shared<DataFileMeta>(
        "data-2e26b69c-b24a-4760-9654-05b315d7b57f-0.orc", /*file_size=*/541, /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("David"), 10, 0, 17.1},
                                          {std::string("David"), 10, 0, 17.1}, {0, 0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/6, /*max_sequence_number=*/6, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724091126209ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataIncrement data_increment1({file_meta1}, {}, {});
    expected_msgs.emplace_back(BinaryRowGenerator::GenerateRow({10}, pool.get()), /*bucket=*/1,
                               /*total_bucket=*/std::nullopt, data_increment1,
                               CompactIncrement({}, {}, {}));

    // check result
    ASSERT_EQ(res_msgs, expected_msgs);

    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::string serialize_ret,
                         CommitMessage::SerializeList(ret, pool));
}

TEST(CommitMessageTest, TestPkTableCompatibleWithJavaPaimon09) {
    // test with commit message version 3
    int32_t version = 3;
    std::string data_path =
        paimon::test::GetDataDir() +
        "/orc/pk_09_with_dv.db/pk_09_with_dv/commit_messages/commit_messages-01";
    auto file_system = std::make_shared<LocalFileSystem>();
    auto buffer_length = file_system->GetFileStatus(data_path).value()->GetLen();
    std::vector<uint8_t> buffer(buffer_length, 0);

    ASSERT_OK_AND_ASSIGN(auto in_stream, file_system->Open(data_path));
    ASSERT_OK(in_stream->Read(reinterpret_cast<char*>(buffer.data()), buffer.size()));
    ASSERT_OK(in_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> ret,
                         CommitMessage::DeserializeList(
                             version, reinterpret_cast<char*>(buffer.data()), buffer.size(), pool));
    std::vector<CommitMessageImpl> res_msgs;
    for (const auto& msg : ret) {
        auto msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(msg);
        res_msgs.emplace_back(*msg_impl);
    }
    std::vector<CommitMessageImpl> expected_msgs;
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-a7615d0f-aa7f-4523-a3a0-4d9000ceec8c-1.orc", /*file_size=*/833, /*row_count=*/2,
        /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("Bob"), 0}, pool.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("Emily"), 0}, pool.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Bob"), 0}, {std::string("Emily"), 0},
                                          {0, 0}, pool.get()),
        BinaryRowGenerator::GenerateStats({std::string("Bob"), 10, 0, 12.0},
                                          {std::string("Emily"), 10, 0, 113.1}, {0, 0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/4, /*max_sequence_number=*/5, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1747670216104ll, 0),
        /*delete_row_count=*/1, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    auto file_meta_with_level = std::make_shared<DataFileMeta>(
        "data-2eb2a766-97e4-4fe4-88ce-eb606675c101-0.orc", /*file_size=*/789, /*row_count=*/1,
        /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("Bob"), 0}, pool.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("Bob"), 0}, pool.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Bob"), 0}, {std::string("Bob"), 0}, {0, 0},
                                          pool.get()),
        BinaryRowGenerator::GenerateStats({std::string("Bob"), 10, 0, 113.1},
                                          {std::string("Bob"), 10, 0, 113.1}, {0, 0, 0, 0},
                                          pool.get()),
        /*min_sequence_number=*/5, /*max_sequence_number=*/5, /*schema_id=*/0,
        /*level=*/4, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1747670216172ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Compact(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataIncrement data_increment1({file_meta}, {}, {}, {}, {});

    LinkedHashMap<std::string, DeletionVectorMeta> dv_ranges;
    dv_ranges.insert_or_assign(
        "data-a7615d0f-aa7f-4523-a3a0-4d9000ceec8c-0.orc",
        DeletionVectorMeta("data-a7615d0f-aa7f-4523-a3a0-4d9000ceec8c-0.orc", /*offset=*/1,
                           /*length=*/24, /*cardinality=*/std::nullopt));
    auto index_file_meta = std::make_shared<IndexFileMeta>(
        "DELETION_VECTORS", "index-e1bac517-5e97-41ed-a719-e7ee11594946-0", 33, 1, dv_ranges,
        /*external_path=*/std::nullopt);
    expected_msgs.emplace_back(
        BinaryRowGenerator::GenerateRow({10}, pool.get()), /*bucket=*/0,
        /*total_bucket=*/std::nullopt, data_increment1,
        CompactIncrement({file_meta}, {file_meta_with_level}, {}, {index_file_meta}, {}));
    // check result
    ASSERT_EQ(res_msgs, expected_msgs);
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::string serialize_ret,
                         CommitMessage::SerializeList(ret, pool));
}

TEST(CommitMessageTest, TestInvalidMessages) {
    int32_t version = 3;
    std::string data_path = paimon::test::GetDataDir() +
                            "/orc/append_09.db/append_09/commit_messages/commit_messages-01";
    auto file_system = std::make_shared<LocalFileSystem>();
    auto buffer_length = file_system->GetFileStatus(data_path).value()->GetLen();
    std::vector<uint8_t> buffer(buffer_length, 0);

    ASSERT_OK_AND_ASSIGN(auto in_stream, file_system->Open(data_path));
    ASSERT_OK(in_stream->Read(reinterpret_cast<char*>(buffer.data()), buffer.size() - 200));
    ASSERT_OK(in_stream->Close());

    auto pool = GetDefaultPool();
    auto ret = CommitMessage::DeserializeList(version, reinterpret_cast<char*>(buffer.data()),
                                              buffer.size() - 200, pool);
    ASSERT_FALSE(ret.ok());
}

TEST(CommitMessageTest, TestCompatibleWithComplexDataType) {
    int32_t version = 3;
    std::string data_path = paimon::test::GetDataDir() +
                            "/orc/append_09.db/append_09/commit_messages/commit_messages_complex";
    auto file_system = std::make_shared<LocalFileSystem>();
    auto buffer_length = file_system->GetFileStatus(data_path).value()->GetLen();
    std::vector<uint8_t> buffer(buffer_length, 0);

    ASSERT_OK_AND_ASSIGN(auto in_stream, file_system->Open(data_path));
    ASSERT_OK(in_stream->Read(reinterpret_cast<char*>(buffer.data()), buffer.size()));
    ASSERT_OK(in_stream->Close());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> ret,
                         CommitMessage::DeserializeList(
                             version, reinterpret_cast<char*>(buffer.data()), buffer.size(), pool));
    std::vector<CommitMessageImpl> res_msgs;
    for (const auto& msg : ret) {
        auto msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(msg);
        res_msgs.emplace_back(*msg_impl);
    }

    std::vector<CommitMessageImpl> expected_msgs;
    auto file_meta1 = std::make_shared<DataFileMeta>(
        "data-1c67085f-28bd-46ca-9fec-9626feca344c-0.orc", /*file_size=*/1155, /*row_count=*/3,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats(
            {NullType(), NullType(), NullType(), TimestampType(Timestamp(0, 0), 9), 24,
             Decimal(2, 2, 12)},
            {NullType(), NullType(), NullType(), TimestampType(Timestamp(123123, 123000), 9), 2456,
             Decimal(2, 2, 22)},
            {0, 0, 0, 0, 0, 1}, pool.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/2, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1734713760605ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataIncrement data_increment1({file_meta1}, {}, {});
    expected_msgs.emplace_back(BinaryRow::EmptyRow(), /*bucket=*/0, /*total_bucket=*/std::nullopt,
                               data_increment1, CompactIncrement({}, {}, {}));

    // check result
    ASSERT_EQ(res_msgs, expected_msgs);
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::string serialize_ret,
                         CommitMessage::SerializeList(ret, pool));
}

TEST(CommitMessageTest, TestSerialize) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()), arrow::field("f1", arrow::int8()),
        arrow::field("f3", arrow::int16()), arrow::field("non-partition-field", arrow::utf8())};

    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, "mock_format"},
                                                  {Options::MANIFEST_FORMAT, "mock_format"},
                                                  {Options::TARGET_FILE_SIZE, "1024"}};
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), options));
    ASSERT_OK(catalog->CreateDatabase("foo", options, /*ignore_if_exists=*/false));
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema,
                                   /*partition_keys=*/{"f0", "f3"},
                                   /*primary_keys=*/{}, options,
                                   /*ignore_if_exists=*/false));

    WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"),
                                        "commit_user1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                         context_builder.AddOption(Options::FILE_FORMAT, "mock_format")
                             .AddOption(Options::MANIFEST_FORMAT, "mock_format")
                             .AddOption(Options::TARGET_FILE_SIZE, "1024")
                             .Finish());

    std::string root_path = write_context->GetRootPath();
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> file_store_write,
                         FileStoreWrite::Create(std::move(write_context)));

    for (size_t i = 0; i < 10240; i++) {
        auto f0_array = std::make_shared<arrow::Array>();
        auto f1_array = std::make_shared<arrow::Array>();
        auto f3_array = std::make_shared<arrow::Array>();
        auto non_partition_array = std::make_shared<arrow::Array>();
        arrow::BooleanBuilder f0_builder;
        arrow::Int8Builder f1_builder;
        arrow::Int16Builder f3_builder;
        arrow::StringBuilder non_partition_builder;
        for (size_t j = 0; j < 100; j++) {
            ASSERT_TRUE(f0_builder.Append(true).ok());
            ASSERT_TRUE(f1_builder.Append(j).ok());
            ASSERT_TRUE(f3_builder.Append(1).ok());
            ASSERT_TRUE(non_partition_builder.Append(std::to_string(j)).ok());
        }
        ASSERT_TRUE(f0_builder.Finish(&f0_array).ok());
        ASSERT_TRUE(f1_builder.Finish(&f1_array).ok());
        ASSERT_TRUE(f3_builder.Finish(&f3_array).ok());
        ASSERT_TRUE(non_partition_builder.Finish(&non_partition_array).ok());
        auto struct_array =
            arrow::StructArray::Make({f0_array, f1_array, f3_array, non_partition_array}, fields)
                .ValueOrDie();
        ::ArrowArray arrow_array;
        ASSERT_TRUE(arrow::ExportArray(*struct_array, &arrow_array).ok());
        RecordBatchBuilder batch_builder(&arrow_array);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             batch_builder.SetPartition({{"f0", "true"}, {"f3", "1"}}).Finish());
        ASSERT_OK(file_store_write->Write(std::move(batch)));
    }

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> results,
                         file_store_write->PrepareCommit(/*wait_compaction=*/false, 0));
    ASSERT_EQ(results.size(), 1);
    std::shared_ptr<CommitMessage> commit_message = results[0];
    ASSERT_OK_AND_ASSIGN(std::string serialized_commit_message,
                         CommitMessage::Serialize(commit_message, GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<CommitMessage> deserialize_commit_message,
        CommitMessage::Deserialize(7, serialized_commit_message.c_str(),
                                   serialized_commit_message.size(), GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(std::string deserialized_commit_message_str,
                         CommitMessage::ToDebugString(deserialize_commit_message));
    ASSERT_OK_AND_ASSIGN(std::string commit_message_str,
                         CommitMessage::ToDebugString(commit_message));
    ASSERT_EQ(deserialized_commit_message_str, commit_message_str);
    ASSERT_OK_AND_ASSIGN(std::string reserialized_commit_message,
                         CommitMessage::Serialize(deserialize_commit_message, GetDefaultPool()));
    ASSERT_EQ(serialized_commit_message, reserialized_commit_message);

    auto fs = std::make_shared<LocalFileSystem>();
    std::vector<std::unique_ptr<BasicFileStatus>> status_list;
    ASSERT_OK(fs->ListDir(root_path + "/f0=true/f3=1/bucket-0/", &status_list));
    int32_t file_nums = 0;
    for (const auto& file_status : status_list) {
        if (!file_status->IsDir()) {
            file_nums++;
        }
    }
    ASSERT_EQ(file_nums, 10);
    ASSERT_OK(fs->Delete(root_path));
}

}  // namespace paimon::test
