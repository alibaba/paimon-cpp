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

#pragma once

#include "arrow/c/bridge.h"
#include "arrow/ipc/api.h"
#include "paimon/api.h"
#include "paimon/catalog/catalog.h"
#include "paimon/commit_context.h"
#include "paimon/common/data/blob_descriptor.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/operation/append_only_file_store_write.h"
#include "paimon/core/operation/file_store_commit_impl.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/data/blob.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"
#include "paimon/table/source/startup_mode.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"
namespace paimon::test {

class TestHelper {
 public:
    static Result<std::unique_ptr<TestHelper>> Create(
        const std::string& root_path, const std::shared_ptr<arrow::Schema>& schema,
        const std::vector<std::string>& partition_keys,
        const std::vector<std::string>& primary_keys,
        const std::map<std::string, std::string>& options, bool is_streaming_mode) {
        // only for test && only check the key
        auto new_options = options;
        new_options["enable-object-store-catalog-in-inte-test"] = "";
        PAIMON_ASSIGN_OR_RAISE(auto catalog, Catalog::Create(root_path, new_options));
        PAIMON_RETURN_NOT_OK(
            catalog->CreateDatabase("foo", new_options, /*ignore_if_exists=*/false));
        ::ArrowSchema c_schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &c_schema));
        PAIMON_RETURN_NOT_OK(catalog->CreateTable(Identifier("foo", "bar"), &c_schema,
                                                  partition_keys, primary_keys, new_options,
                                                  /*ignore_if_exists=*/false));
        std::string table_path = PathUtil::JoinPath(root_path, "foo.db/bar");
        return Create(table_path, new_options, is_streaming_mode);
    }

    static Result<std::unique_ptr<TestHelper>> Create(
        const std::string& table_path, const std::map<std::string, std::string>& options,
        bool is_streaming_mode) {
        std::string file_system_identifier = "local";
        auto fs_iter = options.find(Options::FILE_SYSTEM);
        if (fs_iter != options.end()) {
            file_system_identifier = StringUtils::ToLowerCase(fs_iter->second);
        }
        PAIMON_ASSIGN_OR_RAISE(auto file_system,
                               FileSystemFactory::Get(file_system_identifier, table_path, options));
        std::string commit_user = "commit_user";
        WriteContextBuilder context_builder(table_path, commit_user);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriteContext> write_context,
                               context_builder.SetOptions(options)
                                   .WithStreamingMode(is_streaming_mode)
                                   .WithWriteId(12345)
                                   .Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreWrite> write,
                               FileStoreWrite::Create(std::move(write_context)));
        std::map<std::string, std::string> new_options = options;
        // only for test && only check the key
        new_options["enable-pk-commit-in-inte-test"] = "";
        new_options["enable-object-store-commit-in-inte-test"] = "";
        CommitContextBuilder commit_context_builder(table_path, commit_user);
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<CommitContext> commit_context,
            commit_context_builder.SetOptions(new_options).IgnoreEmptyCommit(false).Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> commit,
                               FileStoreCommit::Create(std::move(commit_context)));
        return std::unique_ptr<TestHelper>(new TestHelper(std::move(file_system), std::move(write),
                                                          std::move(commit), commit_user,
                                                          table_path, options));
    }

    static Result<std::unique_ptr<RecordBatch>> MakeRecordBatch(
        const std::shared_ptr<arrow::DataType>& data_type, const std::string& data_str,
        const std::map<std::string, std::string>& partition_map, int32_t bucket,
        const std::vector<RecordBatch::RowKind>& row_kinds) {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            auto array, arrow::ipc::internal::json::ArrayFromJSON(data_type, data_str));
        ::ArrowArray arrow_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &arrow_array));
        RecordBatchBuilder batch_builder(&arrow_array);
        return batch_builder.SetPartition(partition_map)
            .SetBucket(bucket)
            .SetRowKinds(row_kinds)
            .Finish();
    }

    Result<std::vector<std::shared_ptr<CommitMessage>>> WriteAndCommit(
        std::unique_ptr<RecordBatch>&& record_batch, int64_t commit_identifier,
        const std::optional<std::vector<std::shared_ptr<CommitMessage>>>&
            expected_commit_messages) {
        std::vector<std::unique_ptr<RecordBatch>> batches;
        batches.emplace_back(std::move(record_batch));
        return WriteAndCommit(std::move(batches), commit_identifier, expected_commit_messages);
    }

    Result<std::vector<std::shared_ptr<CommitMessage>>> WriteAndCommit(
        std::vector<std::unique_ptr<RecordBatch>>&& record_batches, int64_t commit_identifier,
        const std::optional<std::vector<std::shared_ptr<CommitMessage>>>&
            expected_commit_messages) {
        for (auto& record_batch : record_batches) {
            PAIMON_RETURN_NOT_OK(write_->Write(std::move(record_batch)));
        }
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<CommitMessage>> commit_messages,
                               write_->PrepareCommit(/*wait_compaction=*/false, commit_identifier));
        if (expected_commit_messages) {
            CheckCommitMessages(expected_commit_messages.value(), commit_messages);
            CheckExternalPath(commit_messages);
        }
        PAIMON_RETURN_NOT_OK(commit_->Commit(commit_messages, commit_identifier));
        return commit_messages;
    }

    Result<std::vector<std::shared_ptr<Split>>> NewScan(StartupMode startup_mode,
                                                        std::optional<int64_t> snapshot_id,
                                                        bool is_streaming = true) {
        ScanContextBuilder scan_context_builder(table_path_);
        scan_context_builder.WithStreamingMode(is_streaming)
            .SetOptions(options_)
            .AddOption(Options::SCAN_MODE, startup_mode.ToString());
        if (snapshot_id) {
            scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID,
                                           std::to_string(snapshot_id.value()));
        }
        PAIMON_ASSIGN_OR_RAISE(auto scan_context, scan_context_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(scan_, TableScan::Create(std::move(scan_context)));
        return Scan();
    }

    Result<std::vector<std::shared_ptr<Split>>> Scan() {
        if (scan_ == nullptr) {
            return Status::Invalid("need call NewScan first");
        }
        PAIMON_ASSIGN_OR_RAISE(auto result_plan, scan_->CreatePlan());
        return result_plan->Splits();
    }

    static Result<bool> CheckBlobsEqual(const std::vector<std::shared_ptr<Blob>>& result_blobs,
                                        const std::vector<std::shared_ptr<Blob>>& expected_blobs,
                                        const std::shared_ptr<FileSystem>& fs) {
        if (result_blobs.size() != expected_blobs.size()) {
            std::cout << "[result_blobs.size]: " << result_blobs.size() << std::endl;
            std::cout << "[expected_blobs.size]: " << expected_blobs.size() << std::endl;
            return false;
        }
        for (uint32_t i = 0; i < result_blobs.size(); ++i) {
            PAIMON_ASSIGN_OR_RAISE(auto result_stream, result_blobs[i]->NewInputStream(fs));
            PAIMON_ASSIGN_OR_RAISE(auto expected_stream, expected_blobs[i]->NewInputStream(fs));

            PAIMON_ASSIGN_OR_RAISE(uint64_t result_length, result_stream->Length());
            PAIMON_ASSIGN_OR_RAISE(uint64_t expected_length, expected_stream->Length());
            if (result_length != expected_length) {
                auto result_descriptor_bytes = result_blobs[i]->ToDescriptor(GetDefaultPool());
                auto expected_descriptor_bytes = expected_blobs[i]->ToDescriptor(GetDefaultPool());
                PAIMON_ASSIGN_OR_RAISE(
                    auto result_descriptor,
                    BlobDescriptor::Deserialize(result_descriptor_bytes->data(),
                                                result_descriptor_bytes->size()));
                PAIMON_ASSIGN_OR_RAISE(
                    auto expected_descriptor,
                    BlobDescriptor::Deserialize(expected_descriptor_bytes->data(),
                                                expected_descriptor_bytes->size()));
                std::cout << "blobs[" << i << "]: " << std::endl;
                std::cout << "[result_length(" << result_length << ") != expected_length("
                          << expected_length << ")]" << std::endl;
                std::cout << "[result_descriptor]: " << result_descriptor->ToString() << std::endl;
                std::cout << "[expected_descriptor]: " << expected_descriptor->ToString()
                          << std::endl;
                return false;
            }

            std::vector<char> result_bytes(result_length, 0);
            std::vector<char> expected_bytes(expected_length, 0);
            PAIMON_RETURN_NOT_OK(result_stream->Read(result_bytes.data(), result_length));
            PAIMON_RETURN_NOT_OK(expected_stream->Read(expected_bytes.data(), expected_length));
            if (result_bytes != expected_bytes) {
                std::cout << "blobs[" << i << "]: " << std::endl;
                std::cout << "[result_bytes != expected_bytes]" << std::endl;
                return false;
            }
        }
        return true;
    }

    static Result<std::vector<std::shared_ptr<Blob>>> ToBlobs(
        const std::shared_ptr<arrow::StructArray>& blob_struct_array) {
        std::vector<std::shared_ptr<Blob>> result_blobs;
        auto child_array = blob_struct_array->field(0);
        assert(blob_struct_array->num_fields() == 1);
        assert(blob_struct_array->null_count() == 0);
        assert(child_array->null_count() == 0);
        assert(child_array->type_id() == arrow::Type::type::LARGE_BINARY);

        const auto& blob_array =
            arrow::internal::checked_cast<const arrow::LargeBinaryArray&>(*child_array);
        for (int64_t i = 0; i < blob_array.length(); ++i) {
            std::string_view descriptor = blob_array.GetView(i);
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Blob> blob,
                                   Blob::FromDescriptor(descriptor.data(), descriptor.size()));
            result_blobs.emplace_back(blob);
        }
        return result_blobs;
    }

    // need to reconstruct the blob array, because the array in read result do not have blob meta
    Result<std::shared_ptr<arrow::Array>> ReconstructBlobArray(
        const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::Schema>& schema) {
        ::ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
        ::ArrowSchema new_c_schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &new_c_schema));
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(auto new_array,
                                          arrow::ImportArray(&c_array, &new_c_schema));
        return new_array;
    }

    Result<bool> ReadAndCheckResultForBlobTable(
        const std::shared_ptr<arrow::Schema>& all_columns_schema,
        const std::vector<std::shared_ptr<Split>>& splits, const std::string& main_expected_json,
        const std::vector<PAIMON_UNIQUE_PTR<Bytes>>& expected_blob_descriptors) {
        ReadContextBuilder read_context_builder(table_path_);
        read_context_builder.SetOptions(options_);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> read_context,
                               read_context_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto table_read, TableRead::Create(std::move(read_context)));
        PAIMON_ASSIGN_OR_RAISE(auto batch_reader, table_read->CreateReader(splits));
        PAIMON_ASSIGN_OR_RAISE(auto read_result,
                               ReadResultCollector::CollectResult(batch_reader.get()));

        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(auto concat_array,
                                          arrow::Concatenate(read_result->chunks()));
        PAIMON_ASSIGN_OR_RAISE(auto reconstruct_array,
                               ReconstructBlobArray(concat_array, all_columns_schema));
        PAIMON_ASSIGN_OR_RAISE(
            auto separated_array,
            BlobUtils::SeparateBlobArray(
                std::dynamic_pointer_cast<arrow::StructArray>(reconstruct_array)));

        arrow::EqualOptions equal_options = arrow::EqualOptions::Defaults();

        // check main columns
        auto separated_schema = BlobUtils::SeparateBlobSchema(all_columns_schema);
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            auto main_expected_array,
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_(separated_schema.main_schema->fields()), main_expected_json));
        auto main_expected_chunk_array = std::make_shared<arrow::ChunkedArray>(main_expected_array);
        bool main_equal = main_expected_chunk_array->Equals(
            arrow::ChunkedArray(separated_array.main_array), equal_options.diff_sink(&std::cout));
        if (!main_equal) {
            std::cout << "[expected_data_type]" << main_expected_chunk_array->type()->ToString()
                      << std::endl;
            std::cout << "[actual_data_type]" << separated_array.main_array->type()->ToString()
                      << std::endl;
            std::cout << "[expected]:" << main_expected_chunk_array->ToString() << std::endl;
            std::cout << "[actual]: " << separated_array.main_array->ToString() << std::endl;
        }

        // check blob column
        std::vector<std::shared_ptr<Blob>> expected_blobs;
        for (const auto& descriptor : expected_blob_descriptors) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Blob> blob,
                                   Blob::FromDescriptor(descriptor->data(), descriptor->size()));
            expected_blobs.emplace_back(blob);
        }
        PAIMON_ASSIGN_OR_RAISE(auto result_blobs, ToBlobs(separated_array.blob_array));
        PAIMON_ASSIGN_OR_RAISE(bool blob_equal, CheckBlobsEqual(result_blobs, expected_blobs, fs_));

        table_read.reset();
        return main_equal && blob_equal;
    }

    Result<bool> ReadAndCheckResult(const std::shared_ptr<arrow::DataType>& data_type,
                                    const std::vector<std::shared_ptr<Split>>& splits,
                                    const std::string& expected_result) {
        ReadContextBuilder read_context_builder(table_path_);
        read_context_builder.SetOptions(options_);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> read_context,
                               read_context_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto table_read, TableRead::Create(std::move(read_context)));
        PAIMON_ASSIGN_OR_RAISE(auto batch_reader, table_read->CreateReader(splits));
        PAIMON_ASSIGN_OR_RAISE(auto read_result,
                               ReadResultCollector::CollectResult(batch_reader.get()));
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            auto expected_array,
            arrow::ipc::internal::json::ArrayFromJSON(data_type, expected_result));
        auto expected_chunk_array = std::make_shared<arrow::ChunkedArray>(expected_array);

        arrow::EqualOptions equal_options = arrow::EqualOptions::Defaults();
        bool is_equal =
            expected_chunk_array->Equals(read_result, equal_options.diff_sink(&std::cout));

        if (!is_equal) {
            std::cout << "[expected_data_type]" << expected_chunk_array->type()->ToString()
                      << std::endl;
            std::cout << "[actual_data_type]" << read_result->type()->ToString() << std::endl;
            std::cout << "[expected]:" << expected_chunk_array->ToString() << std::endl;
            std::cout << "[actual]: " << read_result->ToString() << std::endl;
        }
        table_read.reset();
        return is_equal;
    }

    Result<std::optional<Snapshot>> LatestSnapshot() const {
        auto commit_impl = dynamic_cast<FileStoreCommitImpl*>(commit_.get());
        return commit_impl->snapshot_manager_->LatestSnapshotOfUser(commit_user_);
    }

    Result<std::optional<std::shared_ptr<TableSchema>>> LatestSchema() const {
        auto commit_impl = dynamic_cast<FileStoreCommitImpl*>(commit_.get());
        return commit_impl->schema_manager_->Latest();
    }

    Result<std::string> PartitionStr(const BinaryRow& partition) const {
        auto abstract_write = dynamic_cast<AbstractFileStoreWrite*>(write_.get());
        return abstract_write->file_store_path_factory_->GetPartitionString(partition);
    }

    static void CheckCommitMessages(std::vector<std::shared_ptr<CommitMessage>> expected,
                                    std::vector<std::shared_ptr<CommitMessage>> actual) {
        ASSERT_EQ(expected.size(), actual.size());
        auto commit_message_sort_function = [](const std::shared_ptr<CommitMessage>& lhs,
                                               const std::shared_ptr<CommitMessage>& rhs) -> bool {
            auto lhs_impl = std::dynamic_pointer_cast<CommitMessageImpl>(lhs);
            auto rhs_impl = std::dynamic_pointer_cast<CommitMessageImpl>(rhs);
            if (lhs_impl->Partition() == rhs_impl->Partition()) {
                return lhs_impl->Bucket() < rhs_impl->Bucket();
            }
            return lhs_impl->Partition().HashCode() < rhs_impl->Partition().HashCode();
        };
        std::stable_sort(expected.begin(), expected.end(), commit_message_sort_function);
        std::stable_sort(actual.begin(), actual.end(), commit_message_sort_function);
        for (size_t i = 0; i < actual.size(); i++) {
            auto result_impl = std::dynamic_pointer_cast<CommitMessageImpl>(actual[i]);
            auto expect_impl = std::dynamic_pointer_cast<CommitMessageImpl>(expected[i]);
            bool is_equal = result_impl->TEST_Equal(*expect_impl);
            if (!is_equal) {
                std::cout << "actual_msg:" << result_impl->ToString() << std::endl;
                std::cout << "expect_msg:" << expect_impl->ToString() << std::endl;
            }
            ASSERT_TRUE(is_equal);
        }
    }

 private:
    void CheckExternalPath(const std::vector<std::shared_ptr<CommitMessage>>& actuals) {
        for (const auto& actual : actuals) {
            auto msg = std::dynamic_pointer_cast<CommitMessageImpl>(actual);
            auto files = msg->GetNewFilesIncrement().NewFiles();
            for (auto& file : files) {
                if (file->external_path) {
                    ASSERT_OK_AND_ASSIGN(bool file_exist, fs_->Exists(file->external_path.value()));
                    ASSERT_TRUE(file_exist);
                }
            }
        }
    }

    TestHelper(std::unique_ptr<FileSystem> fs, std::unique_ptr<FileStoreWrite> write,
               std::unique_ptr<FileStoreCommit> commit, const std::string& commit_user,
               const std::string& table_path, const std::map<std::string, std::string>& options)
        : fs_(std::move(fs)),
          write_(std::move(write)),
          commit_(std::move(commit)),
          commit_user_(commit_user),
          table_path_(table_path),
          options_(options) {}

    const std::string& CommitUser() const {
        return commit_user_;
    }

    std::shared_ptr<FileSystem> fs_;
    std::unique_ptr<FileStoreWrite> write_;
    std::unique_ptr<FileStoreCommit> commit_;
    std::unique_ptr<TableScan> scan_;
    std::string commit_user_;
    std::string table_path_;
    std::map<std::string, std::string> options_;
};

}  // namespace paimon::test
