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

#include "paimon/format/lance/lance_file_batch_reader.h"

#include "arrow/api.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/format/lance/lance_utils.h"
namespace paimon::lance {
LanceFileBatchReader::LanceFileBatchReader(LanceFileReader* file_reader, int32_t batch_size,
                                           int32_t batch_readahead, uint64_t num_rows,
                                           std::string&& error_message)
    : batch_size_(batch_size),
      batch_readahead_(batch_readahead),
      num_rows_(num_rows),
      error_message_(std::move(error_message)),
      file_reader_(file_reader),
      metrics_(std::make_shared<MetricsImpl>()) {}

Result<std::unique_ptr<LanceFileBatchReader>> LanceFileBatchReader::Create(
    const std::string& file_path, int32_t batch_size, int32_t batch_readahead) {
    PAIMON_ASSIGN_OR_RAISE(std::string normalized_path,
                           LanceUtils::NormalizeLanceFilePath(file_path));
    std::string error_message;
    error_message.resize(1024, '\0');
    LanceFileReader* file_reader = nullptr;
    int32_t err_code = create_reader(normalized_path.data(), &file_reader, error_message.data(),
                                     error_message.size());
    PAIMON_RETURN_NOT_OK(LanceToPaimonStatus(err_code, error_message));
    assert(file_reader);

    uint64_t num_of_rows = 0;
    err_code = num_rows(file_reader, &num_of_rows, error_message.data(), error_message.size());
    PAIMON_RETURN_NOT_OK(LanceToPaimonStatus(err_code, error_message));

    return std::unique_ptr<LanceFileBatchReader>(new LanceFileBatchReader(
        file_reader, batch_size, batch_readahead, num_of_rows, std::move(error_message)));
}

LanceFileBatchReader::~LanceFileBatchReader() {
    DoClose();
}

Result<std::unique_ptr<::ArrowSchema>> LanceFileBatchReader::GetFileSchema() const {
    auto c_schema = std::make_unique<::ArrowSchema>();
    int32_t err_code =
        get_schema(file_reader_, c_schema.get(), error_message_.data(), error_message_.size());
    PAIMON_RETURN_NOT_OK(LanceToPaimonStatus(err_code, error_message_));
    return c_schema;
}

Status LanceFileBatchReader::SetReadSchema(::ArrowSchema* read_schema,
                                           const std::shared_ptr<Predicate>& predicate,
                                           const std::optional<RoaringBitmap32>& selection_bitmap) {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema,
                                      arrow::ImportSchema(read_schema));
    read_field_names_ = arrow_schema->field_names();
    assert(!read_field_names_.empty());
    if (selection_bitmap) {
        read_row_ids_.clear();
        read_row_ids_.reserve(selection_bitmap.value().Cardinality());
        for (auto iter = selection_bitmap.value().Begin(); iter != selection_bitmap.value().End();
             ++iter) {
            read_row_ids_.push_back(*iter);
        }
    }
    // reset stream_reader_ for new read schema
    if (stream_reader_) {
        int32_t err_code =
            release_stream_reader(stream_reader_, error_message_.data(), error_message_.size());
        PAIMON_RETURN_NOT_OK(LanceToPaimonStatus(err_code, error_message_));
        stream_reader_ = nullptr;
    }
    return Status::OK();
}

Result<BatchReader::ReadBatch> LanceFileBatchReader::NextBatch() {
    if (!stream_reader_) {
        std::vector<const char*> c_field_names;
        for (const auto& name : read_field_names_) {
            c_field_names.push_back(name.data());
        }
        // if not set read schema, read_field_names_ is empty, read all fields in file
        int32_t err_code = create_stream_reader(
            file_reader_, &stream_reader_, batch_size_, batch_readahead_, c_field_names.data(),
            c_field_names.size(), read_row_ids_.data(), read_row_ids_.size(), error_message_.data(),
            error_message_.size());
        PAIMON_RETURN_NOT_OK(LanceToPaimonStatus(err_code, error_message_));
        assert(stream_reader_);
    }
    auto c_array = std::make_unique<ArrowArray>();
    auto c_schema = std::make_unique<ArrowSchema>();
    bool is_eof = false;
    int32_t err_code = next_batch(stream_reader_, c_array.get(), c_schema.get(), &is_eof,
                                  error_message_.data(), error_message_.size());
    PAIMON_RETURN_NOT_OK(LanceToPaimonStatus(err_code, error_message_));
    if (is_eof) {
        return BatchReader::MakeEofBatch();
    }
    return std::make_pair(std::move(c_array), std::move(c_schema));
}

void LanceFileBatchReader::DoClose() {
    if (stream_reader_) {
        [[maybe_unused]] int32_t err_code =
            release_stream_reader(stream_reader_, error_message_.data(), error_message_.size());
        stream_reader_ = nullptr;
    }
    if (file_reader_) {
        [[maybe_unused]] int32_t err_code =
            release_reader(file_reader_, error_message_.data(), error_message_.size());
        file_reader_ = nullptr;
    }
}

}  // namespace paimon::lance
