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

#include "paimon/format/orc/orc_file_batch_reader.h"

#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "orc/OrcFile.hh"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/schema/arrow_schema_validator.h"
#include "paimon/format/orc/orc_adapter.h"
#include "paimon/format/orc/orc_format_defs.h"
#include "paimon/format/orc/orc_input_stream_impl.h"
#include "paimon/format/orc/orc_memory_pool.h"
#include "paimon/format/orc/orc_metrics.h"
#include "paimon/format/orc/predicate_converter.h"
namespace paimon::orc {
OrcFileBatchReader::OrcFileBatchReader(const std::string& file_name, int32_t batch_size,
                                       std::unique_ptr<::orc::ReaderMetrics>&& reader_metrics,
                                       std::unique_ptr<::orc::Reader>&& reader,
                                       const std::map<std::string, std::string>& options,
                                       std::unique_ptr<arrow::MemoryPool>&& arrow_pool,
                                       const std::shared_ptr<::orc::MemoryPool>& orc_pool)
    : file_name_(file_name),
      batch_size_(batch_size),
      options_(options),
      arrow_pool_(std::move(arrow_pool)),
      orc_pool_(orc_pool),
      reader_metrics_(std::move(reader_metrics)),
      reader_(std::move(reader)),
      metrics_(std::make_shared<MetricsImpl>()) {}

Result<std::unique_ptr<OrcFileBatchReader>> OrcFileBatchReader::Create(
    std::unique_ptr<::orc::InputStream>&& input_stream, const std::shared_ptr<MemoryPool>& pool,
    const std::map<std::string, std::string>& options, int32_t batch_size) {
    assert(input_stream);
    std::string file_name = input_stream->getName();
    try {
        ::orc::ReaderOptions reader_options;
        if (pool == nullptr) {
            return Status::Invalid("memory pool is nullptr");
        }
        auto orc_pool = std::make_shared<OrcMemoryPool>(pool);
        reader_options.setMemoryPool(*orc_pool);

        std::unique_ptr<::orc::ReaderMetrics> reader_metrics;
        PAIMON_ASSIGN_OR_RAISE(
            bool read_enable_metrics,
            OptionsUtils::GetValueFromMap<bool>(options, ORC_READ_ENABLE_METRICS, false));
        if (read_enable_metrics) {
            reader_metrics = std::make_unique<::orc::ReaderMetrics>();
            reader_options.setReaderMetrics(reader_metrics.get());
            auto orc_input_stream = dynamic_cast<OrcInputStreamImpl*>(input_stream.get());
            if (orc_input_stream) {
                orc_input_stream->SetMetrics(reader_metrics.get());
            }
        }
        std::unique_ptr<::orc::Reader> reader =
            ::orc::createReader(std::move(input_stream), reader_options);
        auto orc_file_batch_reader = std::unique_ptr<OrcFileBatchReader>(
            new OrcFileBatchReader(file_name, batch_size, std::move(reader_metrics),
                                   std::move(reader), options, GetArrowPool(pool), orc_pool));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::ArrowSchema> file_schema,
                               orc_file_batch_reader->GetFileSchema());
        PAIMON_RETURN_NOT_OK(orc_file_batch_reader->SetReadSchema(
            file_schema.get(), /*predicate=*/nullptr, /*selection_bitmap=*/std::nullopt));
        return orc_file_batch_reader;
    } catch (const std::exception& e) {
        return Status::Invalid(fmt::format(
            "create orc file batch reader failed for file {}, with {} error", file_name, e.what()));
    } catch (...) {
        return Status::UnknownError(fmt::format(
            "create orc file batch reader failed for file {}, with unknown error", file_name));
    }
}

Result<std::unique_ptr<::ArrowSchema>> OrcFileBatchReader::GetFileSchema() const {
    assert(reader_);
    const auto& orc_file_type = reader_->getType();
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::DataType> arrow_file_type,
                           OrcAdapter::GetArrowType(&orc_file_type));
    auto c_schema = std::make_unique<::ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportType(*arrow_file_type, c_schema.get()));
    return c_schema;
}

Status OrcFileBatchReader::SetReadSchema(::ArrowSchema* read_schema,
                                         const std::shared_ptr<Predicate>& predicate,
                                         const std::optional<RoaringBitmap32>& selection_bitmap) {
    if (!read_schema) {
        return Status::Invalid("SetReadSchema failed: read schema cannot be nullptr");
    }
    if (selection_bitmap) {
        // TODO(liancheng.lsz): support bitmap
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema,
                                      arrow::ImportSchema(read_schema));
    if (ArrowSchemaValidator::ContainTimestampWithTimezone(
            *arrow::struct_(arrow_schema->fields()))) {
        PAIMON_ASSIGN_OR_RAISE(bool ltz_legacy, OptionsUtils::GetValueFromMap<bool>(
                                                    options_, ORC_TIMESTAMP_LTZ_LEGACY_TYPE, true));
        if (ltz_legacy) {
            return Status::Invalid(
                "invalid config, do not support reading timestamp with timezone in legacy format "
                "for orc");
        }
    }
    PAIMON_ASSIGN_OR_RAISE(auto orc_target_type, OrcAdapter::GetOrcType(*arrow_schema));
    const auto& orc_src_type = reader_->getType();
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::orc::SearchArgument> search_arg,
                           PredicateConverter::Convert(orc_src_type, predicate));
    target_type_ = arrow::struct_(arrow_schema->fields());
    PAIMON_ASSIGN_OR_RAISE(::orc::RowReaderOptions row_reader_options,
                           CreateRowReaderOptions(&orc_src_type, orc_target_type.get(),
                                                  std::move(search_arg), options_));
    try {
        row_reader_ = reader_->createRowReader(row_reader_options);
    } catch (const std::exception& e) {
        return Status::Invalid(
            fmt::format("orc file batch reader create row reader failed for file {}, with {} error",
                        file_name_, e.what()));
    } catch (...) {
        return Status::UnknownError(fmt::format(
            "orc file batch reader create row reader failed for file {}, with unknown error",
            file_name_));
    }
    return Status::OK();
}

Status OrcFileBatchReader::SeekToRow(uint64_t row_number) {
    try {
        row_reader_->seekToRow(row_number);
    } catch (const std::exception& e) {
        return Status::Invalid(
            fmt::format("orc file batch reader seek to row {} failed for file {}, with {} error",
                        row_number, file_name_, e.what()));
    } catch (...) {
        return Status::UnknownError(fmt::format(
            "orc file batch reader seek to row {} failed for file {}, with unknown error",
            row_number, file_name_));
    }
    return Status::OK();
}

Result<BatchReader::ReadBatch> OrcFileBatchReader::NextBatch() {
    if (has_error_) {
        return Status::Invalid(fmt::format(
            "Since an error has occurred, next batch has been prohibited. file '{}'", file_name_));
    }
    std::unique_ptr<ArrowArray> c_array = std::make_unique<ArrowArray>();
    std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
    try {
        auto orc_batch = row_reader_->createRowBatch(batch_size_);
        bool eof = !row_reader_->next(*orc_batch);
        if (eof) {
            return BatchReader::MakeEofBatch();
        }
        ScopeGuard guard([this]() { has_error_ = true; });
        assert(orc_batch->numElements > 0);
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::Array> array,
            OrcAdapter::AppendBatch(target_type_, orc_batch.get(), arrow_pool_.get()));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, c_array.get(), c_schema.get()));
        guard.Release();
    } catch (const std::exception& e) {
        return Status::Invalid(
            fmt::format("orc file batch reader get next batch failed for file {}, with {} error",
                        file_name_, e.what()));
    } catch (...) {
        return Status::UnknownError(fmt::format(
            "orc file batch reader get next batch failed for file {}, with unknown error",
            file_name_));
    }
    return make_pair(std::move(c_array), std::move(c_schema));
}

std::shared_ptr<Metrics> OrcFileBatchReader::GetReaderMetrics() const {
    if (reader_metrics_) {
        metrics_->SetCounter(OrcMetrics::READ_INCLUSIVE_LATENCY_US,
                             reader_metrics_->ReaderInclusiveLatencyUs);
        metrics_->SetCounter(OrcMetrics::READ_IO_COUNT, reader_metrics_->IOCount);
    }
    return metrics_;
}

Result<::orc::RowReaderOptions> OrcFileBatchReader::CreateRowReaderOptions(
    const ::orc::Type* src_type, const ::orc::Type* target_type,
    std::unique_ptr<::orc::SearchArgument>&& search_arg,
    const std::map<std::string, std::string>& options) {
    ::orc::RowReaderOptions row_reader_options;
    std::list<std::string> include_fields;
    std::unordered_map<std::string, const ::orc::Type*> src_type_map;
    for (uint64_t i = 0; i < src_type->getSubtypeCount(); i++) {
        src_type_map[src_type->getFieldName(i)] = src_type->getSubtype(i);
    }
    int64_t prev_target_field_col_id = -1;
    for (uint64_t i = 0; i < target_type->getSubtypeCount(); i++) {
        auto& field_name = target_type->getFieldName(i);
        auto iter = src_type_map.find(field_name);
        if (iter == src_type_map.end()) {
            return Status::Invalid(
                fmt::format("field {} not in file schema {}", field_name, src_type->toString()));
        }
        // Noted that: do not support recall partial fields in nested type
        if (iter->second->toString() != target_type->getSubtype(i)->toString()) {
            return Status::Invalid(
                fmt::format("target_type {} not match src_type {}, mismatch field name {}",
                            target_type->toString(), src_type->toString(), field_name));
        }
        int64_t target_field_col_id = iter->second->getColumnId();
        if (prev_target_field_col_id >= target_field_col_id) {
            return Status::Invalid(
                "The column id of the target field should be monotonically increasing in format "
                "reader");
        }
        prev_target_field_col_id = target_field_col_id;
        include_fields.push_back(field_name);
    }
    row_reader_options.include(include_fields);
    // In order to avoid issue like https://github.com/alibaba/paimon-cpp/issues/42, we explicitly
    // set GMT timezone.
    row_reader_options.setTimezoneName("GMT");
    row_reader_options.searchArgument(std::move(search_arg));

    PAIMON_ASSIGN_OR_RAISE(
        bool enable_lazy_decoding,
        OptionsUtils::GetValueFromMap<bool>(options, ORC_READ_ENABLE_LAZY_DECODING, false));
    row_reader_options.setEnableLazyDecoding(enable_lazy_decoding);

    // always use tight numeric vector
    row_reader_options.setUseTightNumericVector(true);

    return row_reader_options;
}

}  // namespace paimon::orc
