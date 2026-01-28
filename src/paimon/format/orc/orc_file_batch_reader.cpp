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

OrcFileBatchReader::OrcFileBatchReader(std::unique_ptr<::orc::ReaderMetrics>&& reader_metrics,
                                       std::unique_ptr<OrcReaderWrapper>&& reader,
                                       const std::map<std::string, std::string>& options,
                                       const std::shared_ptr<arrow::MemoryPool>& arrow_pool,
                                       const std::shared_ptr<::orc::MemoryPool>& orc_pool)
    : options_(options),
      arrow_pool_(arrow_pool),
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
        uint64_t natural_read_size = input_stream->getNaturalReadSize();
        auto orc_pool = std::make_shared<OrcMemoryPool>(pool);
        std::shared_ptr<arrow::MemoryPool> arrow_pool = GetArrowPool(pool);
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

        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<OrcReaderWrapper> reader_wrapper,
            OrcReaderWrapper::Create(std::move(reader), file_name, batch_size, natural_read_size,
                                     options, arrow_pool, orc_pool));
        auto orc_file_batch_reader = std::unique_ptr<OrcFileBatchReader>(new OrcFileBatchReader(
            std::move(reader_metrics), std::move(reader_wrapper), options, arrow_pool, orc_pool));
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
    const auto& orc_file_type = reader_->GetOrcType();
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
    const auto& orc_src_type = reader_->GetOrcType();
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::orc::SearchArgument> search_arg,
                           PredicateConverter::Convert(orc_src_type, predicate));
    auto target_type = arrow::struct_(arrow_schema->fields());
    std::vector<uint64_t> target_column_ids;
    PAIMON_ASSIGN_OR_RAISE(
        ::orc::RowReaderOptions row_reader_options,
        CreateRowReaderOptions(&orc_src_type, orc_target_type.get(), std::move(search_arg),
                               options_, &target_column_ids));

    target_column_ids_ = target_column_ids;
    PAIMON_RETURN_NOT_OK(reader_->SetReadSchema(target_type, row_reader_options));
    return Status::OK();
}

Status OrcFileBatchReader::SeekToRow(uint64_t row_number) {
    return reader_->SeekToRow(row_number);
}

Result<std::vector<std::pair<uint64_t, uint64_t>>> OrcFileBatchReader::PreBufferRange() {
    return reader_->PreBufferRange(target_column_ids_);
}

Result<BatchReader::ReadBatch> OrcFileBatchReader::NextBatch() {
    return reader_->Next();
}

std::shared_ptr<Metrics> OrcFileBatchReader::GetReaderMetrics() const {
    if (reader_metrics_) {
        metrics_->SetCounter(OrcMetrics::READ_INCLUSIVE_LATENCY_US,
                             reader_metrics_->ReaderInclusiveLatencyUs);
        metrics_->SetCounter(OrcMetrics::READ_IO_COUNT, reader_metrics_->IOCount);
    }
    return metrics_;
}

Result<std::list<std::string>> OrcFileBatchReader::GetAndCheckIncludedFields(
    const ::orc::Type* src_type, const ::orc::Type* target_type,
    std::vector<uint64_t>* target_column_ids) {
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
        GetSubColumnIds(iter->second, target_column_ids);
        if (prev_target_field_col_id >= target_field_col_id) {
            return Status::Invalid(
                "The column id of the target field should be monotonically increasing in "
                "format reader");
        }
        prev_target_field_col_id = target_field_col_id;
        include_fields.push_back(field_name);
    }
    return include_fields;
}

void OrcFileBatchReader::GetSubColumnIds(const ::orc::Type* type, std::vector<uint64_t>* col_ids) {
    col_ids->push_back(type->getColumnId());
    for (uint64_t i = 0; i < type->getSubtypeCount(); i++) {
        GetSubColumnIds(type->getSubtype(i), col_ids);
    }
}

Result<::orc::RowReaderOptions> OrcFileBatchReader::CreateRowReaderOptions(
    const ::orc::Type* src_type, const ::orc::Type* target_type,
    std::unique_ptr<::orc::SearchArgument>&& search_arg,
    const std::map<std::string, std::string>& options, std::vector<uint64_t>* target_column_ids) {
    PAIMON_ASSIGN_OR_RAISE(std::list<std::string> include_fields,
                           GetAndCheckIncludedFields(src_type, target_type, target_column_ids));
    ::orc::RowReaderOptions row_reader_options;
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
