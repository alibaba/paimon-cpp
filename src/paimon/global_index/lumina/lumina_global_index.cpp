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

#include "paimon/global_index/lumina/lumina_global_index.h"

#include <utility>

#include "arrow/c/bridge.h"
#include "lumina/api/Dataset.h"
#include "lumina/api/LuminaBuilder.h"
#include "lumina/api/LuminaSearcher.h"
#include "lumina/api/OptionsNormalize.h"
#include "lumina/core/Constants.h"
#include "lumina/core/Status.h"
#include "lumina/core/Types.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/global_index/lumina/lumina_file_reader.h"
#include "paimon/global_index/lumina/lumina_file_writer.h"
#include "paimon/global_index/lumina/lumina_utils.h"
namespace paimon::lumina {
#define CHECK_NOT_NULL(pointer, error_msg)     \
    do {                                       \
        if (!(pointer)) {                      \
            return Status::Invalid(error_msg); \
        }                                      \
    } while (0)

Result<std::shared_ptr<GlobalIndexWriter>> LuminaGlobalIndex::CreateWriter(
    const std::string& field_name, ::ArrowSchema* arrow_schema,
    const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
    const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::DataType> arrow_type,
                                      arrow::ImportType(arrow_schema));
    // check data type
    auto struct_type = std::dynamic_pointer_cast<arrow::StructType>(arrow_type);
    CHECK_NOT_NULL(struct_type, "arrow schema must be struct type when create LuminaIndexWriter");
    auto index_field = struct_type->GetFieldByName(field_name);
    CHECK_NOT_NULL(index_field,
                   fmt::format("field {} not exist in arrow schema when create LuminaIndexWriter",
                               field_name));
    auto list_type = std::dynamic_pointer_cast<arrow::ListType>(index_field->type());
    CHECK_NOT_NULL(list_type, "field type must be list[float] when create LuminaIndexWriter");
    if (list_type->value_type()->id() != arrow::Type::type::FLOAT) {
        return Status::Invalid("field type must be list[float] when create LuminaIndexWriter");
    }

    // check options
    auto lumina_options =
        OptionsUtils::FetchOptionsWithPrefix(LuminaDefines::kOptionKeyPrefix, options_);
    PAIMON_ASSIGN_OR_RAISE(uint32_t dimension,
                           OptionsUtils::GetValueFromMap<uint32_t>(
                               lumina_options, std::string(::lumina::core::kDimension)));

    PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(
        ::lumina::api::BuilderOptions builder_options,
        ::lumina::api::NormalizeBuilderOptions(std::unordered_map<std::string, std::string>(
            lumina_options.begin(), lumina_options.end())));
    auto lumina_pool = std::make_shared<LuminaMemoryPool>(pool);
    return std::make_shared<LuminaIndexWriter>(
        field_name, arrow_type, dimension, file_writer, std::move(builder_options),
        ::lumina::api::IOOptions(), lumina_options, lumina_pool);
}

Result<LuminaIndexReader::IndexInfo> LuminaIndexReader::GetIndexInfo(
    const GlobalIndexIOMeta& io_meta) {
    auto meta_bytes = io_meta.metadata;
    if (!meta_bytes) {
        return Status::Invalid("Lumina global index must have meta data");
    }
    std::map<std::string, std::string> lumina_write_options;
    PAIMON_RETURN_NOT_OK(RapidJsonUtil::FromJsonString(
        std::string(meta_bytes->data(), meta_bytes->size()), &lumina_write_options));

    // check options
    PAIMON_ASSIGN_OR_RAISE(uint32_t dimension,
                           OptionsUtils::GetValueFromMap<uint32_t>(
                               lumina_write_options, std::string(::lumina::core::kDimension)));
    PAIMON_ASSIGN_OR_RAISE(std::string index_type,
                           OptionsUtils::GetValueFromMap<std::string>(
                               lumina_write_options, std::string(::lumina::core::kIndexType)));
    PAIMON_ASSIGN_OR_RAISE(std::string distance_type_str,
                           OptionsUtils::GetValueFromMap<std::string>(
                               lumina_write_options, std::string(::lumina::core::kDistanceMetric)));
    VectorSearch::DistanceType distance_type = VectorSearch::DistanceType::UNKNOWN;
    if (distance_type_str == ::lumina::core::kDistanceL2) {
        distance_type = VectorSearch::DistanceType::EUCLIDEAN;
    } else if (distance_type_str == ::lumina::core::kDistanceCosine) {
        distance_type = VectorSearch::DistanceType::COSINE;
    } else if (distance_type_str == ::lumina::core::kDistanceInnerProduct) {
        distance_type = VectorSearch::DistanceType::INNER_PRODUCT;
    }
    if (distance_type == VectorSearch::DistanceType::UNKNOWN) {
        return Status::Invalid(
            fmt::format("invalid distance type {} for lumina", distance_type_str));
    }
    return LuminaIndexReader::IndexInfo({dimension, index_type, distance_type});
}

Result<std::shared_ptr<GlobalIndexReader>> LuminaGlobalIndex::CreateReader(
    ::ArrowSchema* c_arrow_schema, const std::shared_ptr<GlobalIndexFileReader>& file_manager,
    const std::vector<GlobalIndexIOMeta>& files, const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema,
                                      arrow::ImportSchema(c_arrow_schema));
    if (files.size() != 1) {
        return Status::Invalid("lumina index only has one index file per shard");
    }
    const auto& io_meta = files[0];
    // check data type
    if (arrow_schema->num_fields() != 1) {
        return Status::Invalid("LuminaGlobalIndex now only support one field");
    }
    auto index_field = arrow_schema->field(0);
    auto list_type = std::dynamic_pointer_cast<arrow::ListType>(index_field->type());
    CHECK_NOT_NULL(list_type, "field type must be list[float] when create LuminaIndexReader");
    if (list_type->value_type()->id() != arrow::Type::type::FLOAT) {
        return Status::Invalid("field type must be list[float] when create LuminaIndexReader");
    }

    // get index info from meta
    PAIMON_ASSIGN_OR_RAISE(LuminaIndexReader::IndexInfo index_info,
                           LuminaIndexReader::GetIndexInfo(io_meta));

    auto lumina_pool = std::make_shared<LuminaMemoryPool>(pool);
    ::lumina::core::MemoryResourceConfig memory_resource(lumina_pool.get());

    auto lumina_options =
        OptionsUtils::FetchOptionsWithPrefix(LuminaDefines::kOptionKeyPrefix, options_);
    lumina_options[std::string(::lumina::core::kDimension)] = std::to_string(index_info.dimension);
    lumina_options[std::string(::lumina::core::kIndexType)] = index_info.index_type;

    PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(
        ::lumina::api::SearcherOptions searcher_options,
        ::lumina::api::NormalizeSearcherOptions(std::unordered_map<std::string, std::string>(
            lumina_options.begin(), lumina_options.end())));

    PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(
        ::lumina::api::LuminaSearcher lumina_searcher,
        ::lumina::api::LuminaSearcher::Create(searcher_options, memory_resource));
    auto searcher = std::make_unique<::lumina::api::LuminaSearcher>(std::move(lumina_searcher));
    // get input stream and open index
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> in,
                           file_manager->GetInputStream(io_meta.file_path));
    auto lumina_file_reader = std::make_unique<LuminaFileReader>(in);
    PAIMON_RETURN_NOT_OK_FROM_LUMINA(
        searcher->Open(std::move(lumina_file_reader), ::lumina::api::IOOptions()));

    // check meta
    PAIMON_RETURN_NOT_OK(CheckLuminaIndexMeta(
        searcher->GetMeta(), /*row_count=*/io_meta.range_end + 1, index_info.dimension));
    auto searcher_with_filter = std::make_unique<::lumina::extensions::SearchWithFilterExtension>();
    PAIMON_RETURN_NOT_OK_FROM_LUMINA(searcher->Attach(*searcher_with_filter));
    return std::make_shared<LuminaIndexReader>(io_meta.range_end, index_info, std::move(searcher),
                                               std::move(searcher_with_filter), lumina_pool);
}

Status LuminaGlobalIndex::CheckLuminaIndexMeta(const ::lumina::api::LuminaSearcher::IndexInfo& meta,
                                               int64_t row_count, uint32_t dimension) {
    if (meta.dim != dimension) {
        return Status::Invalid(fmt::format(
            "lumina index dimension {} mismatch dimension {} in io meta", meta.dim, dimension));
    }
    if (meta.count != static_cast<uint64_t>(row_count)) {
        return Status::Invalid(fmt::format(
            "lumina index row count {} mismatch row count {} in io meta", meta.count, row_count));
    }
    return Status::OK();
}

class LuminaDataset : public ::lumina::api::Dataset {
 public:
    LuminaDataset(int64_t element_count, uint32_t dimension,
                  const std::vector<std::shared_ptr<arrow::FloatArray>>& array_vec)
        : element_count_(element_count), dimension_(dimension), array_vec_(array_vec) {}

    uint32_t Dim() const noexcept override {
        return dimension_;
    }
    uint64_t TotalSize() const noexcept override {
        return element_count_;
    }

    ::lumina::core::Result<uint64_t> GetNextBatch(
        std::vector<float>& vector_buffer,
        std::vector<::lumina::core::vector_id_t>& id_buffer) noexcept override {
        if (cursor_ >= array_vec_.size()) {
            return ::lumina::core::Result<uint64_t>::Ok(0);
        }
        auto& value_array = array_vec_[cursor_];
        int64_t value_array_length = value_array->length();
        int64_t element_count = value_array_length / dimension_;
        const float* value_ptr = value_array->raw_values();
        vector_buffer.resize(value_array_length);
        memcpy(vector_buffer.data(), value_ptr, sizeof(float) * value_array_length);
        id_buffer.resize(element_count);
        std::iota(id_buffer.begin(), id_buffer.end(), id_);
        id_ += element_count;

        // release the array when copy to vector_buffer
        value_array.reset();
        cursor_++;
        return ::lumina::core::Result<uint64_t>::Ok(static_cast<uint64_t>(element_count));
    }

 private:
    int64_t element_count_;
    uint32_t dimension_;
    std::vector<std::shared_ptr<arrow::FloatArray>> array_vec_;
    size_t cursor_ = 0;
    ::lumina::core::vector_id_t id_ = 0;
};

LuminaIndexWriter::LuminaIndexWriter(const std::string& field_name,
                                     const std::shared_ptr<arrow::DataType>& arrow_type,
                                     uint32_t dimension,
                                     const std::shared_ptr<GlobalIndexFileWriter>& file_manager,
                                     ::lumina::api::BuilderOptions&& builder_options,
                                     ::lumina::api::IOOptions&& io_options,
                                     const std::map<std::string, std::string>& lumina_options,
                                     const std::shared_ptr<LuminaMemoryPool>& pool)
    : pool_(pool),
      field_name_(field_name),
      arrow_type_(arrow_type),
      dimension_(dimension),
      file_manager_(file_manager),
      builder_options_(std::move(builder_options)),
      io_options_(std::move(io_options)),
      lumina_options_(lumina_options) {}

Status LuminaIndexWriter::AddBatch(::ArrowArray* arrow_array) {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                      arrow::ImportArray(arrow_array, arrow_type_));
    if (array->null_count() != 0) {
        return Status::Invalid("arrow_array in LuminaIndexWriter is invalid, must not null");
    }
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
    CHECK_NOT_NULL(struct_array, "invalid input array in LuminaIndexWriter, must be struct array");
    auto field_array = struct_array->GetFieldByName(field_name_);
    CHECK_NOT_NULL(
        field_array,
        fmt::format("invalid input array in LuminaIndexWriter, field {} not in input array",
                    field_name_));
    int64_t field_length = field_array->length();
    auto list_field_array = std::dynamic_pointer_cast<arrow::ListArray>(field_array);
    CHECK_NOT_NULL(list_field_array,
                   "invalid input array in LuminaIndexWriter, field array must be list array");
    auto value_array = std::dynamic_pointer_cast<arrow::FloatArray>(list_field_array->values());
    CHECK_NOT_NULL(
        value_array,
        "invalid input array in LuminaIndexWriter, field value array must be float array");
    if (value_array->null_count() != 0) {
        return Status::Invalid("field value array in LuminaIndexWriter is invalid, must not null");
    }
    if (value_array->length() != field_length * dimension_) {
        return Status::Invalid(fmt::format(
            "invalid input array in LuminaIndexWriter, length of field array [{}] multiplied "
            "dimension [{}] must match length of field value array [{}]",
            field_length, dimension_, value_array->length()));
    }
    count_ += array->length();
    array_vec_.push_back(std::move(value_array));
    return Status::OK();
}

Result<std::vector<GlobalIndexIOMeta>> LuminaIndexWriter::Finish() {
    ::lumina::core::MemoryResourceConfig memory_resource(pool_.get());
    PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(
        ::lumina::api::LuminaBuilder builder,
        ::lumina::api::LuminaBuilder::Create(builder_options_, memory_resource));
    // pretrain
    LuminaDataset dataset1(count_, dimension_, array_vec_);
    PAIMON_RETURN_NOT_OK_FROM_LUMINA(builder.PretrainFrom(dataset1));

    // insert data
    LuminaDataset dataset2(count_, dimension_, array_vec_);
    std::vector<std::shared_ptr<arrow::FloatArray>>().swap(array_vec_);
    PAIMON_RETURN_NOT_OK_FROM_LUMINA(builder.InsertFrom(dataset2));

    // dump index
    PAIMON_ASSIGN_OR_RAISE(std::string index_file_name,
                           file_manager_->NewFileName(LuminaDefines::kIdentifier));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OutputStream> out,
                           file_manager_->NewOutputStream(index_file_name))
    auto file_writer = std::make_unique<LuminaFileWriter>(out);
    PAIMON_RETURN_NOT_OK_FROM_LUMINA(builder.Dump(std::move(file_writer), io_options_));
    // prepare GlobalIndexIOMeta
    PAIMON_ASSIGN_OR_RAISE(int64_t file_size, file_manager_->GetFileSize(index_file_name));
    std::string options_json;
    PAIMON_RETURN_NOT_OK(RapidJsonUtil::ToJsonString(lumina_options_, &options_json));
    auto meta_bytes = std::make_shared<Bytes>(options_json, pool_->GetPaimonPool().get());
    GlobalIndexIOMeta meta(file_manager_->ToPath(index_file_name), file_size,
                           /*range_end=*/count_ - 1,
                           /*metadata=*/meta_bytes);
    return std::vector<GlobalIndexIOMeta>({meta});
}

LuminaIndexReader::LuminaIndexReader(
    int64_t range_end, const LuminaIndexReader::IndexInfo& index_info,
    std::unique_ptr<::lumina::api::LuminaSearcher>&& searcher,
    std::unique_ptr<::lumina::extensions::SearchWithFilterExtension>&& searcher_with_filter,
    const std::shared_ptr<LuminaMemoryPool>& pool)
    : range_end_(range_end),
      index_info_(index_info),
      pool_(pool),
      searcher_(std::move(searcher)),
      searcher_with_filter_(std::move(searcher_with_filter)) {}

Result<std::shared_ptr<ScoredGlobalIndexResult>> LuminaIndexReader::VisitVectorSearch(
    const std::shared_ptr<VectorSearch>& vector_search) {
    if (vector_search->predicate) {
        return Status::NotImplemented("lumina index not support predicate in VisitVectorSearch");
    }
    if (vector_search->distance_type &&
        vector_search->distance_type.value() != index_info_.distance_type) {
        return Status::Invalid("distance type for index and search not match");
    }
    if (vector_search->query.size() != index_info_.dimension) {
        return Status::Invalid("dimension for index and search not match");
    }

    auto lumina_options = OptionsUtils::FetchOptionsWithPrefix(LuminaDefines::kOptionKeyPrefix,
                                                               vector_search->options);
    auto index_type_iter = lumina_options.find(std::string(::lumina::core::kIndexType));
    if (index_type_iter != lumina_options.end() &&
        index_type_iter->second != index_info_.index_type) {
        return Status::Invalid("index type for index and search not match");
    }

    lumina_options[std::string(::lumina::core::kTopK)] = std::to_string(vector_search->limit);
    lumina_options[std::string(::lumina::core::kSearchThreadSafeFilter)] = "true";
    PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(
        ::lumina::api::SearchOptions search_options,
        ::lumina::api::NormalizeSearchOptions(index_info_.index_type,
                                              std::unordered_map<std::string, std::string>(
                                                  lumina_options.begin(), lumina_options.end())));

    ::lumina::api::Query lumina_query(vector_search->query.data(), vector_search->query.size());
    ::lumina::api::LuminaSearcher::SearchResult search_result;
    if (!vector_search->pre_filter) {
        PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(search_result,
                                           searcher_->Search(lumina_query, search_options, *pool_));
    } else {
        auto lumina_filter = [filter = vector_search->pre_filter](
                                 ::lumina::core::vector_id_t id) -> bool { return filter(id); };
        PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(
            search_result, searcher_with_filter_->SearchWithFilter(lumina_query, lumina_filter,
                                                                   search_options, *pool_));
    }

    // prepare BitmapScoredGlobalIndexResult
    std::map<int64_t, float> id_to_score;
    for (const auto& [id, score] : search_result.topk) {
        id_to_score[id] = score;
    }

    RoaringBitmap64 bitmap;
    std::vector<float> scores;
    scores.reserve(id_to_score.size());
    for (const auto& [id, score] : id_to_score) {
        bitmap.Add(id);
        scores.push_back(score);
    }
    return std::make_shared<BitmapScoredGlobalIndexResult>(std::move(bitmap), std::move(scores));
}

}  // namespace paimon::lumina
