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
#include "paimon/global_index/lucene/lucene_global_index.h"

#include <filesystem>

#include "arrow/c/bridge.h"
#include "lucene++/FileUtils.h"
#include "paimon/common/io/data_output_stream.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/common/utils/uuid.h"
#include "paimon/global_index/bitmap_vector_search_global_index_result.h"
#include "paimon/global_index/lucene/lucene_collector.h"
#include "paimon/global_index/lucene/lucene_defs.h"
#include "paimon/global_index/lucene/lucene_directory.h"
#include "paimon/global_index/lucene/lucene_filter.h"
#include "paimon/global_index/lucene/lucene_utils.h"
#include "paimon/io/data_input_stream.h"

namespace paimon::lucene {
#define CHECK_NOT_NULL(pointer, error_msg)     \
    do {                                       \
        if (!(pointer)) {                      \
            return Status::Invalid(error_msg); \
        }                                      \
    } while (0)

LuceneGlobalIndex::LuceneGlobalIndex(const std::map<std::string, std::string>& options)
    : options_(OptionsUtils::FetchOptionsWithPrefix(kOptionKeyPrefix, options)) {}

Result<std::shared_ptr<GlobalIndexWriter>> LuceneGlobalIndex::CreateWriter(
    const std::string& field_name, ::ArrowSchema* arrow_schema,
    const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
    const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::DataType> arrow_type,
                                      arrow::ImportType(arrow_schema));
    // check data type
    auto struct_type = std::dynamic_pointer_cast<arrow::StructType>(arrow_type);
    CHECK_NOT_NULL(struct_type, "arrow schema must be struct type when create LuceneIndexWriter");
    auto index_field = struct_type->GetFieldByName(field_name);
    CHECK_NOT_NULL(index_field,
                   fmt::format("field {} not exist in arrow schema when create LuceneIndexWriter",
                               field_name));
    if (index_field->type()->id() != arrow::Type::type::STRING) {
        return Status::Invalid("field type must be string");
    }
    return LuceneGlobalIndexWriter::Create(field_name, arrow_type, file_writer, options_, pool);
}

Result<std::shared_ptr<GlobalIndexReader>> LuceneGlobalIndex::CreateReader(
    ::ArrowSchema* c_arrow_schema, const std::shared_ptr<GlobalIndexFileReader>& file_reader,
    const std::vector<GlobalIndexIOMeta>& files, const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema,
                                      arrow::ImportSchema(c_arrow_schema));
    if (files.size() != 1) {
        return Status::Invalid("lucene index only has one index file per shard");
    }
    const auto& io_meta = files[0];
    // check data type
    if (arrow_schema->num_fields() != 1) {
        return Status::Invalid("LuceneGlobalIndex now only support one field");
    }
    auto index_field = arrow_schema->field(0);
    if (index_field->type()->id() != arrow::Type::type::STRING) {
        return Status::Invalid("field type must be string");
    }
    return LuceneGlobalIndexReader::Create(index_field->name(), io_meta, file_reader, options_,
                                           pool);
}

LuceneGlobalIndexWriter::LuceneWriteContext::LuceneWriteContext(
    const std::string& _tmp_index_path, const Lucene::FSDirectoryPtr& _lucene_dir,
    const Lucene::IndexWriterPtr& _index_writer, const Lucene::DocumentPtr& _doc,
    const Lucene::FieldPtr& _field, const Lucene::FieldPtr& _row_id_field)
    : tmp_index_path(_tmp_index_path),
      lucene_dir(_lucene_dir),
      index_writer(_index_writer),
      doc(_doc),
      field(_field),
      row_id_field(_row_id_field) {}

Result<std::shared_ptr<LuceneGlobalIndexWriter>> LuceneGlobalIndexWriter::Create(
    const std::string& field_name, const std::shared_ptr<arrow::DataType>& arrow_type,
    const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
    const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool) {
    try {
        std::string uuid;
        if (!UUID::Generate(&uuid)) {
            return Status::Invalid("generate uuid for lucene tmp path failed.");
        }
        // create a local tmp path
        std::string tmp_path = PathUtil::JoinPath(std::filesystem::temp_directory_path().string(),
                                                  "paimon-lucene-" + uuid);
        auto lucene_dir = Lucene::FSDirectory::open(LuceneUtils::StringToWstring(tmp_path),
                                                    Lucene::NoLockFactory::getNoLockFactory());
        // TODO(xinyu.lxy): support other tokenizer
        // open lucene index writer
        Lucene::IndexWriterPtr writer = Lucene::newLucene<Lucene::IndexWriter>(
            lucene_dir,
            Lucene::newLucene<Lucene::StandardAnalyzer>(Lucene::LuceneVersion::LUCENE_CURRENT),
            /*create=*/true, Lucene::IndexWriter::MaxFieldLengthLIMITED);

        // prepare field and document
        Lucene::DocumentPtr doc = Lucene::newLucene<Lucene::Document>();
        auto field = Lucene::newLucene<Lucene::Field>(LuceneUtils::StringToWstring(field_name),
                                                      kEmptyWstring, Lucene::Field::STORE_NO,
                                                      Lucene::Field::INDEX_ANALYZED_NO_NORMS);
        auto row_id_field = Lucene::newLucene<Lucene::Field>(
            kRowIdFieldWstring, kEmptyWstring, Lucene::Field::STORE_YES,
            Lucene::Field::INDEX_NOT_ANALYZED_NO_NORMS);
        PAIMON_ASSIGN_OR_RAISE(
            bool omit_term_freq_and_positions,
            OptionsUtils::GetValueFromMap(options, kLuceneWriteOmitTermFreqAndPositions, false));
        field->setOmitTermFreqAndPositions(omit_term_freq_and_positions);
        row_id_field->setOmitTermFreqAndPositions(true);
        doc->add(field);
        doc->add(row_id_field);
        return std::shared_ptr<LuceneGlobalIndexWriter>(new LuceneGlobalIndexWriter(
            field_name, arrow_type,
            LuceneWriteContext(tmp_path, lucene_dir, writer, doc, field, row_id_field), file_writer,
            options, pool));
    } catch (const std::exception& e) {
        return Status::Invalid(
            fmt::format("create lucene global index writer failed, with {} error.", e.what()));
    } catch (...) {
        return Status::UnknownError(
            "create lucene global index writer failed, with unknown error.");
    }
}

LuceneGlobalIndexWriter::LuceneGlobalIndexWriter(
    const std::string& field_name, const std::shared_ptr<arrow::DataType>& arrow_type,
    LuceneWriteContext&& write_context, const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
    const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      field_name_(field_name),
      arrow_type_(arrow_type),
      write_context_(std::move(write_context)),
      file_writer_(file_writer),
      options_(options) {}

LuceneGlobalIndexWriter::~LuceneGlobalIndexWriter() {
    try {
        [[maybe_unused]] bool ec = Lucene::FileUtils::removeDirectory(
            LuceneUtils::StringToWstring(write_context_.tmp_index_path));
    } catch (...) {
        // do nothing
    }
}

Status LuceneGlobalIndexWriter::AddBatch(::ArrowArray* arrow_array) {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                      arrow::ImportArray(arrow_array, arrow_type_));
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
    CHECK_NOT_NULL(struct_array, "invalid input array in LuceneIndexWriter, must be struct array");
    auto field_array = struct_array->GetFieldByName(field_name_);
    CHECK_NOT_NULL(
        field_array,
        fmt::format("invalid input array in LuceneIndexWriter, field {} not in input array",
                    field_name_));
    auto string_array = std::dynamic_pointer_cast<arrow::StringArray>(field_array);
    CHECK_NOT_NULL(
        string_array,
        fmt::format(
            "invalid input array in LuceneIndexWriter, field array {} is not a string array",
            field_name_));
    try {
        for (int64_t i = 0; i < string_array->length(); i++) {
            if (string_array->IsNull(i)) {
                write_context_.field->setValue(kEmptyWstring);
            } else {
                auto view = string_array->Value(i);
                write_context_.field->setValue(LuceneUtils::StringToWstring(view));
            }
            write_context_.row_id_field->setValue(
                LuceneUtils::StringToWstring(std::to_string(row_id_++)));
            write_context_.index_writer->addDocument(write_context_.doc);
        }
    } catch (const std::exception& e) {
        return Status::Invalid(fmt::format(
            "add batch for lucene global index writer failed, with {} error.", e.what()));
    } catch (...) {
        return Status::UnknownError(
            "add batch for lucene global index writer failed, with unknown error.");
    }
    return Status::OK();
}

Result<std::string> LuceneGlobalIndexWriter::FlushIndexToFinal() const {
    try {
        // flush index to tmp dir
        write_context_.index_writer->optimize();
        write_context_.index_writer->close();

        // list tmp dir
        auto tmp_file_names = write_context_.lucene_dir->listAll();
        PAIMON_ASSIGN_OR_RAISE(std::string index_file_name, file_writer_->NewFileName(kIdentifier));
        // prepare output from file_writer
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OutputStream> out,
                               file_writer_->NewOutputStream(index_file_name));
        DataOutputStream data_output_stream(out);
        PAIMON_RETURN_NOT_OK(data_output_stream.WriteValue<int32_t>(kVersion));
        PAIMON_RETURN_NOT_OK(
            data_output_stream.WriteValue<int32_t>(static_cast<int32_t>(tmp_file_names.size())));
        // read all data from index files and write to target output
        auto buffer = std::make_shared<Bytes>(kDefaultReadBufferSize, pool_.get());
        for (const auto& wfile_name : tmp_file_names) {
            auto file_name = LuceneUtils::WstringToString(wfile_name);
            PAIMON_RETURN_NOT_OK(
                data_output_stream.WriteValue<int32_t>(static_cast<int32_t>(file_name.size())));
            PAIMON_RETURN_NOT_OK(
                data_output_stream.WriteBytes(std::make_shared<Bytes>(file_name, pool_.get())));
            int64_t file_length = write_context_.lucene_dir->fileLength(wfile_name);
            PAIMON_RETURN_NOT_OK(data_output_stream.WriteValue<int64_t>(file_length));

            Lucene::IndexInputPtr input = write_context_.lucene_dir->openInput(wfile_name);
            int64_t total_write_size = 0;
            while (total_write_size < file_length) {
                int64_t current_write_size = std::min(file_length - total_write_size,
                                                      static_cast<int64_t>(kDefaultReadBufferSize));
                input->readBytes(reinterpret_cast<uint8_t*>(buffer->data()), /*offset=*/0,
                                 static_cast<int32_t>(current_write_size));
                PAIMON_ASSIGN_OR_RAISE(
                    int32_t actual_write_size,
                    out->Write(buffer->data(), static_cast<uint32_t>(current_write_size)));
                if (static_cast<int64_t>(actual_write_size) != current_write_size) {
                    return Status::Invalid(
                        fmt::format("invalid write, try to write {} while actual write {}",
                                    current_write_size, actual_write_size));
                }
                total_write_size += current_write_size;
            }
            input->close();
        }
        PAIMON_RETURN_NOT_OK(out->Flush());
        PAIMON_RETURN_NOT_OK(out->Close());
        write_context_.lucene_dir->close();
        return index_file_name;
    } catch (const std::exception& e) {
        return Status::Invalid(
            fmt::format("finish for lucene global index writer failed, with {} error.", e.what()));
    } catch (...) {
        return Status::UnknownError(
            "finish for lucene global index writer failed, with unknown error.");
    }
}

Result<std::vector<GlobalIndexIOMeta>> LuceneGlobalIndexWriter::Finish() {
    PAIMON_ASSIGN_OR_RAISE(std::string index_file_name, FlushIndexToFinal());
    // prepare global index meta
    PAIMON_ASSIGN_OR_RAISE(int64_t file_size, file_writer_->GetFileSize(index_file_name));
    std::string options_json;
    PAIMON_RETURN_NOT_OK(RapidJsonUtil::ToJsonString(options_, &options_json));
    auto meta_bytes = std::make_shared<Bytes>(options_json, pool_.get());
    GlobalIndexIOMeta meta(file_writer_->ToPath(index_file_name), file_size,
                           /*range_end=*/static_cast<int64_t>(row_id_) - 1,
                           /*metadata=*/meta_bytes);
    return std::vector<GlobalIndexIOMeta>({meta});
}

Result<std::shared_ptr<LuceneGlobalIndexReader>> LuceneGlobalIndexReader::Create(
    const std::string& field_name, const GlobalIndexIOMeta& io_meta,
    const std::shared_ptr<GlobalIndexFileReader>& file_reader,
    const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool) {
    try {
        std::map<std::string, std::pair<int64_t, int64_t>> file_name_to_offset_and_length;
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> paimon_input,
                               file_reader->GetInputStream(io_meta.file_path));
        DataInputStream data_input_stream(paimon_input);
        PAIMON_ASSIGN_OR_RAISE(int32_t version, data_input_stream.ReadValue<int32_t>());
        if (version != kVersion) {
            return Status::Invalid(fmt::format("LuceneGlobalIndex not support version {}"),
                                   kVersion);
        }
        PAIMON_ASSIGN_OR_RAISE(int32_t num_files, data_input_stream.ReadValue<int32_t>());
        for (int32_t i = 0; i < num_files; i++) {
            PAIMON_ASSIGN_OR_RAISE(int32_t file_name_len, data_input_stream.ReadValue<int32_t>());
            auto file_name_bytes = std::make_shared<Bytes>(file_name_len, pool.get());
            PAIMON_RETURN_NOT_OK(data_input_stream.ReadBytes(file_name_bytes.get()));
            std::string file_name(file_name_bytes->data(), file_name_bytes->size());
            PAIMON_ASSIGN_OR_RAISE(int64_t file_len, data_input_stream.ReadValue<int64_t>());
            PAIMON_ASSIGN_OR_RAISE(int64_t pos, data_input_stream.GetPos());
            file_name_to_offset_and_length[file_name] = {pos, file_len};
            pos += file_len;
            if (i != num_files - 1) {
                PAIMON_RETURN_NOT_OK(data_input_stream.Seek(pos));
            }
        }
        PAIMON_ASSIGN_OR_RAISE(
            int32_t read_buffer_size,
            OptionsUtils::GetValueFromMap(options, kLuceneReadBufferSize, kDefaultReadBufferSize));
        Lucene::DirectoryPtr lucene_dir = Lucene::newLucene<LuceneDirectory>(
            PathUtil::GetParentDirPath(io_meta.file_path), file_name_to_offset_and_length,
            paimon_input, read_buffer_size);

        Lucene::IndexReaderPtr reader = Lucene::IndexReader::open(lucene_dir, /*read_only=*/true);
        Lucene::IndexSearcherPtr searcher = Lucene::newLucene<Lucene::IndexSearcher>(reader);
        return std::shared_ptr<LuceneGlobalIndexReader>(new LuceneGlobalIndexReader(
            LuceneUtils::StringToWstring(field_name), io_meta.range_end, searcher));
    } catch (const std::exception& e) {
        return Status::Invalid(
            fmt::format("create lucene global index reader failed, with {} error.", e.what()));
    } catch (...) {
        return Status::UnknownError(
            "create lucene global index reader failed, with unknown error.");
    }
}

std::vector<std::wstring> LuceneGlobalIndexReader::TokenizeQuery(const std::string& query) {
    // TODO(xinyu.lxy): support jieba analyzer
    std::vector<std::string> terms =
        StringUtils::Split(query, /*sep_str=*/" ", /*ignore_empty=*/true);
    std::vector<std::wstring> wterms;
    wterms.reserve(terms.size());
    for (const auto& term : terms) {
        wterms.push_back(LuceneUtils::StringToWstring(term));
    }
    return wterms;
}

Result<std::shared_ptr<GlobalIndexResult>> LuceneGlobalIndexReader::VisitFullTextSearch(
    const std::shared_ptr<FullTextSearch>& full_text_search) {
    try {
        Lucene::QueryPtr query;
        switch (full_text_search->search_type) {
            case FullTextSearch::SearchType::MATCH_ALL:
            case FullTextSearch::SearchType::MATCH_ANY: {
                Lucene::BooleanClause::Occur occur =
                    full_text_search->search_type == FullTextSearch::SearchType::MATCH_ALL
                        ? Lucene::BooleanClause::Occur::MUST
                        : Lucene::BooleanClause::Occur::SHOULD;
                std::vector<std::wstring> query_terms = TokenizeQuery(full_text_search->query);
                if (query_terms.size() == 1) {
                    query = Lucene::newLucene<Lucene::TermQuery>(
                        Lucene::newLucene<Lucene::Term>(wfield_name_, query_terms[0]));
                } else {
                    auto typed_query = Lucene::newLucene<Lucene::BooleanQuery>();
                    for (const auto& term : query_terms) {
                        typed_query->add(Lucene::newLucene<Lucene::TermQuery>(
                                             Lucene::newLucene<Lucene::Term>(wfield_name_, term)),
                                         occur);
                    }
                    query = typed_query;
                }
                break;
            }
            case FullTextSearch::SearchType::PHRASE: {
                std::vector<std::wstring> query_terms = TokenizeQuery(full_text_search->query);
                auto typed_query = Lucene::newLucene<Lucene::PhraseQuery>();
                for (const auto& term : query_terms) {
                    typed_query->add(Lucene::newLucene<Lucene::Term>(wfield_name_, term));
                }
                query = typed_query;
                break;
            }
            case FullTextSearch::SearchType::PREFIX: {
                query = Lucene::newLucene<Lucene::PrefixQuery>(Lucene::newLucene<Lucene::Term>(
                    wfield_name_, LuceneUtils::StringToWstring(full_text_search->query)));
                break;
            }
            case FullTextSearch::SearchType::WILDCARD: {
                query = Lucene::newLucene<Lucene::WildcardQuery>(Lucene::newLucene<Lucene::Term>(
                    wfield_name_, LuceneUtils::StringToWstring(full_text_search->query)));
                break;
            }
            default:
                return Status::Invalid(
                    fmt::format("Not support for FullTextSearch SearchType {}",
                                static_cast<int32_t>(full_text_search->search_type)));
        }
        Lucene::FilterPtr filter =
            full_text_search->pre_filter
                ? Lucene::newLucene<LuceneFilter>(&(full_text_search->pre_filter.value()))
                : Lucene::FilterPtr();

        if (full_text_search->limit) {
            Lucene::TopDocsPtr results =
                searcher_->search(query, filter, full_text_search->limit.value());

            // prepare BitmapVectorSearchGlobalIndexResult
            std::map<int64_t, float> id_to_score;
            for (auto score_doc : results->scoreDocs) {
                Lucene::DocumentPtr result_doc = searcher_->doc(score_doc->doc);
                std::string row_id_str =
                    LuceneUtils::WstringToString(result_doc->get(kRowIdFieldWstring));
                std::optional<int32_t> row_id = StringUtils::StringToValue<int32_t>(row_id_str);
                if (!row_id) {
                    return Status::Invalid(
                        fmt::format("parse row id str {} to int failed", row_id_str));
                }
                id_to_score[static_cast<int64_t>(row_id.value())] =
                    static_cast<float>(score_doc->score);
            }
            RoaringBitmap64 bitmap;
            std::vector<float> scores;
            scores.reserve(id_to_score.size());
            for (const auto& [id, score] : id_to_score) {
                bitmap.Add(id);
                scores.push_back(score);
            }
            return std::make_shared<BitmapVectorSearchGlobalIndexResult>(std::move(bitmap),
                                                                         std::move(scores));
        } else {
            // with no limit & no score
            auto collector = Lucene::newLucene<LuceneCollector>();
            searcher_->search(query, filter, collector);
            return std::make_shared<BitmapGlobalIndexResult>(
                [collector]() -> Result<RoaringBitmap64> { return collector->GetBitmap(); });
        }
    } catch (const std::exception& e) {
        return Status::Invalid(fmt::format("visit term query failed, with {} error.", e.what()));
    } catch (...) {
        return Status::UnknownError("visit term query failed, with unknown error.");
    }
}

}  // namespace paimon::lucene
