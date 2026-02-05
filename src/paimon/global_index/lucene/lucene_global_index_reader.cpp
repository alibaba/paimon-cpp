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
#include "paimon/global_index/lucene/lucene_global_index_reader.h"

#include "arrow/c/bridge.h"
#include "lucene++/FileUtils.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/bitmap_vector_search_global_index_result.h"
#include "paimon/global_index/lucene/jieba_analyzer.h"
#include "paimon/global_index/lucene/lucene_collector.h"
#include "paimon/global_index/lucene/lucene_defs.h"
#include "paimon/global_index/lucene/lucene_directory.h"
#include "paimon/global_index/lucene/lucene_filter.h"
#include "paimon/global_index/lucene/lucene_utils.h"
#include "paimon/io/data_input_stream.h"

namespace paimon::lucene {
Result<std::shared_ptr<LuceneGlobalIndexReader>> LuceneGlobalIndexReader::Create(
    const std::string& field_name, const GlobalIndexIOMeta& io_meta,
    const std::shared_ptr<GlobalIndexFileReader>& file_reader,
    const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool) {
    try {
        auto meta_bytes = io_meta.metadata;
        if (!meta_bytes) {
            return Status::Invalid("Lucene global index must have meta data");
        }
        std::map<std::string, std::string> write_options;
        PAIMON_RETURN_NOT_OK(RapidJsonUtil::FromJsonString(
            std::string(meta_bytes->data(), meta_bytes->size()), &write_options));

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

        PAIMON_ASSIGN_OR_RAISE(std::string dictionary_dir, LuceneUtils::GetJiebaDictionaryDir());
        auto jieba = std::make_shared<cppjieba::Jieba>(
            dictionary_dir + "/jieba.dict.utf8", dictionary_dir + "/hmm_model.utf8",
            dictionary_dir + "/user.dict.utf8", dictionary_dir + "/idf.utf8",
            dictionary_dir + "/stop_words.utf8");

        // priority: read options > write options > kDefaultJiebaTokenizeMode
        PAIMON_ASSIGN_OR_RAISE(
            std::string tokenize_mode,
            OptionsUtils::GetValueFromMap(options, kJiebaTokenizeMode, std::string("")));
        if (tokenize_mode.empty()) {
            PAIMON_ASSIGN_OR_RAISE(tokenize_mode, OptionsUtils::GetValueFromMap(
                                                      write_options, kJiebaTokenizeMode,
                                                      std::string(kDefaultJiebaTokenizeMode)));
        }
        return std::shared_ptr<LuceneGlobalIndexReader>(
            new LuceneGlobalIndexReader(LuceneUtils::StringToWstring(field_name), io_meta.range_end,
                                        searcher, tokenize_mode, jieba));
    } catch (const std::exception& e) {
        return Status::Invalid(
            fmt::format("create lucene global index reader failed, with {} error.", e.what()));
    } catch (...) {
        return Status::UnknownError(
            "create lucene global index reader failed, with unknown error.");
    }
}

std::vector<std::wstring> LuceneGlobalIndexReader::TokenizeQuery(const std::string& query) const {
    std::vector<std::string> terms;
    JiebaTokenizer::CutWithMode(tokenize_mode_, jieba_.get(), query, &terms);
    std::vector<std::string_view> normalized_terms;
    JiebaTokenizer::Normalize(jieba_->extractor.GetStopWords(), &terms, &normalized_terms);
    std::vector<std::wstring> wterms;
    wterms.reserve(normalized_terms.size());
    for (const auto& term : normalized_terms) {
        wterms.push_back(LuceneUtils::StringToWstring(term));
    }
    return wterms;
}

Lucene::QueryPtr LuceneGlobalIndexReader::ConstructMatchQuery(
    const std::shared_ptr<FullTextSearch>& full_text_search) const noexcept(false) {
    assert(full_text_search->search_type == FullTextSearch::SearchType::MATCH_ALL ||
           full_text_search->search_type == FullTextSearch::SearchType::MATCH_ANY);
    Lucene::BooleanClause::Occur occur =
        full_text_search->search_type == FullTextSearch::SearchType::MATCH_ALL
            ? Lucene::BooleanClause::Occur::MUST
            : Lucene::BooleanClause::Occur::SHOULD;
    std::vector<std::wstring> query_terms = TokenizeQuery(full_text_search->query);
    if (query_terms.size() == 1) {
        return Lucene::newLucene<Lucene::TermQuery>(
            Lucene::newLucene<Lucene::Term>(wfield_name_, query_terms[0]));
    } else {
        auto typed_query = Lucene::newLucene<Lucene::BooleanQuery>();
        for (const auto& term : query_terms) {
            typed_query->add(Lucene::newLucene<Lucene::TermQuery>(
                                 Lucene::newLucene<Lucene::Term>(wfield_name_, term)),
                             occur);
        }
        return typed_query;
    }
}

Lucene::QueryPtr LuceneGlobalIndexReader::ConstructPhraseQuery(
    const std::shared_ptr<FullTextSearch>& full_text_search) const noexcept(false) {
    assert(full_text_search->search_type == FullTextSearch::SearchType::PHRASE);
    std::vector<std::wstring> query_terms = TokenizeQuery(full_text_search->query);
    auto typed_query = Lucene::newLucene<Lucene::PhraseQuery>();
    for (const auto& term : query_terms) {
        typed_query->add(Lucene::newLucene<Lucene::Term>(wfield_name_, term));
    }
    return typed_query;
}

Lucene::QueryPtr LuceneGlobalIndexReader::ConstructPrefixQuery(
    const std::shared_ptr<FullTextSearch>& full_text_search) const noexcept(false) {
    assert(full_text_search->search_type == FullTextSearch::SearchType::PREFIX);
    return Lucene::newLucene<Lucene::PrefixQuery>(Lucene::newLucene<Lucene::Term>(
        wfield_name_, LuceneUtils::StringToWstring(full_text_search->query)));
}

Lucene::QueryPtr LuceneGlobalIndexReader::ConstructWildCardQuery(
    const std::shared_ptr<FullTextSearch>& full_text_search) const noexcept(false) {
    assert(full_text_search->search_type == FullTextSearch::SearchType::WILDCARD);
    return Lucene::newLucene<Lucene::WildcardQuery>(Lucene::newLucene<Lucene::Term>(
        wfield_name_, LuceneUtils::StringToWstring(full_text_search->query)));
}

Result<std::shared_ptr<GlobalIndexResult>> LuceneGlobalIndexReader::SearchWithLimit(
    const Lucene::QueryPtr& query, const std::shared_ptr<FullTextSearch>& full_text_search) const
    noexcept(false) {
    assert(full_text_search->limit);
    Lucene::FilterPtr filter =
        full_text_search->pre_filter
            ? Lucene::newLucene<LuceneFilter>(&(full_text_search->pre_filter.value()))
            : Lucene::FilterPtr();

    Lucene::TopDocsPtr results = searcher_->search(query, filter, full_text_search->limit.value());

    // prepare BitmapVectorSearchGlobalIndexResult
    std::map<int64_t, float> id_to_score;
    for (auto score_doc : results->scoreDocs) {
        Lucene::DocumentPtr result_doc = searcher_->doc(score_doc->doc);
        std::string row_id_str = LuceneUtils::WstringToString(result_doc->get(kRowIdFieldWstring));
        std::optional<int32_t> row_id = StringUtils::StringToValue<int32_t>(row_id_str);
        if (!row_id) {
            return Status::Invalid(fmt::format("parse row id str {} to int failed", row_id_str));
        }
        id_to_score[static_cast<int64_t>(row_id.value())] = static_cast<float>(score_doc->score);
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
}

std::shared_ptr<GlobalIndexResult> LuceneGlobalIndexReader::SearchWithNoLimit(
    const Lucene::QueryPtr& query, const std::shared_ptr<FullTextSearch>& full_text_search) const
    noexcept(false) {
    assert(!full_text_search->limit);
    Lucene::FilterPtr filter =
        full_text_search->pre_filter
            ? Lucene::newLucene<LuceneFilter>(&(full_text_search->pre_filter.value()))
            : Lucene::FilterPtr();

    // with no limit & no score
    auto collector = Lucene::newLucene<LuceneCollector>();
    searcher_->search(query, filter, collector);
    return std::make_shared<BitmapGlobalIndexResult>(
        [collector]() -> Result<RoaringBitmap64> { return collector->GetBitmap(); });
}

Result<std::shared_ptr<GlobalIndexResult>> LuceneGlobalIndexReader::VisitFullTextSearch(
    const std::shared_ptr<FullTextSearch>& full_text_search) {
    try {
        Lucene::QueryPtr query;
        switch (full_text_search->search_type) {
            case FullTextSearch::SearchType::MATCH_ALL:
            case FullTextSearch::SearchType::MATCH_ANY: {
                query = ConstructMatchQuery(full_text_search);
                break;
            }
            case FullTextSearch::SearchType::PHRASE: {
                query = ConstructPhraseQuery(full_text_search);
                break;
            }
            case FullTextSearch::SearchType::PREFIX: {
                query = ConstructPrefixQuery(full_text_search);
                break;
            }
            case FullTextSearch::SearchType::WILDCARD: {
                query = ConstructWildCardQuery(full_text_search);
                break;
            }
            default:
                return Status::Invalid(
                    fmt::format("Not support for FullTextSearch SearchType {}",
                                static_cast<int32_t>(full_text_search->search_type)));
        }
        if (full_text_search->limit) {
            return SearchWithLimit(query, full_text_search);
        } else {
            return SearchWithNoLimit(query, full_text_search);
        }
    } catch (const std::exception& e) {
        return Status::Invalid(
            fmt::format("visit full text search failed, with {} error.", e.what()));
    } catch (...) {
        return Status::UnknownError("visit full text search failed, with unknown error.");
    }
}

}  // namespace paimon::lucene
