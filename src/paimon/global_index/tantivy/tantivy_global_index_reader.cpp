/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

#include "paimon/global_index/tantivy/tantivy_global_index_reader.h"

#include <cstdlib>
#include <cstring>
#include <memory>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/global_index/tantivy/tantivy_archive_layout.h"
#include "paimon/global_index/tantivy/tantivy_ffi_status.h"
#include "paimon/global_index/tantivy/tantivy_stream_ctx.h"

namespace paimon::tantivy {

namespace {

Result<std::string> GetJiebaDictionaryDir() {
    const char* env_dir = std::getenv(kJiebaDictDirEnv);
    if (env_dir && *env_dir != '\0') {
        return std::string(env_dir);
    }
    return Status::Invalid(fmt::format(
        "jieba dictionary dir not found, please set {} env var", kJiebaDictDirEnv));
}

}  // namespace

Result<std::shared_ptr<TantivyGlobalIndexReader>> TantivyGlobalIndexReader::Create(
    const std::string& field_name, const GlobalIndexIOMeta& io_meta,
    const std::shared_ptr<GlobalIndexFileReader>& file_reader,
    const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool) {
    (void)field_name;  // Rust-side knows the field via the schema embedded in meta.json
    if (!io_meta.metadata) {
        return Status::Invalid("Tantivy global index must have meta data");
    }

    std::map<std::string, std::string> write_options;
    PAIMON_RETURN_NOT_OK(RapidJsonUtil::FromJsonString(
        std::string(io_meta.metadata->data(), io_meta.metadata->size()), &write_options));

    PAIMON_ASSIGN_OR_RAISE(
        std::string tokenize_mode,
        OptionsUtils::GetValueFromMap(options, kJiebaTokenizeMode, std::string("")));
    if (tokenize_mode.empty()) {
        PAIMON_ASSIGN_OR_RAISE(tokenize_mode, OptionsUtils::GetValueFromMap(
                                                  write_options, kJiebaTokenizeMode,
                                                  std::string(kDefaultJiebaTokenizeMode)));
    }
    PAIMON_ASSIGN_OR_RAISE(
        bool omit_term_freq_and_positions,
        OptionsUtils::GetValueFromMap(write_options, kTantivyWriteOmitTermFreqAndPositions, false));

    PAIMON_ASSIGN_OR_RAISE(std::string dict_dir, GetJiebaDictionaryDir());

    // V3 streaming read path:
    //   1) open stream
    //   2) ParseArchiveHeader — reads only header bytes, seeks past payloads
    //   3) wrap stream in StreamCtx (owned by Rust via release callback)
    //   4) build PaimonStreamCallbacks → paimon_tantivy_reader_new_streaming
    // Archive payloads are read lazily through read_at callbacks as tantivy
    // accesses posting lists, meta.json, etc.
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> stream,
                           file_reader->GetInputStream(io_meta.file_path));
    PAIMON_ASSIGN_OR_RAISE(ArchiveLayout layout, ParseArchiveHeader(stream.get()));

    // Transfer stream ownership to a heap-allocated StreamCtx; Rust will
    // `paimon_cpp_stream_release(ctx)` on reader drop, which `delete`s it.
    auto* stream_ctx = new StreamCtx{std::move(stream), {}};
    PaimonStreamCallbacks callbacks{
        static_cast<void*>(stream_ctx),
        paimon_cpp_stream_read_at,
        paimon_cpp_stream_release,
    };

    // Build C-string array pointing into layout.names (stable during this call).
    std::vector<const char*> name_ptrs;
    name_ptrs.reserve(layout.count);
    for (const auto& n : layout.names) {
        name_ptrs.push_back(n.c_str());
    }

    PaimonTantivyReader* raw = nullptr;
    ::PaimonTantivyStatus st = paimon_tantivy_reader_new_streaming(
        name_ptrs.data(),
        layout.offsets.data(),
        layout.lengths.data(),
        layout.count,
        callbacks,
        tokenize_mode.c_str(),
        /*with_position=*/!omit_term_freq_and_positions,
        dict_dir.c_str(),
        &raw);
    if (st != PAIMON_TANTIVY_STATUS_OK) {
        // On failure, Rust did NOT take ownership of ctx (FFI contract):
        // release it here so the stream doesn't leak.
        paimon_cpp_stream_release(stream_ctx);
        PAIMON_TANTIVY_RETURN_NOT_OK(st);
    }
    return std::shared_ptr<TantivyGlobalIndexReader>(
        new TantivyGlobalIndexReader(io_meta.range_end, ReaderPtr(raw), pool));
}

Result<std::shared_ptr<GlobalIndexResult>> TantivyGlobalIndexReader::VisitFullTextSearch(
    const std::shared_ptr<FullTextSearch>& full_text_search) {
    if (!full_text_search) {
        return Status::Invalid("VisitFullTextSearch: null FullTextSearch pointer");
    }

    // Serialize pre_filter (if any) to croaring portable bytes for FFI.
    // NB: Serialize() returns a pooled_unique_ptr with MemoryPool::AllocatorDelete;
    // converting via raw.release() + shared_ptr<Bytes>(raw_ptr) would substitute
    // std::default_delete, causing alloc/dealloc mismatch (malloc vs operator
    // delete) — detected by ASAN on 2026-04-21. Move directly into shared_ptr
    // so the pooled deleter is preserved in the control block.
    PAIMON_UNIQUE_PTR<Bytes> pre_filter_bytes_owned;
    const char* pre_filter_ptr = nullptr;
    std::size_t pre_filter_len = 0;
    if (full_text_search->pre_filter.has_value()) {
        pre_filter_bytes_owned = full_text_search->pre_filter.value().Serialize(pool_.get());
        pre_filter_ptr = pre_filter_bytes_owned->data();
        pre_filter_len = pre_filter_bytes_owned->size();
    }

    int32_t limit_arg = full_text_search->limit.has_value()
                            ? static_cast<int32_t>(full_text_search->limit.value())
                            : -1;

    BufferGuard out;
    PaimonTantivyStatus st = paimon_tantivy_reader_search(
        reader_.get(), static_cast<int32_t>(full_text_search->search_type),
        full_text_search->query.data(), full_text_search->query.size(),
        full_text_search->with_score, limit_arg,
        pre_filter_ptr, pre_filter_len, out.out());
    PAIMON_TANTIVY_RETURN_NOT_OK(st);

    // Decode `[u8 has_scores | u64 count | u64 row_ids[] | optional f32 scores[]]`.
    // (B1 schema: row_id is the explicit u64 column read from the fast field.)
    if (out.size() < 9) {
        return Status::Invalid(
            fmt::format("tantivy reader output too small ({} bytes)", out.size()));
    }
    const uint8_t* p = out.data();
    bool has_scores = (p[0] != 0);
    // v0.2 consistency check: the wire-level has_scores byte must match the caller's
    // with_score flag. A mismatch would indicate FFI / wire-protocol drift.
    if (has_scores != full_text_search->with_score) {
        return Status::Invalid(fmt::format(
            "tantivy wire protocol mismatch: caller with_score={} but buffer has_scores={}",
            full_text_search->with_score, has_scores));
    }
    uint64_t count;
    std::memcpy(&count, p + 1, sizeof(uint64_t));
    std::size_t expected = 1 + 8 + count * 8 + (has_scores ? count * 4 : 0);
    if (out.size() != expected) {
        return Status::Invalid(fmt::format(
            "tantivy reader output size mismatch: has_scores={} count={} expected {} bytes, got {}",
            has_scores, count, expected, out.size()));
    }

    const uint8_t* row_id_p = p + 9;
    if (!has_scores) {
        RoaringBitmap64 bitmap;
        for (uint64_t i = 0; i < count; i++) {
            uint64_t row_id;
            std::memcpy(&row_id, row_id_p + i * 8, sizeof(uint64_t));
            bitmap.Add(static_cast<int64_t>(row_id));
        }
        return std::make_shared<BitmapGlobalIndexResult>(
            [b = std::move(bitmap)]() -> Result<RoaringBitmap64> { return b; });
    }
    // has_scores=true: produce BitmapScoredGlobalIndexResult. Rust may send rows
    // in either row_id-asc order (path C: with_score=true, limit=None) or score-desc
    // order (path D: with_score=true, limit=Some). The bitmap iteration order is
    // row_id-asc (RoaringBitmap set semantics), so we always re-sort by row_id here
    // to keep `scores[i]` aligned with the i-th row_id from the bitmap iterator —
    // matching the contract documented in BitmapScoredGlobalIndexResult.
    const uint8_t* score_p = row_id_p + count * 8;
    std::vector<std::pair<int64_t, float>> id_score_pairs;
    id_score_pairs.reserve(count);
    for (uint64_t i = 0; i < count; i++) {
        uint64_t row_id;
        std::memcpy(&row_id, row_id_p + i * 8, sizeof(uint64_t));
        float score;
        std::memcpy(&score, score_p + i * 4, sizeof(float));
        id_score_pairs.emplace_back(static_cast<int64_t>(row_id), score);
    }
    // Sort by row_id ascending so scores align with bitmap iteration order.
    std::sort(id_score_pairs.begin(), id_score_pairs.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
    RoaringBitmap64 bitmap;
    std::vector<float> scores;
    scores.reserve(id_score_pairs.size());
    for (const auto& [id, sc] : id_score_pairs) {
        bitmap.Add(id);
        scores.push_back(sc);
    }
    return std::make_shared<BitmapScoredGlobalIndexResult>(std::move(bitmap), std::move(scores));
}

}  // namespace paimon::tantivy
