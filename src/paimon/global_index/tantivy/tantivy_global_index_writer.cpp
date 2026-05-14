/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

#include "paimon/global_index/tantivy/tantivy_global_index_writer.h"

#include <cstdlib>

#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/global_index/tantivy/tantivy_ffi_status.h"
#include "paimon/global_index/tantivy/tantivy_stream_ctx.h"

namespace paimon::tantivy {

#define CHECK_NOT_NULL(pointer, error_msg)     \
    do {                                       \
        if (!(pointer)) {                      \
            return Status::Invalid(error_msg); \
        }                                      \
    } while (0)

namespace {

/// Resolve the jieba dictionary directory for the writer. Mirrors lucene-fts'
/// LuceneUtils::GetJiebaDictionaryDir but kept separate to avoid coupling
/// tantivy-fulltext to the lucene module.
Result<std::string> GetJiebaDictionaryDir() {
    const char* env_dir = std::getenv(kJiebaDictDirEnv);
    if (env_dir && *env_dir != '\0') {
        return std::string(env_dir);
    }
    return Status::Invalid(fmt::format(
        "jieba dictionary dir not found, please set {} env var", kJiebaDictDirEnv));
}

}  // namespace

Result<std::shared_ptr<TantivyGlobalIndexWriter>> TantivyGlobalIndexWriter::Create(
    const std::string& field_name, const std::shared_ptr<arrow::DataType>& arrow_type,
    const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
    const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(
        bool omit_term_freq_and_positions,
        OptionsUtils::GetValueFromMap(options, kTantivyWriteOmitTermFreqAndPositions, false));
    PAIMON_ASSIGN_OR_RAISE(
        std::string tokenize_mode,
        OptionsUtils::GetValueFromMap(options, kJiebaTokenizeMode,
                                      std::string(kDefaultJiebaTokenizeMode)));
    PAIMON_ASSIGN_OR_RAISE(
        std::string tokenizer,
        OptionsUtils::GetValueFromMap(options, kTantivyWriteTokenizer,
                                      std::string(kDefaultTantivyWriteTokenizer)));
    // Jieba dict is only needed when actually using jieba. For tantivy built-in
    // tokenizers (e.g. "default") we don't force the caller to ship the jieba
    // dict dir — pass an empty string and Rust skips jieba construction.
    std::string dict_dir;
    if (tokenizer == "paimon_jieba") {
        PAIMON_ASSIGN_OR_RAISE(dict_dir, GetJiebaDictionaryDir());
    }

    PaimonTantivyWriter* raw = nullptr;
    PaimonTantivyStatus st = paimon_tantivy_writer_new(
        field_name.c_str(), tokenize_mode.c_str(),
        /*with_position=*/!omit_term_freq_and_positions, dict_dir.c_str(),
        tokenizer.c_str(), &raw);
    PAIMON_TANTIVY_RETURN_NOT_OK(st);
    WriterPtr writer(raw);
    return std::shared_ptr<TantivyGlobalIndexWriter>(new TantivyGlobalIndexWriter(
        field_name, arrow_type, std::move(writer), file_writer, options, pool));
}

TantivyGlobalIndexWriter::TantivyGlobalIndexWriter(
    const std::string& field_name, const std::shared_ptr<arrow::DataType>& arrow_type,
    WriterPtr writer, const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
    const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      field_name_(field_name),
      arrow_type_(arrow_type),
      writer_(std::move(writer)),
      file_writer_(file_writer),
      options_(options) {}

Status TantivyGlobalIndexWriter::AddBatch(::ArrowArray* arrow_array) {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                      arrow::ImportArray(arrow_array, arrow_type_));
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
    CHECK_NOT_NULL(struct_array,
                   "invalid input array in TantivyGlobalIndexWriter, must be struct array");
    auto field_array = struct_array->GetFieldByName(field_name_);
    CHECK_NOT_NULL(
        field_array,
        fmt::format("invalid input array in TantivyGlobalIndexWriter, field {} not in input array",
                    field_name_));
    auto string_array = std::dynamic_pointer_cast<arrow::StringArray>(field_array);
    CHECK_NOT_NULL(string_array,
                   fmt::format("invalid input array in TantivyGlobalIndexWriter, field array {} "
                               "is not a string array",
                               field_name_));

    for (int64_t i = 0; i < string_array->length(); i++) {
        const char* text_ptr = nullptr;
        size_t text_len = 0;
        if (!string_array->IsNull(i)) {
            std::string_view view = string_array->Value(i);
            text_ptr = view.data();
            text_len = view.size();
        }
        // B1 schema: pass the caller-tracked row_id as an explicit u64 field.
        PaimonTantivyStatus st = paimon_tantivy_writer_add(
            writer_.get(), static_cast<uint64_t>(row_id_), text_ptr, text_len);
        PAIMON_TANTIVY_RETURN_NOT_OK(st);
        row_id_++;
    }
    return Status::OK();
}

Result<std::vector<GlobalIndexIOMeta>> TantivyGlobalIndexWriter::Finish() {
    // W1 streaming finish: open the output file, pipe archive bytes from Rust
    // through `paimon_cpp_writer_push` directly into the OutputStream. Peak
    // RAM (Rust side) = 64KB buffer, independent of archive size.
    PAIMON_ASSIGN_OR_RAISE(std::string index_file_name,
                           file_writer_->NewFileName(kIdentifier));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OutputStream> out,
                           file_writer_->NewOutputStream(index_file_name));

    WriteCtx ctx{out.get(), Status::OK()};
    PaimonWriteCallbacks cb{
        static_cast<void*>(&ctx),
        paimon_cpp_writer_push,
    };

    int64_t rust_row_count = 0;
    ::PaimonTantivyStatus st =
        paimon_tantivy_writer_finish_streaming(writer_.get(), cb, &rust_row_count);
    if (st != PAIMON_TANTIVY_STATUS_OK) {
        // Prefer the detailed C++-side Status stashed by the write callback
        // (if the failure originated there); fall back to FFI-derived status.
        if (!ctx.last_error.ok()) {
            return ctx.last_error;
        }
        PAIMON_TANTIVY_RETURN_NOT_OK(st);
    }
    if (rust_row_count != row_id_) {
        return Status::Invalid(
            fmt::format("tantivy writer row count {} mismatch paimon inner row count {}",
                        rust_row_count, row_id_));
    }

    PAIMON_RETURN_NOT_OK(out->Flush());
    PAIMON_RETURN_NOT_OK(out->Close());

    PAIMON_ASSIGN_OR_RAISE(int64_t file_size, file_writer_->GetFileSize(index_file_name));
    std::string options_json;
    PAIMON_RETURN_NOT_OK(RapidJsonUtil::ToJsonString(options_, &options_json));
    auto meta_bytes = std::make_shared<Bytes>(options_json, pool_.get());
    GlobalIndexIOMeta meta(file_writer_->ToPath(index_file_name), file_size,
                           /*range_end=*/row_id_ - 1, /*metadata=*/meta_bytes);
    return std::vector<GlobalIndexIOMeta>({meta});
}

}  // namespace paimon::tantivy
