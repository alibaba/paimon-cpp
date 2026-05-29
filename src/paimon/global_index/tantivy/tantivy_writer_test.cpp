/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0.
 *
 * Stage 4 writer test: build a tantivy-fulltext global index from an Arrow batch,
 * persist it through GlobalIndexFileManager, then verify the resulting file
 * conforms to the packing format documented in tantivy_defs.h:
 *
 *   [i32 version | i32 file_count |
 *     (i32 name_len | name | i64 file_len | file_bytes)*]
 *
 * Stage 6 (reader) will round-trip these bytes back to a queryable index;
 * this stage only checks structural validity + meta correctness.
 */

#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/api.h"
#include "arrow/type.h"
#include "gtest/gtest.h"

#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/global_index/global_index_file_manager.h"
#include "paimon/core/index/index_path_factory.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/testing/utils/testharness.h"

#include "paimon/global_index/tantivy/tantivy_defs.h"
#include "paimon/global_index/tantivy/tantivy_global_index_writer.h"

#ifndef JIEBA_TEST_DICT_DIR
#error "JIEBA_TEST_DICT_DIR must be set at compile time"
#endif

namespace paimon::tantivy::test {

namespace {

class FakeIndexPathFactory : public IndexPathFactory {
 public:
    explicit FakeIndexPathFactory(const std::string& root) : root_(root) {}
    std::string NewPath() const override {
        assert(false);
        return "";
    }
    std::string ToPath(const std::shared_ptr<IndexFileMeta>&) const override {
        assert(false);
        return "";
    }
    std::string ToPath(const std::string& file_name) const override {
        return PathUtil::JoinPath(root_, file_name);
    }
    bool IsExternalPath() const override {
        return false;
    }

 private:
    std::string root_;
};

/// Read the entire file at `path` into a byte buffer.
std::vector<uint8_t> ReadFile(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    EXPECT_TRUE(in.good()) << "open " << path;
    in.seekg(0, std::ios::end);
    auto sz = static_cast<std::size_t>(in.tellg());
    in.seekg(0, std::ios::beg);
    std::vector<uint8_t> buf(sz);
    in.read(reinterpret_cast<char*>(buf.data()), sz);
    return buf;
}

/// Read a big-endian integer from a raw pointer.
template <typename T>
T ReadBE(const uint8_t* p) {
    T v = 0;
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        v = static_cast<T>((v << 8) | static_cast<T>(p[i]));
    }
    return v;
}

struct PackedEntry {
    std::string name;
    int64_t length = 0;
    std::size_t offset = 0;  // offset in the buffer where bytes start
};

/// Parse the packing header into a list of entries; verifies that the offsets
/// and lengths cover the full buffer with no leftover bytes.
/// Format (Java-compatible, big-endian, no version header):
///   [i32 BE file_count | (i32 BE name_len | name | i64 BE file_len | bytes)*]
std::vector<PackedEntry> ParsePacked(const std::vector<uint8_t>& bytes) {
    std::vector<PackedEntry> entries;
    EXPECT_GE(bytes.size(), 4u);
    int32_t file_count = ReadBE<int32_t>(bytes.data());
    EXPECT_GT(file_count, 0);
    std::size_t off = 4;
    for (int32_t i = 0; i < file_count; ++i) {
        EXPECT_LE(off + 4, bytes.size());
        int32_t nlen = ReadBE<int32_t>(bytes.data() + off);
        off += 4;
        EXPECT_GT(nlen, 0);
        EXPECT_LE(off + static_cast<std::size_t>(nlen), bytes.size());
        std::string name(reinterpret_cast<const char*>(bytes.data() + off),
                         static_cast<std::size_t>(nlen));
        off += nlen;
        EXPECT_LE(off + 8, bytes.size());
        int64_t flen = ReadBE<int64_t>(bytes.data() + off);
        off += 8;
        EXPECT_GE(flen, 0);
        EXPECT_LE(off + static_cast<std::size_t>(flen), bytes.size());
        entries.push_back({name, flen, off});
        off += static_cast<std::size_t>(flen);
    }
    EXPECT_EQ(off, bytes.size()) << "trailing bytes after pack";
    return entries;
}

class TantivyGlobalIndexWriterTest : public ::testing::Test {
 public:
    void SetUp() override {
        // Make jieba dict dir visible to the writer's GetJiebaDictionaryDir
        // (it reads the env var directly).
        setenv(kJiebaDictDirEnv, JIEBA_TEST_DICT_DIR, /*overwrite=*/1);
    }

    std::unique_ptr<::ArrowSchema> CreateArrowSchema(
        const std::shared_ptr<arrow::DataType>& data_type) const {
        auto c_schema = std::make_unique<::ArrowSchema>();
        EXPECT_TRUE(arrow::ExportType(*data_type, c_schema.get()).ok());
        return c_schema;
    }

    Result<std::vector<GlobalIndexIOMeta>> WriteIndex(
        const std::string& root, const std::shared_ptr<arrow::DataType>& data_type,
        const std::map<std::string, std::string>& options,
        const std::shared_ptr<arrow::Array>& array) {
        auto path_factory = std::make_shared<FakeIndexPathFactory>(root);
        auto file_writer = std::make_shared<GlobalIndexFileManager>(fs_, path_factory);
        PAIMON_ASSIGN_OR_RAISE(
            auto writer,
            TantivyGlobalIndexWriter::Create("f0", data_type, file_writer, options, pool_));
        ::ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
        std::vector<int64_t> relative_row_ids(array->length());
        for (int64_t i = 0; i < array->length(); ++i) relative_row_ids[i] = i;
        PAIMON_RETURN_NOT_OK(writer->AddBatch(&c_array, std::move(relative_row_ids)));
        return writer->Finish();
    }

 protected:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    std::shared_ptr<FileSystem> fs_ = std::make_shared<LocalFileSystem>();
    std::shared_ptr<arrow::DataType> data_type_ =
        arrow::struct_({arrow::field("f0", arrow::utf8())});
};

}  // namespace

TEST_F(TantivyGlobalIndexWriterTest, EnglishCorpusProducesValidPackedIndex) {
    auto root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(root_dir);
    std::string root = root_dir->Str();

    std::map<std::string, std::string> options = {
        {kTantivyWriteOmitTermFreqAndPositions, "false"},
    };
    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(data_type_, R"([
            ["This is an test document."],
            ["This is an new document document document."],
            ["Document document document document test."],
            ["unordered user-defined doc id"]
        ])")
            .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto metas, WriteIndex(root, data_type_, options, array));
    ASSERT_EQ(metas.size(), 1u);
    const auto& meta = metas[0];

    auto file_name = PathUtil::GetName(meta.file_path);
    EXPECT_TRUE(StringUtils::StartsWith(file_name, "tantivy-fulltext-global-index-"))
        << "file_name=" << file_name;
    EXPECT_TRUE(StringUtils::EndsWith(file_name, ".index"));
    ASSERT_TRUE(meta.metadata);
    EXPECT_EQ(std::string(meta.metadata->data(), meta.metadata->size()),
              R"({"write.omit-term-freq-and-position":"false"})");
    EXPECT_GT(meta.file_size, 8);

    auto bytes = ReadFile(meta.file_path);
    ASSERT_EQ(static_cast<int64_t>(bytes.size()), meta.file_size);
    auto entries = ParsePacked(bytes);
    EXPECT_FALSE(entries.empty());
    bool has_meta_json = false;
    for (const auto& e : entries) {
        if (e.name == "meta.json") has_meta_json = true;
    }
    EXPECT_TRUE(has_meta_json) << "expected meta.json in packed entries";
}

TEST_F(TantivyGlobalIndexWriterTest, ChineseCorpusProducesValidPackedIndex) {
    auto root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(root_dir);
    std::string root = root_dir->Str();

    std::map<std::string, std::string> options = {
        {kTantivyWriteOmitTermFreqAndPositions, "false"},
        {kTantivyWriteTokenizer, "paimon_jieba"},
        {kJiebaTokenizeMode, "query"},
    };
    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(data_type_, R"([
            ["千问是一个智能助手"],
            ["新一代AI助手发布"]
        ])")
            .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto metas, WriteIndex(root, data_type_, options, array));
    ASSERT_EQ(metas.size(), 1u);
    const auto& meta = metas[0];
    auto bytes = ReadFile(meta.file_path);
    ASSERT_EQ(static_cast<int64_t>(bytes.size()), meta.file_size);
    auto entries = ParsePacked(bytes);
    EXPECT_FALSE(entries.empty());
}

TEST_F(TantivyGlobalIndexWriterTest, NullStringRowsBecomeEmptyDocuments) {
    auto root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(root_dir);
    std::string root = root_dir->Str();

    std::map<std::string, std::string> options;
    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(data_type_, R"([
            ["nonempty"],
            [null],
            ["another"]
        ])")
            .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto metas, WriteIndex(root, data_type_, options, array));
    ASSERT_EQ(metas.size(), 1u);
}

TEST_F(TantivyGlobalIndexWriterTest, RejectsHmmTokenizeMode) {
    auto root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(root_dir);
    auto path_factory = std::make_shared<FakeIndexPathFactory>(root_dir->Str());
    auto file_writer = std::make_shared<GlobalIndexFileManager>(fs_, path_factory);
    // hmm rejection only fires when the jieba tokenizer is actually constructed,
    // so this test must explicitly opt into jieba (default tokenizer skips
    // jieba construction entirely).
    std::map<std::string, std::string> options = {
        {kTantivyWriteTokenizer, "paimon_jieba"},
        {kJiebaTokenizeMode, "hmm"},
    };
    auto res =
        TantivyGlobalIndexWriter::Create("f0", data_type_, file_writer, options, pool_);
    ASSERT_FALSE(res.ok());
    EXPECT_TRUE(res.status().IsNotImplemented()) << res.status().ToString();
}

}  // namespace paimon::tantivy::test
