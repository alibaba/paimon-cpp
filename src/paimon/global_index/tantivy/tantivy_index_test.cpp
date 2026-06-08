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
 *
 * Stage 8 integration test: end-to-end via TantivyGlobalIndex (writer + reader),
 * mirroring src/paimon/global_index/lucene/lucene_global_index_test.cpp.
 *
 * Validates parity with lucene-fts on:
 *   - file naming: "tantivy-fulltext-global-index-{uuid}.index"
 *   - meta JSON shape: option-prefix-stripped key/value pairs
 *   - 5 SearchTypes against an English corpus
 *   - 5 SearchTypes against a Chinese corpus (jieba "query" mode)
 *   - limit + pre_filter + scoring (Stage 7) interactions
 *   - factory registration: looking up "tantivy-fulltext" produces a tantivy indexer
 */

#include <memory>
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
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/global_index/global_indexer_factory.h"
#include "paimon/global_index/tantivy/tantivy_defs.h"
#include "paimon/global_index/tantivy/tantivy_global_index.h"
#include "paimon/global_index/tantivy/tantivy_global_index_factory.h"
#include "paimon/global_index/tantivy/tantivy_global_index_reader.h"
#include "paimon/testing/utils/testharness.h"

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

class TantivyGlobalIndexIntegrationTest : public ::testing::Test {
 public:
    void SetUp() override {
        setenv(kJiebaDictDirEnv, JIEBA_TEST_DICT_DIR, /*overwrite=*/1);
    }

    std::unique_ptr<::ArrowSchema> CreateArrowSchema(
        const std::shared_ptr<arrow::DataType>& data_type) const {
        auto c_schema = std::make_unique<::ArrowSchema>();
        EXPECT_TRUE(arrow::ExportType(*data_type, c_schema.get()).ok());
        return c_schema;
    }

    Result<GlobalIndexIOMeta> WriteGlobalIndex(const std::string& root,
                                               const std::shared_ptr<arrow::DataType>& data_type,
                                               const std::map<std::string, std::string>& options,
                                               const std::shared_ptr<arrow::Array>& array,
                                               int64_t /*unused_expected_range_end*/) const {
        auto global_index = std::make_shared<TantivyGlobalIndex>(options);
        auto path_factory = std::make_shared<FakeIndexPathFactory>(root);
        auto file_writer = std::make_shared<GlobalIndexFileManager>(fs_, path_factory);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexWriter> w,
                               global_index->CreateWriter("f0", CreateArrowSchema(data_type).get(),
                                                          file_writer, pool_));
        ::ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
        std::vector<int64_t> relative_row_ids(array->length());
        for (int64_t i = 0; i < array->length(); ++i) relative_row_ids[i] = i;
        PAIMON_RETURN_NOT_OK(w->AddBatch(&c_array, std::move(relative_row_ids)));
        PAIMON_ASSIGN_OR_RAISE(auto metas, w->Finish());
        EXPECT_EQ(metas.size(), 1u);
        auto file_name = PathUtil::GetName(metas[0].file_path);
        EXPECT_TRUE(StringUtils::StartsWith(file_name, "tantivy-fulltext-global-index-"))
            << file_name;
        EXPECT_TRUE(StringUtils::EndsWith(file_name, ".index"));
        EXPECT_TRUE(metas[0].metadata);
        return metas[0];
    }

    Result<std::shared_ptr<GlobalIndexReader>> CreateReader(
        const std::string& root, const std::shared_ptr<arrow::DataType>& data_type,
        const std::map<std::string, std::string>& options, const GlobalIndexIOMeta& meta) const {
        auto global_index = std::make_shared<TantivyGlobalIndex>(options);
        auto path_factory = std::make_shared<FakeIndexPathFactory>(root);
        auto file_reader = std::make_shared<GlobalIndexFileManager>(fs_, path_factory);
        return global_index->CreateReader(CreateArrowSchema(data_type).get(), file_reader, {meta},
                                          pool_);
    }

    void CheckResult(const std::shared_ptr<GlobalIndexResult>& result,
                     const std::vector<int64_t>& expected_ids) const {
        const RoaringBitmap64* bitmap = nullptr;
        if (auto scored = std::dynamic_pointer_cast<BitmapScoredGlobalIndexResult>(result)) {
            ASSERT_OK_AND_ASSIGN(bitmap, scored->GetBitmap());
            ASSERT_EQ(scored->GetScores().size(), expected_ids.size());
        } else if (auto plain = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(result)) {
            ASSERT_OK_AND_ASSIGN(bitmap, plain->GetBitmap());
        }
        ASSERT_TRUE(bitmap);
        ASSERT_EQ(*bitmap, RoaringBitmap64::From(expected_ids))
            << "result=" << bitmap->ToString()
            << ", expected=" << RoaringBitmap64::From(expected_ids).ToString();
    }

 protected:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    std::shared_ptr<FileSystem> fs_ = std::make_shared<LocalFileSystem>();
    std::shared_ptr<arrow::DataType> data_type_ =
        arrow::struct_({arrow::field("f0", arrow::utf8())});
};

}  // namespace

TEST_F(TantivyGlobalIndexIntegrationTest, EnglishCorpus) {
    auto root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(root_dir);
    std::string root = root_dir->Str();

    std::map<std::string, std::string> options = {
        {"tantivy-fulltext.write.omit-term-freq-and-position", "false"},
    };
    auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type_, R"([
        ["This is an test document."],
        ["This is an new document document document."],
        ["Document document document document test."],
        ["unordered user-defined doc id"]
    ])")
                     .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto meta, WriteGlobalIndex(root, data_type_, options, array, 3));
    EXPECT_EQ(std::string(meta.metadata->data(), meta.metadata->size()),
              R"({"write.omit-term-freq-and-position":"false"})");

    ASSERT_OK_AND_ASSIGN(auto reader, CreateReader(root, data_type_, options, meta));
    auto t_reader = std::dynamic_pointer_cast<TantivyGlobalIndexReader>(reader);
    ASSERT_TRUE(t_reader);
    EXPECT_EQ(t_reader->GetIndexType(), std::string(kIdentifier));

    auto run = [&](const std::string& q, FullTextSearch::SearchType t,
                   std::optional<int32_t> limit = std::nullopt,
                   std::optional<RoaringBitmap64> filter = std::nullopt) {
        // Use scored path so `limit` returns top-N by BM25, matching test
        // expectations (otherwise unscored Path B returns any-N, non-deterministic).
        auto fts = std::make_shared<FullTextSearch>("f0", limit, q, t, filter);
        fts->with_score = true;
        auto res = t_reader->VisitFullTextSearch(fts);
        EXPECT_TRUE(res.ok()) << res.status().ToString();
        return res.value();
    };

    CheckResult(run("document", FullTextSearch::SearchType::MATCH_ALL, 10), {2, 1, 0});
    CheckResult(run("document", FullTextSearch::SearchType::MATCH_ANY, 1), {2});
    CheckResult(run("test document", FullTextSearch::SearchType::MATCH_ALL, 10), {2, 0});
    CheckResult(run("test new", FullTextSearch::SearchType::MATCH_ANY, 10), {1, 0, 2});
    CheckResult(run("test document", FullTextSearch::SearchType::PHRASE, 10), {0});
    CheckResult(run("unordered", FullTextSearch::SearchType::MATCH_ALL, 10), {3});
    CheckResult(run("unorder", FullTextSearch::SearchType::PREFIX, 10), {3});
    CheckResult(run("*order*", FullTextSearch::SearchType::WILDCARD, 10), {3});
    CheckResult(run("*or*er*", FullTextSearch::SearchType::WILDCARD, 10), {3});

    // pre_filter
    CheckResult(
        run("document", FullTextSearch::SearchType::MATCH_ALL, 10, RoaringBitmap64::From({0l, 1l})),
        {0, 1});
    CheckResult(run("document", FullTextSearch::SearchType::MATCH_ALL, 10,
                    RoaringBitmap64::From({2l, 100l})),
                {2});
    CheckResult(run("document", FullTextSearch::SearchType::MATCH_ALL, 10,
                    RoaringBitmap64::From({20l, 100l})),
                {});

    // No limit
    CheckResult(run("document", FullTextSearch::SearchType::MATCH_ALL), {0, 1, 2});
    CheckResult(run("document", FullTextSearch::SearchType::MATCH_ALL, std::nullopt,
                    RoaringBitmap64::From({2l})),
                {2});
    CheckResult(run("document test", FullTextSearch::SearchType::MATCH_ALL, std::nullopt,
                    RoaringBitmap64::From({1l, 2l, 3l, 100l})),
                {2});
}

TEST_F(TantivyGlobalIndexIntegrationTest, ChineseCorpus) {
    auto root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(root_dir);
    std::string root = root_dir->Str();

    std::map<std::string, std::string> options = {
        {"tantivy-fulltext.write.omit-term-freq-and-position", "false"},
        {"tantivy-fulltext.tantivy.write.tokenizer", "paimon_jieba"},
        {"tantivy-fulltext.jieba.tokenize-mode", "query"},
    };
    auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type_, R"([
["QianWen 是一个基于 AI 的智能助手，类似于 Siri 和 Alexa。我们正在用 Python 开发 QianWen 的 Natural Language Understanding 模块，该模块支持多轮对话和意图识别功能，是新一代智能助手的核心技术之一。"],
["最近开源了一个新项目叫ｑｉａｎｗｅｎ（全角字符），功能类似之前的 Qianwen，是一个面向 AI 应用的智能助手。它不仅支持 Machine Learning 和 NLP 技术，还提供了可扩展的开发框架，便于开发者构建自己的智能助手系统。"],
["我们在测试 qianwen-core v1.2 和 ai-engine-alpha 中的 bug，重点优化了 qianwen 的响应速度和稳定性。本次更新增强了核心模块的功能，提升了智能助手的开发效率，并修复了与 NLP 模块相关的多个问题。"],
["AI 助手开发中常用的技术包括 Speech Recognition、Natural Language Processing 和 Recommendation System。我们使用 TensorFlow 和 PyTorch 构建模型，开发了多个智能助手原型，支持语音交互和上下文理解功能，是当前热门的人工智能发展应用方向。"],
["新一代的 AI 助手代号为「千问」，内部命名为 QianwenX-2024，计划在 next quarter 发布。QianwenX 将集成更强的 multimodel 能力，支持图像和文本联合处理，进一步提升智能助手的理解能力和交互体验，是未来智能助手的重要发展方向。"]
    ])")
                     .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto meta, WriteGlobalIndex(root, data_type_, options, array, 4));
    EXPECT_EQ(
        std::string(meta.metadata->data(), meta.metadata->size()),
        R"({"jieba.tokenize-mode":"query","tantivy.write.tokenizer":"paimon_jieba","write.omit-term-freq-and-position":"false"})");

    ASSERT_OK_AND_ASSIGN(auto reader, CreateReader(root, data_type_, options, meta));
    auto t_reader = std::dynamic_pointer_cast<TantivyGlobalIndexReader>(reader);
    ASSERT_TRUE(t_reader);

    auto run = [&](const std::string& q, FullTextSearch::SearchType t,
                   std::optional<int32_t> limit = std::nullopt,
                   std::optional<RoaringBitmap64> filter = std::nullopt) {
        // Use scored path so `limit` returns top-N by BM25, matching test
        // expectations (otherwise unscored Path B returns any-N, non-deterministic).
        auto fts = std::make_shared<FullTextSearch>("f0", limit, q, t, filter);
        fts->with_score = true;
        auto res = t_reader->VisitFullTextSearch(fts);
        EXPECT_TRUE(res.ok()) << res.status().ToString();
        return res.value();
    };

    CheckResult(run("模块", FullTextSearch::SearchType::MATCH_ALL, 10), {0, 2});
    CheckResult(run("模块", FullTextSearch::SearchType::MATCH_ANY, 1), {0});
    CheckResult(run("模块技术", FullTextSearch::SearchType::MATCH_ALL, 10), {0});
    CheckResult(run("模块技术", FullTextSearch::SearchType::MATCH_ANY, 10), {0, 1, 2, 3});
    CheckResult(run("发展方向", FullTextSearch::SearchType::PHRASE, 10), {4});
    CheckResult(run("模块技术", FullTextSearch::SearchType::MATCH_ANY, 10,
                    RoaringBitmap64::From({1l, 3l, 4l})),
                {1, 3});
    CheckResult(run("模块技术", FullTextSearch::SearchType::MATCH_ANY), {0, 1, 2, 3});
}

TEST_F(TantivyGlobalIndexIntegrationTest, FactoryLookupReturnsTantivyIndexer) {
    std::map<std::string, std::string> options = {
        {"tantivy-fulltext.jieba.tokenize-mode", "query"},
    };
    // Identifier passed to GlobalIndexerFactory::Get is the prefix; "-global"
    // is appended automatically. So "tantivy-fulltext" must route to our factory.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<GlobalIndexer> indexer,
                         GlobalIndexerFactory::Get("tantivy-fulltext", options));
    ASSERT_TRUE(indexer);
    auto* casted = dynamic_cast<TantivyGlobalIndex*>(indexer.get());
    ASSERT_TRUE(casted) << "factory did not return a TantivyGlobalIndex";
}

}  // namespace paimon::tantivy::test
