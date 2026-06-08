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
 * Stage 6 reader test: write an index via TantivyGlobalIndexWriter, persist
 * it, then run all 5 FullTextSearch SearchTypes through TantivyGlobalIndexReader
 * and assert matching local row ids. Mirrors the no-limit / no-pre_filter
 * subset of paimon-lucene-index-test's TestSimple/TestSimpleChinese cases.
 *
 * limit / pre_filter coverage lands in Stage 7 (paimon-tantivy-filter-limit-test).
 */

#include <memory>
#include <vector>

#include "arrow/array.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/api.h"
#include "arrow/type.h"
#include "gtest/gtest.h"

#include "paimon/common/utils/path_util.h"
#include "paimon/core/global_index/global_index_file_manager.h"
#include "paimon/core/index/index_path_factory.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/testing/utils/testharness.h"

#include "paimon/global_index/tantivy/tantivy_defs.h"
#include "paimon/global_index/tantivy/tantivy_global_index_reader.h"
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

class TantivyReaderTest : public ::testing::Test {
 public:
    void SetUp() override {
        setenv(kJiebaDictDirEnv, JIEBA_TEST_DICT_DIR, /*overwrite=*/1);
    }

    /// Write `array` to a fresh test directory and return (file_manager, meta).
    std::pair<std::shared_ptr<GlobalIndexFileManager>, GlobalIndexIOMeta> WriteAndOpen(
        const std::shared_ptr<arrow::Array>& array,
        const std::map<std::string, std::string>& options) {
        auto root_dir = paimon::test::UniqueTestDirectory::Create();
        EXPECT_TRUE(root_dir);
        // Hold the directory alive across this test by leaking the
        // unique_ptr's owned dir into a static — UniqueTestDirectory::Create
        // returns RAII; need the path to outlive the function.
        // Easier path: reach in via member, save root string, then wrap a
        // fresh GlobalIndexFileManager pointing at that string.
        std::string root = root_dir->Str();
        // keep the directory alive
        kept_dirs_.push_back(std::move(root_dir));

        auto path_factory = std::make_shared<FakeIndexPathFactory>(root);
        auto fm = std::make_shared<GlobalIndexFileManager>(fs_, path_factory);
        auto data_type = arrow::struct_({arrow::field("f0", arrow::utf8())});
        auto writer_res =
            TantivyGlobalIndexWriter::Create("f0", data_type, fm, options, GetDefaultPool());
        EXPECT_TRUE(writer_res.ok()) << writer_res.status().ToString();
        auto writer = writer_res.value();
        ::ArrowArray c_array;
        EXPECT_TRUE(arrow::ExportArray(*array, &c_array).ok());
        std::vector<int64_t> relative_row_ids(array->length());
        for (int64_t i = 0; i < array->length(); ++i) relative_row_ids[i] = i;
        EXPECT_TRUE(writer->AddBatch(&c_array, std::move(relative_row_ids)).ok());
        auto metas_res = writer->Finish();
        EXPECT_TRUE(metas_res.ok()) << metas_res.status().ToString();
        return {fm, metas_res.value()[0]};
    }

    static std::vector<int64_t> BitmapToVec(
        const std::shared_ptr<GlobalIndexResult>& result) {
        auto bg = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(result);
        EXPECT_TRUE(bg) << "expected BitmapGlobalIndexResult";
        auto bitmap_res = bg->GetBitmap();
        EXPECT_TRUE(bitmap_res.ok()) << bitmap_res.status().ToString();
        const RoaringBitmap64* bitmap = bitmap_res.value();
        std::vector<int64_t> ids;
        for (auto it = bitmap->Begin(); it != bitmap->End(); ++it) {
            ids.push_back(static_cast<int64_t>(*it));
        }
        std::sort(ids.begin(), ids.end());
        return ids;
    }

    std::shared_ptr<arrow::DataType> DataType() const {
        return arrow::struct_({arrow::field("f0", arrow::utf8())});
    }

 protected:
    std::shared_ptr<FileSystem> fs_ = std::make_shared<LocalFileSystem>();
    /// Keep test directories alive for the duration of the test.
    std::vector<std::unique_ptr<paimon::test::UniqueTestDirectory>> kept_dirs_;
};

}  // namespace

TEST_F(TantivyReaderTest, EnglishMatchAllAndAny) {
    auto array = arrow::ipc::internal::json::ArrayFromJSON(DataType(), R"([
        ["This is an test document."],
        ["This is an new document document document."],
        ["Document document document document test."],
        ["unordered user-defined doc id"]
    ])")
                     .ValueOrDie();
    auto [fm, meta] = WriteAndOpen(array, {});
    ASSERT_OK_AND_ASSIGN(auto reader,
                         TantivyGlobalIndexReader::Create("f0", meta, fm, {}, GetDefaultPool()));

    auto run = [&](const std::string& q, FullTextSearch::SearchType t) {
        auto res = reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
            "f0", /*limit=*/std::nullopt, q, t, /*pre_filter=*/std::nullopt));
        EXPECT_TRUE(res.ok()) << res.status().ToString();
        return BitmapToVec(res.value());
    };

    EXPECT_EQ(run("document", FullTextSearch::SearchType::MATCH_ALL),
              (std::vector<int64_t>{0, 1, 2}));
    EXPECT_EQ(run("test document", FullTextSearch::SearchType::MATCH_ALL),
              (std::vector<int64_t>{0, 2}));
    EXPECT_EQ(run("test new", FullTextSearch::SearchType::MATCH_ANY),
              (std::vector<int64_t>{0, 1, 2}));
}

TEST_F(TantivyReaderTest, EnglishPhrasePrefixWildcard) {
    auto array = arrow::ipc::internal::json::ArrayFromJSON(DataType(), R"([
        ["This is an test document."],
        ["This is an new document document document."],
        ["Document document document document test."],
        ["unordered user-defined doc id"]
    ])")
                     .ValueOrDie();
    auto [fm, meta] = WriteAndOpen(array, {});
    ASSERT_OK_AND_ASSIGN(auto reader,
                         TantivyGlobalIndexReader::Create("f0", meta, fm, {}, GetDefaultPool()));

    auto run = [&](const std::string& q, FullTextSearch::SearchType t) {
        auto res = reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
            "f0", /*limit=*/std::nullopt, q, t, /*pre_filter=*/std::nullopt));
        EXPECT_TRUE(res.ok()) << res.status().ToString();
        return BitmapToVec(res.value());
    };

    // "test document" is consecutive only in row 0 ("an test document.")
    EXPECT_EQ(run("test document", FullTextSearch::SearchType::PHRASE),
              (std::vector<int64_t>{0}));
    EXPECT_EQ(run("unorder", FullTextSearch::SearchType::PREFIX),
              (std::vector<int64_t>{3}));
    EXPECT_EQ(run("*order*", FullTextSearch::SearchType::WILDCARD),
              (std::vector<int64_t>{3}));
    EXPECT_EQ(run("*or*er*", FullTextSearch::SearchType::WILDCARD),
              (std::vector<int64_t>{3}));
}

TEST_F(TantivyReaderTest, ChineseQueryMode) {
    auto array = arrow::ipc::internal::json::ArrayFromJSON(DataType(), R"([
["QianWen 是一个基于 AI 的智能助手，类似于 Siri 和 Alexa。我们正在用 Python 开发 QianWen 的 Natural Language Understanding 模块，该模块支持多轮对话和意图识别功能，是新一代智能助手的核心技术之一。"],
["最近开源了一个新项目叫ｑｉａｎｗｅｎ（全角字符），功能类似之前的 Qianwen，是一个面向 AI 应用的智能助手。它不仅支持 Machine Learning 和 NLP 技术，还提供了可扩展的开发框架，便于开发者构建自己的智能助手系统。"],
["我们在测试 qianwen-core v1.2 和 ai-engine-alpha 中的 bug，重点优化了 qianwen 的响应速度和稳定性。本次更新增强了核心模块的功能，提升了智能助手的开发效率，并修复了与 NLP 模块相关的多个问题。"],
["AI 助手开发中常用的技术包括 Speech Recognition、Natural Language Processing 和 Recommendation System。我们使用 TensorFlow 和 PyTorch 构建模型，开发了多个智能助手原型，支持语音交互和上下文理解功能，是当前热门的人工智能发展应用方向。"],
["新一代的 AI 助手代号为「千问」，内部命名为 QianwenX-2024，计划在 next quarter 发布。QianwenX 将集成更强的 multimodel 能力，支持图像和文本联合处理，进一步提升智能助手的理解能力和交互体验，是未来智能助手的重要发展方向。"]
    ])")
                     .ValueOrDie();
    std::map<std::string, std::string> options = {
        {kTantivyWriteTokenizer, "paimon_jieba"},
        {kJiebaTokenizeMode, "query"},
    };
    auto [fm, meta] = WriteAndOpen(array, options);
    ASSERT_OK_AND_ASSIGN(
        auto reader, TantivyGlobalIndexReader::Create("f0", meta, fm, options, GetDefaultPool()));

    auto run = [&](const std::string& q, FullTextSearch::SearchType t) {
        auto res = reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
            "f0", /*limit=*/std::nullopt, q, t, /*pre_filter=*/std::nullopt));
        EXPECT_TRUE(res.ok()) << res.status().ToString();
        return BitmapToVec(res.value());
    };

    EXPECT_EQ(run("模块", FullTextSearch::SearchType::MATCH_ALL),
              (std::vector<int64_t>{0, 2}));
    EXPECT_EQ(run("模块技术", FullTextSearch::SearchType::MATCH_ALL),
              (std::vector<int64_t>{0}));
    EXPECT_EQ(run("模块技术", FullTextSearch::SearchType::MATCH_ANY),
              (std::vector<int64_t>{0, 1, 2, 3}));
    EXPECT_EQ(run("发展方向", FullTextSearch::SearchType::PHRASE),
              (std::vector<int64_t>{4}));
}

}  // namespace paimon::tantivy::test
