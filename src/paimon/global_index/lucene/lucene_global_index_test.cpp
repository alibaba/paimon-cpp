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

#include "arrow/c/bridge.h"
#include "arrow/ipc/api.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/global_index/global_index_file_manager.h"
#include "paimon/core/index/index_path_factory.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/global_index/lucene/lucene_global_index_reader.h"
#include "paimon/global_index/lucene/lucene_global_index_writer.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::lucene::test {
class LuceneGlobalIndexTest : public ::testing::Test,
                              public ::testing::WithParamInterface<int32_t> {
 public:
    void SetUp() override {}
    void TearDown() override {}

    class FakeIndexPathFactory : public IndexPathFactory {
     public:
        explicit FakeIndexPathFactory(const std::string& index_path) : index_path_(index_path) {}
        std::string NewPath() const override {
            assert(false);
            return "";
        }
        std::string ToPath(const std::shared_ptr<IndexFileMeta>& file) const override {
            assert(false);
            return "";
        }
        std::string ToPath(const std::string& file_name) const override {
            return PathUtil::JoinPath(index_path_, file_name);
        }
        bool IsExternalPath() const override {
            return false;
        }

     private:
        std::string index_path_;
    };

    std::unique_ptr<::ArrowSchema> CreateArrowSchema(
        const std::shared_ptr<arrow::DataType>& data_type) const {
        auto c_schema = std::make_unique<::ArrowSchema>();
        EXPECT_TRUE(arrow::ExportType(*data_type, c_schema.get()).ok());
        return c_schema;
    }

    Result<GlobalIndexIOMeta> WriteGlobalIndex(const std::string& index_root,
                                               const std::shared_ptr<arrow::DataType>& data_type,
                                               const std::map<std::string, std::string>& options,
                                               const std::shared_ptr<arrow::Array>& array,
                                               const Range& expected_range) const {
        auto global_index = std::make_shared<LuceneGlobalIndex>(options);
        auto path_factory = std::make_shared<FakeIndexPathFactory>(index_root);
        auto file_writer = std::make_shared<GlobalIndexFileManager>(fs_, path_factory);

        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexWriter> global_writer,
                               global_index->CreateWriter("f0", CreateArrowSchema(data_type).get(),
                                                          file_writer, pool_));

        ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
        PAIMON_RETURN_NOT_OK(global_writer->AddBatch(&c_array));
        PAIMON_ASSIGN_OR_RAISE(auto result_metas, global_writer->Finish());
        // check meta
        EXPECT_EQ(result_metas.size(), 1);
        auto file_name = PathUtil::GetName(result_metas[0].file_path);
        EXPECT_TRUE(StringUtils::StartsWith(file_name, "lucene-fts-global-index-"));
        EXPECT_TRUE(StringUtils::EndsWith(file_name, ".index"));
        EXPECT_EQ(result_metas[0].range_end, expected_range.to);
        EXPECT_TRUE(result_metas[0].metadata);
        return result_metas[0];
    }

    Result<std::shared_ptr<GlobalIndexReader>> CreateGlobalIndexReader(
        const std::string& index_root, const std::shared_ptr<arrow::DataType>& data_type,
        const std::map<std::string, std::string>& options, const GlobalIndexIOMeta& meta) const {
        auto global_index = std::make_shared<LuceneGlobalIndex>(options);
        auto path_factory = std::make_shared<FakeIndexPathFactory>(index_root);
        auto file_reader = std::make_shared<GlobalIndexFileManager>(fs_, path_factory);
        return global_index->CreateReader(CreateArrowSchema(data_type).get(), file_reader, {meta},
                                          pool_);
    }

    void CheckResult(const std::shared_ptr<GlobalIndexResult>& result,
                     const std::vector<int64_t>& expected_ids) const {
        const RoaringBitmap64* bitmap = nullptr;
        if (auto scored_result = std::dynamic_pointer_cast<BitmapScoredGlobalIndexResult>(result)) {
            ASSERT_OK_AND_ASSIGN(bitmap, scored_result->GetBitmap());
            ASSERT_EQ(scored_result->GetScores().size(), expected_ids.size());
        } else if (auto bitmap_result =
                       std::dynamic_pointer_cast<BitmapGlobalIndexResult>(result)) {
            ASSERT_OK_AND_ASSIGN(bitmap, bitmap_result->GetBitmap());
        }
        ASSERT_TRUE(bitmap);
        ASSERT_EQ(*bitmap, RoaringBitmap64::From(expected_ids))
            << "result=" << bitmap->ToString()
            << ", expected=" << RoaringBitmap64::From(expected_ids).ToString();
    }

 private:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    std::shared_ptr<FileSystem> fs_ = std::make_shared<LocalFileSystem>();
    std::shared_ptr<arrow::DataType> data_type_ =
        arrow::struct_({arrow::field("f0", arrow::utf8())});
};

TEST_P(LuceneGlobalIndexTest, TestSimple) {
    int32_t read_buffer_size = GetParam();

    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();

    std::map<std::string, std::string> options = {
        {"lucene-fts.write.omit-term-freq-and-position", "false"},
        {"lucene-fts.read.buffer-size", std::to_string(read_buffer_size)}};
    std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(data_type_,
                                                                                    R"([
        ["This is an test document."],
        ["This is an new document document document."],
        ["Document document document document test."],
        ["unordered user-defined doc id"]
    ])")
                                              .ValueOrDie();

    // write index
    ASSERT_OK_AND_ASSIGN(auto meta,
                         WriteGlobalIndex(test_root, data_type_, options, array, Range(0, 3)));
    if (read_buffer_size == 10) {
        ASSERT_EQ(std::string(meta.metadata->data(), meta.metadata->size()),
                  R"({"read.buffer-size":"10","write.omit-term-freq-and-position":"false"})");
    }

    // create reader
    ASSERT_OK_AND_ASSIGN(auto reader,
                         CreateGlobalIndexReader(test_root, data_type_, options, meta));
    auto lucene_reader = std::dynamic_pointer_cast<LuceneGlobalIndexReader>(reader);
    ASSERT_TRUE(lucene_reader);

    // test visit
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             lucene_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                                 "f0",
                                 /*limit=*/10, "document", FullTextSearch::SearchType::MATCH_ALL,
                                 /*pre_filter=*/std::nullopt)));
        CheckResult(result, {2l, 1l, 0l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             lucene_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                                 "f0",
                                 /*limit=*/1, "document", FullTextSearch::SearchType::MATCH_ANY,
                                 /*pre_filter=*/std::nullopt)));
        CheckResult(result, {2l});
    }
    {
        ASSERT_OK_AND_ASSIGN(
            auto result, lucene_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                             "f0",
                             /*limit=*/10, "test document", FullTextSearch::SearchType::MATCH_ALL,
                             /*pre_filter=*/std::nullopt)));
        CheckResult(result, {2l, 0l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             lucene_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                                 "f0",
                                 /*limit=*/10, "test new", FullTextSearch::SearchType::MATCH_ANY,
                                 /*pre_filter=*/std::nullopt)));
        CheckResult(result, {1l, 0l, 2l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             lucene_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                                 "f0",
                                 /*limit=*/10, "test document", FullTextSearch::SearchType::PHRASE,
                                 /*pre_filter=*/std::nullopt)));
        CheckResult(result, {0l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             lucene_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                                 "f0",
                                 /*limit=*/10, "unordered", FullTextSearch::SearchType::MATCH_ALL,
                                 /*pre_filter=*/std::nullopt)));
        CheckResult(result, {3l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             lucene_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                                 "f0",
                                 /*limit=*/10, "unorder", FullTextSearch::SearchType::PREFIX,
                                 /*pre_filter=*/std::nullopt)));
        CheckResult(result, {3l});
    }
    // test wildcard query
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             lucene_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                                 "f0",
                                 /*limit=*/10, "*order*", FullTextSearch::SearchType::WILDCARD,
                                 /*pre_filter=*/std::nullopt)));
        CheckResult(result, {3l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             lucene_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                                 "f0",
                                 /*limit=*/10, "*or*er*", FullTextSearch::SearchType::WILDCARD,
                                 /*pre_filter=*/std::nullopt)));
        CheckResult(result, {3l});
    }
    // test filter
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             lucene_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                                 "f0",
                                 /*limit=*/10, "document", FullTextSearch::SearchType::MATCH_ALL,
                                 /*pre_filter=*/RoaringBitmap64::From({0l, 1l}))));
        CheckResult(result, {0l, 1l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             lucene_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                                 "f0",
                                 /*limit=*/10, "document", FullTextSearch::SearchType::MATCH_ALL,
                                 /*pre_filter=*/RoaringBitmap64::From({2l, 100l}))));
        CheckResult(result, {2l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             lucene_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                                 "f0",
                                 /*limit=*/10, "document", FullTextSearch::SearchType::MATCH_ALL,
                                 /*pre_filter=*/RoaringBitmap64::From({20l, 100l}))));
        CheckResult(result, {});
    }
    // test no limit
    {
        ASSERT_OK_AND_ASSIGN(
            auto result,
            lucene_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                "f0",
                /*limit=*/std::nullopt, "document", FullTextSearch::SearchType::MATCH_ALL,
                /*pre_filter=*/std::nullopt)));
        CheckResult(result, {0l, 1l, 2l});
    }
    {
        ASSERT_OK_AND_ASSIGN(
            auto result,
            lucene_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                "f0",
                /*limit=*/std::nullopt, "document", FullTextSearch::SearchType::MATCH_ALL,
                /*pre_filter=*/RoaringBitmap64::From({2l}))));
        CheckResult(result, {2l});
    }
    {
        ASSERT_OK_AND_ASSIGN(
            auto result,
            lucene_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                "f0",
                /*limit=*/std::nullopt, "document test", FullTextSearch::SearchType::MATCH_ALL,
                /*pre_filter=*/RoaringBitmap64::From({1l, 2l, 3l, 100l}))));
        CheckResult(result, {2l});
    }
}

TEST_P(LuceneGlobalIndexTest, TestSimpleChinese) {
    int32_t read_buffer_size = GetParam();

    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();

    std::map<std::string, std::string> options = {
        {"lucene-fts.write.omit-term-freq-and-position", "false"},
        {"lucene-fts.read.buffer-size", std::to_string(read_buffer_size)},
        {"lucene-fts.jieba.tokenize-mode", "query"},
    };

    std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(data_type_,
                                                                                    R"([
["QianWen 是一个基于 AI 的智能助手，类似于 Siri 和 Alexa。我们正在用 Python 开发 QianWen 的 Natural Language Understanding 模块，该模块支持多轮对话和意图识别功能，是新一代智能助手的核心技术之一。"],
["最近开源了一个新项目叫ｑｉａｎｗｅｎ（全角字符），功能类似之前的 Qianwen，是一个面向 AI 应用的智能助手。它不仅支持 Machine Learning 和 NLP 技术，还提供了可扩展的开发框架，便于开发者构建自己的智能助手系统。"],
["我们在测试 qianwen-core v1.2 和 ai-engine-alpha 中的 bug，重点优化了 qianwen 的响应速度和稳定性。本次更新增强了核心模块的功能，提升了智能助手的开发效率，并修复了与 NLP 模块相关的多个问题。"],
["AI 助手开发中常用的技术包括 Speech Recognition、Natural Language Processing 和 Recommendation System。我们使用 TensorFlow 和 PyTorch 构建模型，开发了多个智能助手原型，支持语音交互和上下文理解功能，是当前热门的人工智能发展应用方向。"],
["新一代的 AI 助手代号为「千问」，内部命名为 QianwenX-2024，计划在 next quarter 发布。QianwenX 将集成更强的 multimodel 能力，支持图像和文本联合处理，进一步提升智能助手的理解能力和交互体验，是未来智能助手的重要发展方向。"]
    ])")
                                              .ValueOrDie();

    // write index
    ASSERT_OK_AND_ASSIGN(auto meta,
                         WriteGlobalIndex(test_root, data_type_, options, array, Range(0, 4)));
    if (read_buffer_size == 10) {
        ASSERT_EQ(
            std::string(meta.metadata->data(), meta.metadata->size()),
            R"({"jieba.tokenize-mode":"query","read.buffer-size":"10","write.omit-term-freq-and-position":"false"})");
    }

    // create reader
    ASSERT_OK_AND_ASSIGN(auto reader,
                         CreateGlobalIndexReader(test_root, data_type_, options, meta));
    auto lucene_reader = std::dynamic_pointer_cast<LuceneGlobalIndexReader>(reader);
    ASSERT_TRUE(lucene_reader);

    // test visit
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             lucene_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                                 "f0",
                                 /*limit=*/10, "模块", FullTextSearch::SearchType::MATCH_ALL,
                                 /*pre_filter=*/std::nullopt)));
        CheckResult(result, {0l, 2l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             lucene_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                                 "f0",
                                 /*limit=*/1, "模块", FullTextSearch::SearchType::MATCH_ANY,
                                 /*pre_filter=*/std::nullopt)));
        CheckResult(result, {0l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             lucene_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                                 "f0",
                                 /*limit=*/10, "模块技术", FullTextSearch::SearchType::MATCH_ALL,
                                 /*pre_filter=*/std::nullopt)));
        CheckResult(result, {0l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             lucene_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                                 "f0",
                                 /*limit=*/10, "模块技术", FullTextSearch::SearchType::MATCH_ANY,
                                 /*pre_filter=*/std::nullopt)));
        CheckResult(result, {0l, 1l, 2l, 3l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             lucene_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                                 "f0",
                                 /*limit=*/10, "发展方向", FullTextSearch::SearchType::PHRASE,
                                 /*pre_filter=*/std::nullopt)));
        CheckResult(result, {4l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             lucene_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                                 "f0",
                                 /*limit=*/10, "发", FullTextSearch::SearchType::PREFIX,
                                 /*pre_filter=*/std::nullopt)));
        CheckResult(result, {3l, 4l});
    }
    // test wildcard query
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             lucene_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                                 "f0",
                                 /*limit=*/10, "*发*", FullTextSearch::SearchType::WILDCARD,
                                 /*pre_filter=*/std::nullopt)));
        CheckResult(result, {0l, 1l, 2l, 3l, 4l});
    }
    // test filter
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             lucene_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                                 "f0",
                                 /*limit=*/10, "模块技术", FullTextSearch::SearchType::MATCH_ANY,
                                 /*pre_filter=*/RoaringBitmap64::From({1l, 3l, 4l}))));

        CheckResult(result, {1l, 3l});
    }
    // test no limit
    {
        ASSERT_OK_AND_ASSIGN(
            auto result,
            lucene_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                "f0",
                /*limit=*/std::nullopt, "模块技术", FullTextSearch::SearchType::MATCH_ANY,
                /*pre_filter=*/std::nullopt)));
        CheckResult(result, {0l, 1l, 2l, 3l});
    }
}

INSTANTIATE_TEST_SUITE_P(ReadBufferSize, LuceneGlobalIndexTest,
                         ::testing::ValuesIn(std::vector<int32_t>({10, 100, 1024})));

}  // namespace paimon::lucene::test
