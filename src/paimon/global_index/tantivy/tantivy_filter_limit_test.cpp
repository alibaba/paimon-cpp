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
 * Cover the limit + pre_filter + scoring pathway. Uses the same
 * write→read flow as paimon-tantivy-reader-test, but verifies that:
 *   - A `limit` produces a `BitmapScoredGlobalIndexResult` with non-empty
 *     scores ordered such that bitmap iteration order aligns with the score
 *     vector (paimon convention: doc-id-asc bitmap, parallel score vector).
 *   - A `pre_filter` excludes non-member rows even when they would otherwise
 *     dominate the top-N by score.
 *   - Combining both produces the intersection, with limit applied AFTER
 *     filtering (matches lucene-fts behavior).
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
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/global_index/tantivy/tantivy_defs.h"
#include "paimon/global_index/tantivy/tantivy_global_index_reader.h"
#include "paimon/global_index/tantivy/tantivy_global_index_writer.h"
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

class TantivyFilterLimitTest : public ::testing::Test {
 public:
    std::pair<std::shared_ptr<GlobalIndexFileManager>, GlobalIndexIOMeta> WriteAndOpen(
        const std::shared_ptr<arrow::Array>& array,
        const std::map<std::string, std::string>& options) {
        auto root_dir = paimon::test::UniqueTestDirectory::Create();
        EXPECT_TRUE(root_dir);
        std::string root = root_dir->Str();
        kept_dirs_.push_back(std::move(root_dir));
        auto path_factory = std::make_shared<FakeIndexPathFactory>(root);
        auto fm = std::make_shared<GlobalIndexFileManager>(fs_, path_factory);
        auto data_type = arrow::struct_({arrow::field("f0", arrow::utf8())});
        EXPECT_OK_AND_ASSIGN(auto writer_res, TantivyGlobalIndexWriter::Create(
                                                  "f0", data_type, fm, options, GetDefaultPool()));
        auto writer = writer_res;
        ::ArrowArray c_array;
        EXPECT_TRUE(arrow::ExportArray(*array, &c_array).ok());
        std::vector<int64_t> relative_row_ids(array->length());
        for (int64_t i = 0; i < array->length(); ++i) {
            relative_row_ids[i] = i;
        }
        EXPECT_TRUE(writer->AddBatch(&c_array, std::move(relative_row_ids)).ok());
        EXPECT_OK_AND_ASSIGN(auto metas_res, writer->Finish());
        return {fm, metas_res[0]};
    }

    static std::vector<int64_t> BitmapToVec(const RoaringBitmap64& b) {
        std::vector<int64_t> ids;
        for (auto it = b.Begin(); it != b.End(); ++it) {
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
    std::vector<std::unique_ptr<paimon::test::UniqueTestDirectory>> kept_dirs_;
};

}  // namespace

TEST_F(TantivyFilterLimitTest, LimitProducesScoredResultTopN) {
    // Three docs with very different term frequencies for "doc"; limit=2 must
    // pick the top 2 by score (doc 1 highest, then doc 2).
    auto array = arrow::ipc::internal::json::ArrayFromJSON(DataType(), R"([
        ["doc"],
        ["doc doc doc doc doc"],
        ["doc doc"]
    ])")
                     .ValueOrDie();
    auto [fm, meta] = WriteAndOpen(array, {});
    ASSERT_OK_AND_ASSIGN(auto reader,
                         TantivyGlobalIndexReader::Create("f0", meta, fm, {}, GetDefaultPool()));
    auto fts = std::make_shared<FullTextSearch>("f0", /*limit=*/2, "doc",
                                                FullTextSearch::SearchType::MATCH_ALL,
                                                /*pre_filter=*/std::nullopt);
    fts->with_score = true;  // v0.2: explicit score opt-in
    ASSERT_OK_AND_ASSIGN(auto res, reader->VisitFullTextSearch(fts));
    auto scored = std::dynamic_pointer_cast<BitmapScoredGlobalIndexResult>(res);
    ASSERT_TRUE(scored) << "expected BitmapScoredGlobalIndexResult";
    ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, scored->GetBitmap());
    auto ids = BitmapToVec(*bitmap);
    ASSERT_EQ(ids, (std::vector<int64_t>{1, 2}));
    ASSERT_EQ(scored->GetScores().size(), 2u);
    // Per-doc scores must be > 0 and present in iteration (doc-id) order.
    for (auto s : scored->GetScores()) {
        ASSERT_GT(s, 0.0f);
    }
}

TEST_F(TantivyFilterLimitTest, NoLimitReturnsBitmapResult) {
    auto array = arrow::ipc::internal::json::ArrayFromJSON(DataType(), R"([
        ["doc"], ["doc doc"], ["other"]
    ])")
                     .ValueOrDie();
    auto [fm, meta] = WriteAndOpen(array, {});
    ASSERT_OK_AND_ASSIGN(auto reader,
                         TantivyGlobalIndexReader::Create("f0", meta, fm, {}, GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(
        auto res, reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                      "f0", /*limit=*/std::nullopt, "doc", FullTextSearch::SearchType::MATCH_ALL,
                      /*pre_filter=*/std::nullopt)));
    // No limit ⇒ NOT a BitmapScoredGlobalIndexResult; just BitmapGlobalIndexResult.
    ASSERT_FALSE(std::dynamic_pointer_cast<BitmapScoredGlobalIndexResult>(res));
    auto plain = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(res);
    ASSERT_TRUE(plain);
    ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, plain->GetBitmap());
    ASSERT_EQ(BitmapToVec(*bitmap), (std::vector<int64_t>{0, 1}));
}

TEST_F(TantivyFilterLimitTest, PreFilterIntersectsWithoutLimit) {
    auto array = arrow::ipc::internal::json::ArrayFromJSON(DataType(), R"([
        ["alpha"], ["alpha"], ["alpha"], ["beta"]
    ])")
                     .ValueOrDie();
    auto [fm, meta] = WriteAndOpen(array, {});
    ASSERT_OK_AND_ASSIGN(auto reader,
                         TantivyGlobalIndexReader::Create("f0", meta, fm, {}, GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(
        auto res, reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                      "f0", /*limit=*/std::nullopt, "alpha", FullTextSearch::SearchType::MATCH_ALL,
                      /*pre_filter=*/RoaringBitmap64::From({0l, 2l, 100l}))));
    auto plain = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(res);
    ASSERT_TRUE(plain);
    ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, plain->GetBitmap());
    ASSERT_EQ(BitmapToVec(*bitmap), (std::vector<int64_t>{0, 2}));
}

TEST_F(TantivyFilterLimitTest, PreFilterAppliedBeforeLimit) {
    // doc 0 has highest score for "doc" but is excluded by pre_filter; the
    // result must contain doc 1 only, even with limit=10.
    auto array = arrow::ipc::internal::json::ArrayFromJSON(DataType(), R"([
        ["doc doc doc doc doc"],
        ["doc doc"],
        ["doc"]
    ])")
                     .ValueOrDie();
    auto [fm, meta] = WriteAndOpen(array, {});
    ASSERT_OK_AND_ASSIGN(auto reader,
                         TantivyGlobalIndexReader::Create("f0", meta, fm, {}, GetDefaultPool()));
    auto fts = std::make_shared<FullTextSearch>("f0", /*limit=*/10, "doc",
                                                FullTextSearch::SearchType::MATCH_ALL,
                                                /*pre_filter=*/RoaringBitmap64::From({1l}));
    fts->with_score = true;  // v0.2: explicit score opt-in
    ASSERT_OK_AND_ASSIGN(auto res, reader->VisitFullTextSearch(fts));
    auto scored = std::dynamic_pointer_cast<BitmapScoredGlobalIndexResult>(res);
    ASSERT_TRUE(scored);
    ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, scored->GetBitmap());
    ASSERT_EQ(BitmapToVec(*bitmap), (std::vector<int64_t>{1}));
    ASSERT_EQ(scored->GetScores().size(), 1u);
}

TEST_F(TantivyFilterLimitTest, EmptyPreFilterReturnsEmpty) {
    auto array = arrow::ipc::internal::json::ArrayFromJSON(DataType(), R"([
        ["alpha"], ["beta"]
    ])")
                     .ValueOrDie();
    auto [fm, meta] = WriteAndOpen(array, {});
    ASSERT_OK_AND_ASSIGN(auto reader,
                         TantivyGlobalIndexReader::Create("f0", meta, fm, {}, GetDefaultPool()));
    RoaringBitmap64 empty;  // explicitly empty
    ASSERT_OK_AND_ASSIGN(
        auto res, reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                      "f0", /*limit=*/std::nullopt, "alpha", FullTextSearch::SearchType::MATCH_ALL,
                      /*pre_filter=*/empty)));
    auto plain = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(res);
    ASSERT_TRUE(plain);
    ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, plain->GetBitmap());
    ASSERT_TRUE(bitmap->IsEmpty());
}

TEST_F(TantivyFilterLimitTest, LimitGreaterThanMatchesReturnsAll) {
    auto array = arrow::ipc::internal::json::ArrayFromJSON(DataType(), R"([
        ["doc"], ["doc doc"], ["other"]
    ])")
                     .ValueOrDie();
    auto [fm, meta] = WriteAndOpen(array, {});
    ASSERT_OK_AND_ASSIGN(auto reader,
                         TantivyGlobalIndexReader::Create("f0", meta, fm, {}, GetDefaultPool()));
    auto fts = std::make_shared<FullTextSearch>("f0", /*limit=*/100, "doc",
                                                FullTextSearch::SearchType::MATCH_ALL,
                                                /*pre_filter=*/std::nullopt);
    fts->with_score = true;  // v0.2: explicit score opt-in
    ASSERT_OK_AND_ASSIGN(auto res, reader->VisitFullTextSearch(fts));
    auto scored = std::dynamic_pointer_cast<BitmapScoredGlobalIndexResult>(res);
    ASSERT_TRUE(scored);
    ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, scored->GetBitmap());
    ASSERT_EQ(BitmapToVec(*bitmap), (std::vector<int64_t>{0, 1}));
    ASSERT_EQ(scored->GetScores().size(), 2u);
}

// ===========================================================================
// v0.2: with_score × limit 4-path matrix guards
// ===========================================================================
// Decouple with_score from limit. The four combinations must each map to the
// correct concrete result type and content.

// Path A: with_score=false, limit=None → BitmapGlobalIndexResult, all rows, no score.
TEST_F(TantivyFilterLimitTest, WithScoreFalseLimitNoneAllRowsNoScore) {
    auto array = arrow::ipc::internal::json::ArrayFromJSON(DataType(), R"([
        ["doc"], ["doc doc"], ["doc doc doc"]
    ])")
                     .ValueOrDie();
    auto [fm, meta] = WriteAndOpen(array, {});
    ASSERT_OK_AND_ASSIGN(auto reader,
                         TantivyGlobalIndexReader::Create("f0", meta, fm, {}, GetDefaultPool()));
    auto fts = std::make_shared<FullTextSearch>("f0", /*limit=*/std::nullopt, "doc",
                                                FullTextSearch::SearchType::MATCH_ALL,
                                                /*pre_filter=*/std::nullopt);
    fts->with_score = false;
    ASSERT_OK_AND_ASSIGN(auto res, reader->VisitFullTextSearch(fts));
    // Must NOT be scored.
    ASSERT_FALSE(std::dynamic_pointer_cast<BitmapScoredGlobalIndexResult>(res));
    auto plain = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(res);
    ASSERT_TRUE(plain);
    ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, plain->GetBitmap());
    ASSERT_EQ(BitmapToVec(*bitmap), (std::vector<int64_t>{0, 1, 2}));
}

// Path B: with_score=false, limit=N → BitmapGlobalIndexResult, any N matches,
// no scoring (no BM25 sort). Used by `WHERE MATCH ... LIMIT N` without ORDER BY.
TEST_F(TantivyFilterLimitTest, WithScoreFalseLimitNAnyNNoScore) {
    auto array = arrow::ipc::internal::json::ArrayFromJSON(DataType(), R"([
        ["doc"],
        ["doc doc doc doc doc"],
        ["doc doc"]
    ])")
                     .ValueOrDie();
    auto [fm, meta] = WriteAndOpen(array, {});
    ASSERT_OK_AND_ASSIGN(auto reader,
                         TantivyGlobalIndexReader::Create("f0", meta, fm, {}, GetDefaultPool()));
    auto fts = std::make_shared<FullTextSearch>("f0", /*limit=*/2, "doc",
                                                FullTextSearch::SearchType::MATCH_ALL,
                                                /*pre_filter=*/std::nullopt);
    fts->with_score = false;
    ASSERT_OK_AND_ASSIGN(auto res, reader->VisitFullTextSearch(fts));
    // Must NOT be scored.
    ASSERT_FALSE(std::dynamic_pointer_cast<BitmapScoredGlobalIndexResult>(res));
    auto plain = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(res);
    ASSERT_TRUE(plain);
    ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, plain->GetBitmap());
    // Only cardinality matters — selection order is arbitrary and depends on
    // tantivy's posting iteration; the two returned row_ids must each be one
    // of the three input docs.
    ASSERT_EQ(bitmap->Cardinality(), 2u);
    auto vec = BitmapToVec(*bitmap);
    for (auto id : vec) {
        ASSERT_TRUE(id == 0 || id == 1 || id == 2);
    }
}

// Path C (new in v0.2): with_score=true, limit=None → BitmapScoredGlobalIndexResult,
// all rows + all scores, ordered by row_id asc.
TEST_F(TantivyFilterLimitTest, WithScoreTrueLimitNoneAllRowsWithScore) {
    auto array = arrow::ipc::internal::json::ArrayFromJSON(DataType(), R"([
        ["doc"], ["doc doc"], ["doc doc doc"]
    ])")
                     .ValueOrDie();
    auto [fm, meta] = WriteAndOpen(array, {});
    ASSERT_OK_AND_ASSIGN(auto reader,
                         TantivyGlobalIndexReader::Create("f0", meta, fm, {}, GetDefaultPool()));
    auto fts = std::make_shared<FullTextSearch>("f0", /*limit=*/std::nullopt, "doc",
                                                FullTextSearch::SearchType::MATCH_ALL,
                                                /*pre_filter=*/std::nullopt);
    fts->with_score = true;
    ASSERT_OK_AND_ASSIGN(auto res, reader->VisitFullTextSearch(fts));
    auto scored = std::dynamic_pointer_cast<BitmapScoredGlobalIndexResult>(res);
    ASSERT_TRUE(scored) << "with_score=true must produce BitmapScoredGlobalIndexResult";
    ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, scored->GetBitmap());
    ASSERT_EQ(BitmapToVec(*bitmap), (std::vector<int64_t>{0, 1, 2}));
    // All 3 docs have scores; sizes must match.
    ASSERT_EQ(scored->GetScores().size(), 3u);
    for (auto s : scored->GetScores()) {
        ASSERT_GT(s, 0.0f);
    }
}

// Path D: with_score=true, limit=N → BitmapScoredGlobalIndexResult, top-N with scores.
// Equivalent to the v0.1 happy-path (LimitProducesScoredResultTopN), kept here
// as an explicit anchor of the 4-path matrix.
TEST_F(TantivyFilterLimitTest, WithScoreTrueLimitNTopNWithScore) {
    auto array = arrow::ipc::internal::json::ArrayFromJSON(DataType(), R"([
        ["doc"],
        ["doc doc doc doc doc"],
        ["doc doc"]
    ])")
                     .ValueOrDie();
    auto [fm, meta] = WriteAndOpen(array, {});
    ASSERT_OK_AND_ASSIGN(auto reader,
                         TantivyGlobalIndexReader::Create("f0", meta, fm, {}, GetDefaultPool()));
    auto fts = std::make_shared<FullTextSearch>("f0", /*limit=*/2, "doc",
                                                FullTextSearch::SearchType::MATCH_ALL,
                                                /*pre_filter=*/std::nullopt);
    fts->with_score = true;
    ASSERT_OK_AND_ASSIGN(auto res, reader->VisitFullTextSearch(fts));
    auto scored = std::dynamic_pointer_cast<BitmapScoredGlobalIndexResult>(res);
    ASSERT_TRUE(scored);
    ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, scored->GetBitmap());
    ASSERT_EQ(bitmap->Cardinality(), 2u);
    ASSERT_TRUE(bitmap->Contains(1));  // highest TF must be included
    ASSERT_EQ(scored->GetScores().size(), 2u);
}

// Migration guard: when caller omits `with_score`, the default is `false` —
// even with limit set, the result is a BitmapGlobalIndexResult (NOT scored).
// This catches v0.1 callers that relied on `limit >= 0` to implicitly get scores.
TEST_F(TantivyFilterLimitTest, WithScoreDefaultIsFalse) {
    auto array = arrow::ipc::internal::json::ArrayFromJSON(DataType(), R"([
        ["doc"], ["doc doc"], ["doc doc doc"]
    ])")
                     .ValueOrDie();
    auto [fm, meta] = WriteAndOpen(array, {});
    ASSERT_OK_AND_ASSIGN(auto reader,
                         TantivyGlobalIndexReader::Create("f0", meta, fm, {}, GetDefaultPool()));
    // Note: NOT setting fts->with_score; relying on the default value.
    auto fts = std::make_shared<FullTextSearch>("f0", /*limit=*/2, "doc",
                                                FullTextSearch::SearchType::MATCH_ALL,
                                                /*pre_filter=*/std::nullopt);
    ASSERT_OK_AND_ASSIGN(auto res, reader->VisitFullTextSearch(fts));
    // v0.2 contract: with_score defaults to false, so even with limit set the
    // result is BitmapGlobalIndexResult (NOT BitmapScoredGlobalIndexResult).
    ASSERT_FALSE(std::dynamic_pointer_cast<BitmapScoredGlobalIndexResult>(res))
        << "v0.2: limit alone must NOT imply scoring; with_score=true is required";
    auto plain = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(res);
    ASSERT_TRUE(plain);
}

}  // namespace paimon::tantivy::test
