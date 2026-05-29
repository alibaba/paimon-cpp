/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0.
 *
 * Stage 10: equivalence + benchmark.
 *
 * EQUIVALENCE: a parametric corpus × query battery that compares lucene-fts
 * and tantivy-fulltext result *sets* (doc_id only — not score order, not score
 * values). Coverage targets:
 *   - English bag-of-words: MATCH_ALL / MATCH_ANY / PHRASE
 *   - Chinese (jieba "query" mode): MATCH_ALL / MATCH_ANY / PHRASE
 *   - Pre_filter intersection (no scoring)
 * PREFIX and WILDCARD are NOT compared as required-equal: tantivy's RegexQuery
 * walks byte-level term dictionary, lucene's PrefixQuery/WildcardQuery walks
 * its own; edge cases (empty input, anchors, multi-byte UTF-8) diverge by
 * design. Documented in docs/dev/execute.md Stage 10 decisions.
 *
 * BENCHMARK: build a 200-doc index per backend and time write + 100 queries.
 * Prints to stderr; never fails on perf — guarding against perf regressions
 * is out of scope for this stage. Numbers go in execute.md as a baseline.
 */

#include <chrono>
#include <iostream>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/api.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "gtest/gtest.h"

#include "paimon/common/utils/path_util.h"
#include "paimon/core/global_index/global_index_file_manager.h"
#include "paimon/core/index/index_path_factory.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/global_index/global_index_io_meta.h"
#include "paimon/global_index/global_index_reader.h"
#include "paimon/global_index/global_index_writer.h"
#include "paimon/global_index/global_indexer.h"
#include "paimon/global_index/global_indexer_factory.h"
#include "paimon/predicate/full_text_search.h"
#include "paimon/testing/utils/testharness.h"

#include "paimon/global_index/lucene/lucene_defs.h"
#include "paimon/global_index/tantivy/tantivy_defs.h"

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

struct ReaderPair {
    std::shared_ptr<GlobalIndexReader> lucene;
    std::shared_ptr<GlobalIndexReader> tantivy;
    std::unique_ptr<paimon::test::UniqueTestDirectory> lucene_root;
    std::unique_ptr<paimon::test::UniqueTestDirectory> tantivy_root;
};

class TantivyEquivalenceTest : public ::testing::Test {
 public:
    void SetUp() override {
        setenv(::paimon::lucene::kJiebaDictDirEnv, JIEBA_TEST_DICT_DIR, /*overwrite=*/1);
        setenv(::paimon::tantivy::kJiebaDictDirEnv, JIEBA_TEST_DICT_DIR, /*overwrite=*/1);
    }

    std::unique_ptr<::ArrowSchema> CreateArrowSchema(
        const std::shared_ptr<arrow::DataType>& data_type) const {
        auto c_schema = std::make_unique<::ArrowSchema>();
        EXPECT_TRUE(arrow::ExportType(*data_type, c_schema.get()).ok());
        return c_schema;
    }

    GlobalIndexIOMeta WriteOne(const std::string& factory_id,
                               const std::shared_ptr<arrow::DataType>& data_type,
                               const std::map<std::string, std::string>& options,
                               const std::shared_ptr<arrow::Array>& array,
                               const std::string& root) {
        auto indexer_res = GlobalIndexerFactory::Get(factory_id, options);
        EXPECT_TRUE(indexer_res.ok()) << indexer_res.status().ToString();
        // NB: std::move(result).value() picks the rvalue overload (returns T&&);
        // std::move(result.value()) would call the const T& overload first → no move.
        auto indexer = std::move(indexer_res).value();
        auto path_factory = std::make_shared<FakeIndexPathFactory>(root);
        auto file_writer = std::make_shared<GlobalIndexFileManager>(fs_, path_factory);
        auto writer_res =
            indexer->CreateWriter("f0", CreateArrowSchema(data_type).get(), file_writer, pool_);
        EXPECT_TRUE(writer_res.ok()) << writer_res.status().ToString();
        ::ArrowArray c_array;
        EXPECT_TRUE(arrow::ExportArray(*array, &c_array).ok());
        std::vector<int64_t> relative_row_ids(array->length());
        for (int64_t i = 0; i < array->length(); ++i) relative_row_ids[i] = i;
        EXPECT_TRUE(writer_res.value()->AddBatch(&c_array, std::move(relative_row_ids)).ok());
        auto metas_res = writer_res.value()->Finish();
        EXPECT_TRUE(metas_res.ok()) << metas_res.status().ToString();
        return metas_res.value()[0];
    }

    std::shared_ptr<GlobalIndexReader> OpenOne(
        const std::string& factory_id, const std::shared_ptr<arrow::DataType>& data_type,
        const std::map<std::string, std::string>& options, const GlobalIndexIOMeta& meta,
        const std::string& root) {
        auto indexer = GlobalIndexerFactory::Get(factory_id, options).value();
        auto path_factory = std::make_shared<FakeIndexPathFactory>(root);
        auto file_reader = std::make_shared<GlobalIndexFileManager>(fs_, path_factory);
        return indexer->CreateReader(CreateArrowSchema(data_type).get(), file_reader, {meta}, pool_)
            .value();
    }

    /// Build BOTH lucene + tantivy indexes for the same corpus + options.
    /// Returns an opened-reader pair plus owning UniqueTestDirectory handles.
    ReaderPair WriteAndOpenBoth(const std::shared_ptr<arrow::DataType>& data_type,
                                const std::shared_ptr<arrow::Array>& array,
                                std::map<std::string, std::string> lucene_opts,
                                const std::map<std::string, std::string>& tantivy_opts) {
        auto lroot = paimon::test::UniqueTestDirectory::Create();
        auto troot = paimon::test::UniqueTestDirectory::Create();
        EXPECT_TRUE(lroot && troot);
        // lucene requires a tmp directory option; reuse lroot if caller didn't set one.
        lucene_opts.emplace("lucene-fts.write.tmp.directory", lroot->Str());
        auto lmeta =
            WriteOne("lucene-fts", data_type, lucene_opts, array, lroot->Str());
        auto tmeta =
            WriteOne("tantivy-fulltext", data_type, tantivy_opts, array, troot->Str());
        ReaderPair p;
        p.lucene = OpenOne("lucene-fts", data_type, lucene_opts, lmeta, lroot->Str());
        p.tantivy = OpenOne("tantivy-fulltext", data_type, tantivy_opts, tmeta, troot->Str());
        p.lucene_root = std::move(lroot);
        p.tantivy_root = std::move(troot);
        return p;
    }

    static std::set<int64_t> Ids(const std::shared_ptr<GlobalIndexResult>& result) {
        const RoaringBitmap64* bitmap = nullptr;
        Result<const RoaringBitmap64*> br = Status::Invalid("none");
        if (auto scored = std::dynamic_pointer_cast<BitmapScoredGlobalIndexResult>(result)) {
            br = scored->GetBitmap();
        } else if (auto plain = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(result)) {
            br = plain->GetBitmap();
        }
        EXPECT_TRUE(br.ok()) << br.status().ToString();
        bitmap = br.value();
        std::set<int64_t> out;
        if (bitmap) {
            for (auto it = bitmap->Begin(); it != bitmap->End(); ++it) {
                out.insert(static_cast<int64_t>(*it));
            }
        }
        return out;
    }

    /// Run a single FullTextSearch through both readers, return (lucene, tantivy)
    /// doc id sets.
    std::pair<std::set<int64_t>, std::set<int64_t>> RunPair(
        const ReaderPair& p, const std::string& q, FullTextSearch::SearchType t,
        std::optional<int32_t> limit = std::nullopt,
        std::optional<RoaringBitmap64> filter = std::nullopt) {
        auto lr = p.lucene->VisitFullTextSearch(
            std::make_shared<FullTextSearch>("f0", limit, q, t, filter));
        auto tr = p.tantivy->VisitFullTextSearch(
            std::make_shared<FullTextSearch>("f0", limit, q, t, filter));
        EXPECT_TRUE(lr.ok()) << "lucene: " << lr.status().ToString();
        EXPECT_TRUE(tr.ok()) << "tantivy: " << tr.status().ToString();
        return {Ids(lr.value()), Ids(tr.value())};
    }

 protected:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    std::shared_ptr<FileSystem> fs_ = std::make_shared<LocalFileSystem>();
};

}  // namespace

TEST_F(TantivyEquivalenceTest, EnglishBagOfWordsBattery) {
    auto data_type = arrow::struct_({arrow::field("f0", arrow::utf8())});
    auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
        ["alpha beta gamma delta"],
        ["alpha alpha alpha beta"],
        ["beta gamma delta epsilon"],
        ["zeta eta theta iota"],
        ["alpha gamma epsilon iota"],
        ["lone outlier word here"],
        ["alpha beta gamma alpha beta"],
        ["delta epsilon zeta eta theta"],
        ["nothing matches this row"],
        ["alpha"]
    ])")
                     .ValueOrDie();
    auto pair = WriteAndOpenBoth(data_type, array, {}, {});

    struct Case {
        std::string query;
        FullTextSearch::SearchType type;
    };
    std::vector<Case> cases = {
        {"alpha", FullTextSearch::SearchType::MATCH_ALL},
        {"alpha", FullTextSearch::SearchType::MATCH_ANY},
        {"alpha beta", FullTextSearch::SearchType::MATCH_ALL},
        {"alpha beta", FullTextSearch::SearchType::MATCH_ANY},
        {"alpha gamma delta", FullTextSearch::SearchType::MATCH_ALL},
        {"alpha gamma delta", FullTextSearch::SearchType::MATCH_ANY},
        {"epsilon iota", FullTextSearch::SearchType::MATCH_ALL},
        {"alpha beta gamma", FullTextSearch::SearchType::PHRASE},
        {"beta gamma delta", FullTextSearch::SearchType::PHRASE},
        {"delta epsilon", FullTextSearch::SearchType::PHRASE},
    };
    for (const auto& c : cases) {
        auto [l, t] = RunPair(pair, c.query, c.type);
        EXPECT_EQ(l, t) << "diverge: query=" << c.query << " type=" << static_cast<int>(c.type);
    }
}

TEST_F(TantivyEquivalenceTest, ChineseQueryModeBattery) {
    auto data_type = arrow::struct_({arrow::field("f0", arrow::utf8())});
    auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
["智能助手 AI 模块 开发"],
["智能助手 在 Python 开发 中"],
["AI 助手 开发 框架"],
["智能 模块 技术 实现"],
["发展方向 是 智能 助手"]
    ])")
                     .ValueOrDie();
    std::map<std::string, std::string> lopts = {{"lucene-fts.jieba.tokenize-mode", "query"}};
    std::map<std::string, std::string> topts = {
        {"tantivy-fulltext.tantivy.write.tokenizer", "paimon_jieba"},
        {"tantivy-fulltext.jieba.tokenize-mode", "query"},
    };
    auto pair = WriteAndOpenBoth(data_type, array, lopts, topts);

    struct Case {
        std::string query;
        FullTextSearch::SearchType type;
    };
    // Note: jieba is shared (same dictionary), so tokenization should agree
    // for plain Chinese text. Differences (if any) come from the lowercase /
    // stopword normalization step — tested with neutral CJK terms below.
    std::vector<Case> cases = {
        {"智能", FullTextSearch::SearchType::MATCH_ALL},
        {"智能 助手", FullTextSearch::SearchType::MATCH_ALL},
        {"模块", FullTextSearch::SearchType::MATCH_ANY},
        {"发展方向", FullTextSearch::SearchType::PHRASE},
    };
    for (const auto& c : cases) {
        auto [l, t] = RunPair(pair, c.query, c.type);
        EXPECT_EQ(l, t) << "diverge: query=" << c.query << " type=" << static_cast<int>(c.type);
    }
}

TEST_F(TantivyEquivalenceTest, PreFilterIntersectionEquivalent) {
    auto data_type = arrow::struct_({arrow::field("f0", arrow::utf8())});
    auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
        ["alpha beta"],
        ["alpha gamma"],
        ["alpha delta"],
        ["beta gamma"],
        ["beta delta"]
    ])")
                     .ValueOrDie();
    auto pair = WriteAndOpenBoth(data_type, array, {}, {});

    auto pf = RoaringBitmap64::From({0l, 2l, 4l});
    {
        auto [l, t] = RunPair(pair, "alpha", FullTextSearch::SearchType::MATCH_ALL,
                              std::nullopt, pf);
        EXPECT_EQ(l, t);
        EXPECT_EQ(l, (std::set<int64_t>{0, 2}));
    }
    {
        auto [l, t] = RunPair(pair, "beta gamma", FullTextSearch::SearchType::MATCH_ANY,
                              std::nullopt, pf);
        EXPECT_EQ(l, t);
    }
    {
        auto empty = RoaringBitmap64();
        auto [l, t] = RunPair(pair, "alpha", FullTextSearch::SearchType::MATCH_ALL,
                              std::nullopt, empty);
        EXPECT_EQ(l, t);
        EXPECT_TRUE(l.empty());
    }
}

TEST_F(TantivyEquivalenceTest, BenchmarkBuildAndQuery) {
    // Build a synthetic 200-doc corpus and time write + 100 random queries.
    // This is a reportable baseline, NOT a perf gate — assertions only check
    // semantic correctness (each query returns >= 0 docs without erroring).
    constexpr int kDocCount = 200;
    constexpr int kQueryCount = 100;
    std::vector<std::string> vocab = {"alpha", "beta",  "gamma", "delta", "epsilon",
                                      "zeta",  "eta",   "theta", "iota",  "kappa",
                                      "lambda", "mu",   "nu",    "xi",    "omicron"};
    std::mt19937 rng(0xC0DE);
    std::uniform_int_distribution<size_t> word_pick(0, vocab.size() - 1);
    std::uniform_int_distribution<int> word_count(3, 12);

    // Build the corpus as a JSON Arrow array.
    std::string json = "[";
    for (int i = 0; i < kDocCount; ++i) {
        json += "[\"";
        int n = word_count(rng);
        for (int w = 0; w < n; ++w) {
            if (w > 0) json += ' ';
            json += vocab[word_pick(rng)];
        }
        json += "\"]";
        if (i + 1 < kDocCount) json += ",";
    }
    json += "]";

    auto data_type = arrow::struct_({arrow::field("f0", arrow::utf8())});
    auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, json).ValueOrDie();

    auto time_ms = [](auto&& fn) {
        auto t0 = std::chrono::steady_clock::now();
        fn();
        auto t1 = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    };

    // -------- Lucene: write + open + queries --------
    auto lroot = paimon::test::UniqueTestDirectory::Create();
    std::map<std::string, std::string> lopt = {
        {"lucene-fts.write.tmp.directory", lroot->Str()}};
    GlobalIndexIOMeta lmeta{"", 0, nullptr};
    auto lwrite_ms =
        time_ms([&] { lmeta = WriteOne("lucene-fts", data_type, lopt, array, lroot->Str()); });
    auto lreader = OpenOne("lucene-fts", data_type, lopt, lmeta, lroot->Str());

    auto lquery_ms = time_ms([&] {
        for (int i = 0; i < kQueryCount; ++i) {
            const std::string& w = vocab[word_pick(rng)];
            auto r = lreader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                "f0", std::nullopt, w, FullTextSearch::SearchType::MATCH_ALL, std::nullopt));
            EXPECT_TRUE(r.ok());
        }
    });

    // -------- Tantivy: write + open + queries --------
    auto troot = paimon::test::UniqueTestDirectory::Create();
    GlobalIndexIOMeta tmeta{"", 0, nullptr};
    auto twrite_ms =
        time_ms([&] { tmeta = WriteOne("tantivy-fulltext", data_type, {}, array, troot->Str()); });
    auto treader = OpenOne("tantivy-fulltext", data_type, {}, tmeta, troot->Str());

    auto tquery_ms = time_ms([&] {
        for (int i = 0; i < kQueryCount; ++i) {
            const std::string& w = vocab[word_pick(rng)];
            auto r = treader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                "f0", std::nullopt, w, FullTextSearch::SearchType::MATCH_ALL, std::nullopt));
            EXPECT_TRUE(r.ok());
        }
    });

    std::cerr << fmt::format(
        "[STAGE10-BENCH docs={} queries={}] lucene_write={}ms lucene_query={}ms"
        " tantivy_write={}ms tantivy_query={}ms file_size_lucene={} file_size_tantivy={}\n",
        kDocCount, kQueryCount, lwrite_ms, lquery_ms, twrite_ms, tquery_ms, lmeta.file_size,
        tmeta.file_size);
    SUCCEED() << "benchmark prints to stderr";
}

}  // namespace paimon::tantivy::test
