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
 * Stage 9 coexistence test: prove lucene-fts and tantivy-fulltext can be linked
 * + instantiated + used in the same process without state collisions, and
 * that GlobalIndexerFactory routes correctly between them via index_type.
 *
 * The two implementations are NOT cross-readable (migration plan §0
 * decision 1) — each reader only opens files written by its own writer.
 * This test does NOT attempt a tantivy reader on a lucene file or vice
 * versa; instead it verifies:
 *
 *   - both factories register without symbol clashes
 *   - both writers can produce indexes side-by-side from identical input
 *   - both readers return semantically equivalent doc id sets for queries
 *     where tokenization differences don't matter (English bag-of-words)
 *   - the two indexes coexist on disk under distinct identifiers
 *     ("lucene-fts-global-index-*" vs "tantivy-fulltext-global-index-*")
 */

#include <memory>
#include <set>
#include <vector>

#include "arrow/array.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/api.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
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
#include "paimon/global_index/lucene/lucene_defs.h"
#include "paimon/global_index/tantivy/tantivy_defs.h"
#include "paimon/predicate/full_text_search.h"
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

/// Adopt one of the two factory identifiers; everything else (paths, queries,
/// arrow plumbing) is shared.
struct ImplSpec {
    std::string factory_id;     // "lucene-fts" or "tantivy-fulltext"
    std::string file_prefix;    // "lucene-fts-global-index-" or "tantivy-fulltext-global-index-"
    std::string option_prefix;  // "lucene-fts." or "tantivy-fulltext."
};

class TantivyLuceneCoexistTest : public ::testing::Test {
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

    Result<GlobalIndexIOMeta> WriteWith(const ImplSpec& impl, const std::string& root,
                                        const std::shared_ptr<arrow::DataType>& data_type,
                                        const std::map<std::string, std::string>& options,
                                        const std::shared_ptr<arrow::Array>& array) const {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GlobalIndexer> indexer,
                               GlobalIndexerFactory::Get(impl.factory_id, options));
        if (!indexer) {
            return Status::Invalid(fmt::format("factory returned null for {}", impl.factory_id));
        }
        auto path_factory = std::make_shared<FakeIndexPathFactory>(root);
        auto file_writer = std::make_shared<GlobalIndexFileManager>(fs_, path_factory);
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<GlobalIndexWriter> w,
            indexer->CreateWriter("f0", CreateArrowSchema(data_type).get(), file_writer, pool_));
        ::ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
        std::vector<int64_t> relative_row_ids(array->length());
        for (int64_t i = 0; i < array->length(); ++i) relative_row_ids[i] = i;
        PAIMON_RETURN_NOT_OK(w->AddBatch(&c_array, std::move(relative_row_ids)));
        PAIMON_ASSIGN_OR_RAISE(auto metas, w->Finish());
        EXPECT_EQ(metas.size(), 1u);
        EXPECT_TRUE(
            StringUtils::StartsWith(PathUtil::GetName(metas[0].file_path), impl.file_prefix))
            << metas[0].file_path << " did not start with " << impl.file_prefix;
        return metas[0];
    }

    Result<std::shared_ptr<GlobalIndexReader>> OpenReader(
        const ImplSpec& impl, const std::string& root,
        const std::shared_ptr<arrow::DataType>& data_type,
        const std::map<std::string, std::string>& options, const GlobalIndexIOMeta& meta) const {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GlobalIndexer> indexer,
                               GlobalIndexerFactory::Get(impl.factory_id, options));
        auto path_factory = std::make_shared<FakeIndexPathFactory>(root);
        auto file_reader = std::make_shared<GlobalIndexFileManager>(fs_, path_factory);
        return indexer->CreateReader(CreateArrowSchema(data_type).get(), file_reader, {meta},
                                     pool_);
    }

    static std::set<int64_t> ExtractDocIds(const std::shared_ptr<GlobalIndexResult>& result) {
        const RoaringBitmap64* bitmap = nullptr;
        Result<const RoaringBitmap64*> br = Status::Invalid("no result");
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

 protected:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    std::shared_ptr<FileSystem> fs_ = std::make_shared<LocalFileSystem>();

    inline static const ImplSpec kLucene{"lucene-fts", "lucene-fts-global-index-", "lucene-fts."};
    inline static const ImplSpec kTantivy{"tantivy-fulltext", "tantivy-fulltext-global-index-",
                                          "tantivy-fulltext."};
};

}  // namespace

TEST_F(TantivyLuceneCoexistTest, BothFactoriesResolve) {
    // No options needed; just verify both factories register and dispatch.
    ASSERT_OK_AND_ASSIGN(auto lucene_indexer, GlobalIndexerFactory::Get("lucene-fts", {}));
    ASSERT_OK_AND_ASSIGN(auto tantivy_indexer, GlobalIndexerFactory::Get("tantivy-fulltext", {}));
    ASSERT_TRUE(lucene_indexer);
    ASSERT_TRUE(tantivy_indexer);
    // Sanity: factories return distinct types — different vtables → different
    // GetIndexType() once we open a reader (not testable here without an
    // index), so just check shared_ptr identity differs.
    EXPECT_NE(static_cast<void*>(lucene_indexer.get()), static_cast<void*>(tantivy_indexer.get()));
}

TEST_F(TantivyLuceneCoexistTest, SideBySideEnglishCorpusReturnsSameDocIds) {
    auto data_type = arrow::struct_({arrow::field("f0", arrow::utf8())});
    auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
        ["alpha beta gamma document"],
        ["alpha alpha document"],
        ["gamma delta epsilon"],
        ["alpha beta document document"]
    ])")
                     .ValueOrDie();

    auto lucene_root = paimon::test::UniqueTestDirectory::Create();
    auto tantivy_root = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(lucene_root && tantivy_root);

    // Lucene requires a tmp directory option; tantivy ignores unknown keys.
    std::map<std::string, std::string> lucene_options = {
        {"lucene-fts.write.tmp.directory", lucene_root->Str()}};

    // Write through BOTH factories side by side in the same process.
    ASSERT_OK_AND_ASSIGN(auto lucene_meta,
                         WriteWith(kLucene, lucene_root->Str(), data_type, lucene_options, array));
    ASSERT_OK_AND_ASSIGN(auto tantivy_meta,
                         WriteWith(kTantivy, tantivy_root->Str(), data_type, {}, array));

    ASSERT_OK_AND_ASSIGN(auto lucene_reader,
                         OpenReader(kLucene, lucene_root->Str(), data_type, {}, lucene_meta));
    ASSERT_OK_AND_ASSIGN(auto tantivy_reader,
                         OpenReader(kTantivy, tantivy_root->Str(), data_type, {}, tantivy_meta));
    EXPECT_EQ(lucene_reader->GetIndexType(), std::string("lucene-fts"));
    EXPECT_EQ(tantivy_reader->GetIndexType(), std::string("tantivy-fulltext"));

    auto run_pair = [&](const std::string& q, FullTextSearch::SearchType t) {
        auto lr = lucene_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
            "f0", /*limit=*/std::nullopt, q, t, /*pre_filter=*/std::nullopt));
        auto tr = tantivy_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
            "f0", /*limit=*/std::nullopt, q, t, /*pre_filter=*/std::nullopt));
        EXPECT_TRUE(lr.ok()) << "lucene: " << lr.status().ToString();
        EXPECT_TRUE(tr.ok()) << "tantivy: " << tr.status().ToString();
        return std::make_pair(ExtractDocIds(lr.value()), ExtractDocIds(tr.value()));
    };

    // For an English bag-of-words corpus the two implementations should agree
    // on which docs contain which terms — Lucene and tantivy both store
    // lowercased word tokens.
    {
        auto [l, t] = run_pair("document", FullTextSearch::SearchType::MATCH_ALL);
        EXPECT_EQ(l, t) << "MATCH_ALL document — lucene vs tantivy doc id set differs";
        EXPECT_EQ(l, (std::set<int64_t>{0, 1, 3}));
    }
    {
        auto [l, t] = run_pair("alpha beta", FullTextSearch::SearchType::MATCH_ALL);
        EXPECT_EQ(l, t) << "MATCH_ALL 'alpha beta' — sets differ";
        EXPECT_EQ(l, (std::set<int64_t>{0, 3}));
    }
    {
        auto [l, t] = run_pair("alpha epsilon", FullTextSearch::SearchType::MATCH_ANY);
        EXPECT_EQ(l, t) << "MATCH_ANY 'alpha epsilon' — sets differ";
        EXPECT_EQ(l, (std::set<int64_t>{0, 1, 2, 3}));
    }
    {
        auto [l, t] = run_pair("alpha beta", FullTextSearch::SearchType::PHRASE);
        EXPECT_EQ(l, t) << "PHRASE 'alpha beta' — sets differ";
        EXPECT_EQ(l, (std::set<int64_t>{0, 3}));
    }
}

TEST_F(TantivyLuceneCoexistTest, IndependentLifecycleNoStateLeakage) {
    // Build a lucene index and a tantivy index back-to-back many times in the
    // same process; if either factory leaked global state across instances
    // we'd see crashes or stale results.
    auto data_type = arrow::struct_({arrow::field("f0", arrow::utf8())});

    for (int round = 0; round < 3; ++round) {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["round payload one"],
            ["round payload two"]
        ])")
                         .ValueOrDie();
        auto lroot = paimon::test::UniqueTestDirectory::Create();
        auto troot = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(lroot && troot);

        std::map<std::string, std::string> lopt = {
            {"lucene-fts.write.tmp.directory", lroot->Str()}};
        ASSERT_OK_AND_ASSIGN(auto lm, WriteWith(kLucene, lroot->Str(), data_type, lopt, array));
        ASSERT_OK_AND_ASSIGN(auto tm, WriteWith(kTantivy, troot->Str(), data_type, {}, array));
        ASSERT_OK_AND_ASSIGN(auto lr, OpenReader(kLucene, lroot->Str(), data_type, {}, lm));
        ASSERT_OK_AND_ASSIGN(auto tr, OpenReader(kTantivy, troot->Str(), data_type, {}, tm));

        auto lq = lr->VisitFullTextSearch(std::make_shared<FullTextSearch>(
            "f0", std::nullopt, "payload", FullTextSearch::SearchType::MATCH_ALL, std::nullopt));
        auto tq = tr->VisitFullTextSearch(std::make_shared<FullTextSearch>(
            "f0", std::nullopt, "payload", FullTextSearch::SearchType::MATCH_ALL, std::nullopt));
        ASSERT_TRUE(lq.ok());
        ASSERT_TRUE(tq.ok());
        EXPECT_EQ(ExtractDocIds(lq.value()), (std::set<int64_t>{0, 1})) << "lucene round " << round;
        EXPECT_EQ(ExtractDocIds(tq.value()), (std::set<int64_t>{0, 1}))
            << "tantivy round " << round;
    }
}

}  // namespace paimon::tantivy::test
