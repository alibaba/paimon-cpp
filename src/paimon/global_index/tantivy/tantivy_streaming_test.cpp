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
 * Streaming test: Callback Directory + streaming writer end-to-end.
 *
 * Coverage:
 *   1. ParseArchiveHeaderFuzz — malformed header bytes rejected cleanly
 *   2. ConcurrentQueryOnSameReader — 4 threads query same reader, serialized
 *      by Rust stream_mutex, results consistent, no race
 *   3. ConcurrentCreateAndDropReaders — 10 threads each open/query/close their
 *      own reader on the same archive; no leaks, release exactly-once per reader
 *
 * We don't duplicate tests already covered by the Rust unit tests
 * (callback_directory::tests::* for Directory semantics, writer::tests::
 * streaming_chunk_size_bounded_by_buffer for the streaming buffer guarantee).
 */

#include <atomic>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include "arrow/array.h"
#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/global_index/global_index_file_manager.h"
#include "paimon/core/index/index_path_factory.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/global_index/tantivy/tantivy_archive_layout.h"
#include "paimon/global_index/tantivy/tantivy_defs.h"
#include "paimon/global_index/tantivy/tantivy_global_index.h"
#include "paimon/global_index/tantivy/tantivy_global_index_reader.h"
#include "paimon/io/byte_array_input_stream.h"
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

/// Helper: build an archive with `n` documents, return the GlobalIndexIOMeta.
/// Holds the tmp dir alive (via `holder`) so it's cleaned up when the
/// WriteResult goes out of scope.
struct WriteResult {
    std::unique_ptr<paimon::test::UniqueTestDirectory> holder;
    std::string root_dir;
    GlobalIndexIOMeta meta;
};

class StreamingTestFixture : public ::testing::Test {
 public:
    WriteResult BuildArchive(std::size_t n_docs,
                             const std::string& text_template = "apple banana cherry {}") {
        auto root_dir = paimon::test::UniqueTestDirectory::Create();
        EXPECT_TRUE(root_dir);
        std::string root = root_dir->Str();

        // Build arrow StringArray
        arrow::StringBuilder sb;
        for (std::size_t i = 0; i < n_docs; ++i) {
            char buf[128];
            std::snprintf(buf, sizeof(buf), text_template.c_str(), i);
            EXPECT_TRUE(sb.Append(buf).ok());
        }
        auto text_array = sb.Finish().ValueOrDie();
        auto struct_array =
            arrow::StructArray::Make({text_array}, {arrow::field("f0", arrow::utf8())})
                .ValueOrDie();

        std::map<std::string, std::string> options;
        auto data_type = arrow::struct_({arrow::field("f0", arrow::utf8())});
        auto c_schema = std::make_unique<::ArrowSchema>();
        EXPECT_TRUE(arrow::ExportType(*data_type, c_schema.get()).ok());
        auto global_index = std::make_shared<TantivyGlobalIndex>(options);
        auto path_factory = std::make_shared<FakeIndexPathFactory>(root);
        auto file_writer = std::make_shared<GlobalIndexFileManager>(fs_, path_factory);
        EXPECT_OK_AND_ASSIGN(auto w,
                             global_index->CreateWriter("f0", c_schema.get(), file_writer, pool_));
        ::ArrowArray c_array;
        EXPECT_TRUE(arrow::ExportArray(*struct_array, &c_array).ok());
        std::vector<int64_t> relative_row_ids(struct_array->length());
        for (int64_t i = 0; i < struct_array->length(); ++i) {
            relative_row_ids[i] = i;
        }
        EXPECT_OK(w->AddBatch(&c_array, std::move(relative_row_ids)));
        EXPECT_OK_AND_ASSIGN(auto metas, w->Finish());
        EXPECT_EQ(metas.size(), 1u);

        // Move root_dir into the result — it stays alive as long as the
        // caller holds WriteResult; cleaned up when TEST_F scope exits.
        return WriteResult{std::move(root_dir), std::move(root), metas[0]};
    }

    std::shared_ptr<GlobalIndexReader> OpenReader(const std::string& root,
                                                  const GlobalIndexIOMeta& meta) {
        std::map<std::string, std::string> options;
        auto data_type = arrow::struct_({arrow::field("f0", arrow::utf8())});
        auto c_schema = std::make_unique<::ArrowSchema>();
        EXPECT_TRUE(arrow::ExportType(*data_type, c_schema.get()).ok());
        auto global_index = std::make_shared<TantivyGlobalIndex>(options);
        auto path_factory = std::make_shared<FakeIndexPathFactory>(root);
        auto file_reader = std::make_shared<GlobalIndexFileManager>(fs_, path_factory);
        EXPECT_OK_AND_ASSIGN(
            auto reader, global_index->CreateReader(c_schema.get(), file_reader, {meta}, pool_));
        return reader;
    }

    std::shared_ptr<FullTextSearch> BuildMatchAll(const std::string& query) {
        return std::make_shared<FullTextSearch>(
            /*_field_name=*/"f0",
            /*_limit=*/std::optional<int32_t>{},
            /*_query=*/query,
            /*_search_type=*/FullTextSearch::SearchType::MATCH_ALL,
            /*_pre_filter=*/std::optional<RoaringBitmap64>{});
    }

 protected:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    std::shared_ptr<FileSystem> fs_ = std::make_shared<LocalFileSystem>();
};

// =========================================================================
// 1. ParseArchiveHeader fuzz
// =========================================================================

TEST(ParseArchiveHeaderFuzz, TruncatedHeader) {
    // Fewer than 4 bytes → DataInputStream::ReadValue<int32_t> fails
    std::string bytes = "\x00\x00";
    ByteArrayInputStream in(bytes.data(), bytes.size());
    ASSERT_NOK(ArchiveLayout::Parse(&in)) << "expected failure on truncated header";
}

TEST(ParseArchiveHeaderFuzz, NegativeFileCount) {
    // BE int32 -1 = 0xFFFFFFFF
    char bytes[4] = {static_cast<char>(0xFF), static_cast<char>(0xFF), static_cast<char>(0xFF),
                     static_cast<char>(0xFF)};
    ByteArrayInputStream in(bytes, 4);
    ASSERT_NOK_WITH_MSG(ArchiveLayout::Parse(&in), "bad file_count");
}

TEST(ParseArchiveHeaderFuzz, NameLenOutOfRange) {
    // file_count=1, name_len=2GB (BE int32 0x7FFFFFFF)
    char bytes[8] = {0,
                     0,
                     0,
                     1,
                     static_cast<char>(0x7F),
                     static_cast<char>(0xFF),
                     static_cast<char>(0xFF),
                     static_cast<char>(0xFF)};
    ByteArrayInputStream in(bytes, 8);
    ASSERT_NOK_WITH_MSG(ArchiveLayout::Parse(&in), "bad name_len");
}

TEST(ParseArchiveHeaderFuzz, ZeroFileCountSucceeds) {
    // file_count=0 is structurally valid; caller will fail later when
    // tantivy::Index::open finds no meta.json, but parse itself OK.
    char bytes[4] = {0, 0, 0, 0};
    ByteArrayInputStream in(bytes, 4);
    ASSERT_OK_AND_ASSIGN(auto r, ArchiveLayout::Parse(&in));
    ASSERT_EQ(r.count, 0u);
}

TEST(ParseArchiveHeaderFuzz, PayloadLenNegative) {
    // file_count=1, name_len=1, name="a", data_len=-1 (BE int64 0xFFFFFFFFFFFFFFFF)
    char bytes[4 + 4 + 1 + 8] = {
        // file_count=1
        0,
        0,
        0,
        1,
        // name_len=1
        0,
        0,
        0,
        1,
        // name='a'
        'a',
        // data_len = -1 (BE int64 0xFFFFFFFFFFFFFFFF)
        static_cast<char>(0xFF),
        static_cast<char>(0xFF),
        static_cast<char>(0xFF),
        static_cast<char>(0xFF),
        static_cast<char>(0xFF),
        static_cast<char>(0xFF),
        static_cast<char>(0xFF),
        static_cast<char>(0xFF),
    };
    ByteArrayInputStream in(bytes, sizeof(bytes));
    ASSERT_NOK_WITH_MSG(ArchiveLayout::Parse(&in), "bad data_len");
}

// =========================================================================
// 2. Concurrent query on same reader
// =========================================================================

TEST_F(StreamingTestFixture, ConcurrentQueryOnSameReader) {
    // 50 docs containing "apple" in every one (all should match)
    auto wr = BuildArchive(50, "apple banana {}");
    auto reader = OpenReader(wr.root_dir, wr.meta);

    auto fts = BuildMatchAll("apple");

    // 4 threads × 20 queries each, all must return 50 rowIds
    constexpr int32_t kThreads = 4;
    constexpr int32_t kIters = 20;
    std::vector<std::thread> threads;
    std::atomic<int32_t> failures{0};
    for (int32_t t = 0; t < kThreads; ++t) {
        threads.emplace_back([&] {
            for (int32_t i = 0; i < kIters; ++i) {
                auto result = reader->VisitFullTextSearch(fts);
                if (!result.ok() || !result.value()) {
                    failures++;
                    continue;
                }
                std::shared_ptr<GlobalIndexResult> r = result.value();
                auto plain = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(r);
                if (!plain) {
                    failures++;
                    continue;
                }
                auto bres = plain->GetBitmap();
                if (!bres.ok() || bres.value() == nullptr || bres.value()->Cardinality() != 50) {
                    failures++;
                }
            }
        });
    }
    for (auto& th : threads) {
        th.join();
    }
    ASSERT_EQ(failures.load(), 0) << "concurrent queries produced inconsistent results";
}

// =========================================================================
// 3. Concurrent reader open + close
// =========================================================================

TEST_F(StreamingTestFixture, ConcurrentCreateAndDropReaders) {
    // One archive, many readers opening/closing it concurrently.
    // Validates exactly-once release (no UAF under ASAN) and open/close race safety.
    auto wr = BuildArchive(20);

    constexpr int32_t kThreads = 10;
    std::vector<std::thread> threads;
    std::atomic<int32_t> failures{0};
    for (int32_t t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            for (int32_t i = 0; i < 5; ++i) {
                auto reader = OpenReader(wr.root_dir, wr.meta);
                if (!reader) {
                    failures++;
                    continue;
                }
                auto fts = BuildMatchAll("apple");
                auto r = reader->VisitFullTextSearch(fts);
                if (!r.ok()) {
                    failures++;
                }
                // reader drops here → Rust Arc<CallbackCtx>::drop → paimon_cpp_stream_release
            }
            (void)t;
        });
    }
    for (auto& th : threads) {
        th.join();
    }
    ASSERT_EQ(failures.load(), 0);
}

}  // namespace
}  // namespace paimon::tantivy::test
