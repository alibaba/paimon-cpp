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

#include "paimon/core/mergetree/compact/file_rewrite_compact_task.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

class SpyCompactRewriter final : public CompactRewriter {
 public:
    Result<CompactResult> Rewrite(int32_t output_level, bool drop_delete,
                                  const std::vector<std::vector<SortedRun>>& sections) override {
        output_levels.push_back(output_level);
        drop_deletes.push_back(drop_delete);

        EXPECT_EQ(sections.size(), 1);
        EXPECT_EQ(sections[0].size(), 1);
        EXPECT_EQ(sections[0][0].Files().size(), 1);

        auto input = sections[0][0].Files()[0];
        rewritten_files.push_back(input);

        PAIMON_ASSIGN_OR_RAISE(auto upgraded, input->Upgrade(output_level));
        return CompactResult({input}, {upgraded});
    }

    Result<CompactResult> Upgrade(int32_t, const std::shared_ptr<DataFileMeta>&) override {
        return Status::Invalid("Upgrade should not be called by FileRewriteCompactTask");
    }

    Status Close() override {
        return Status::OK();
    }

    std::vector<int32_t> output_levels;
    std::vector<bool> drop_deletes;
    std::vector<std::shared_ptr<DataFileMeta>> rewritten_files;
};

class FailingCompactRewriter final : public CompactRewriter {
 public:
    explicit FailingCompactRewriter(size_t fail_on_call) : fail_on_call_(fail_on_call) {}

    Result<CompactResult> Rewrite(int32_t, bool,
                                  const std::vector<std::vector<SortedRun>>& sections) override {
        EXPECT_EQ(sections.size(), 1);
        EXPECT_EQ(sections[0].size(), 1);
        EXPECT_EQ(sections[0][0].Files().size(), 1);

        rewritten_files.push_back(sections[0][0].Files()[0]);
        ++call_count_;
        if (call_count_ == fail_on_call_) {
            return Status::Invalid("injected rewrite failure");
        }

        auto input = sections[0][0].Files()[0];
        PAIMON_ASSIGN_OR_RAISE(auto upgraded, input->Upgrade(/*new_level=*/1));
        return CompactResult({input}, {upgraded});
    }

    Result<CompactResult> Upgrade(int32_t, const std::shared_ptr<DataFileMeta>&) override {
        return Status::Invalid("Upgrade should not be called by FileRewriteCompactTask");
    }

    Status Close() override {
        return Status::OK();
    }

    size_t call_count() const {
        return call_count_;
    }

    std::vector<std::shared_ptr<DataFileMeta>> rewritten_files;

 private:
    size_t fail_on_call_;
    size_t call_count_ = 0;
};

std::shared_ptr<DataFileMeta> NewAppendFile(const std::string& file_name, int64_t row_count,
                                            int64_t min_sequence_number,
                                            int64_t max_sequence_number) {
    return DataFileMeta::ForAppend(file_name, /*file_size=*/row_count, row_count,
                                   SimpleStats::EmptyStats(), min_sequence_number,
                                   max_sequence_number, /*schema_id=*/0, FileSource::Append(),
                                   std::nullopt, std::nullopt, std::nullopt, std::nullopt)
        .value();
}

}  // namespace

TEST(FileRewriteCompactTaskTest, TestRewriteEachFileAndMergeResults) {
    auto file1 = NewAppendFile("file-1", /*row_count=*/10, /*min_sequence_number=*/0,
                               /*max_sequence_number=*/9);
    auto file2 = NewAppendFile("file-2", /*row_count=*/20, /*min_sequence_number=*/10,
                               /*max_sequence_number=*/29);
    CompactUnit unit(/*output_level=*/2, {file1, file2}, /*file_rewrite=*/true);

    auto rewriter = std::make_shared<SpyCompactRewriter>();
    FileRewriteCompactTask task(rewriter, unit, /*drop_delete=*/true, /*metrics_reporter=*/nullptr);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CompactResult> result, task.Execute());

    ASSERT_EQ(rewriter->output_levels, std::vector<int32_t>({2, 2}));
    ASSERT_EQ(rewriter->drop_deletes, std::vector<bool>({true, true}));
    ASSERT_EQ(rewriter->rewritten_files.size(), 2);
    EXPECT_EQ(rewriter->rewritten_files[0], file1);
    EXPECT_EQ(rewriter->rewritten_files[1], file2);

    ASSERT_EQ(result->Before().size(), 2);
    EXPECT_EQ(result->Before()[0], file1);
    EXPECT_EQ(result->Before()[1], file2);

    ASSERT_EQ(result->After().size(), 2);
    EXPECT_EQ(result->After()[0]->file_name, file1->file_name);
    EXPECT_EQ(result->After()[1]->file_name, file2->file_name);
    EXPECT_EQ(result->After()[0]->level, 2);
    EXPECT_EQ(result->After()[1]->level, 2);
}

TEST(FileRewriteCompactTaskTest, TestStopOnRewriteFailure) {
    auto file1 = NewAppendFile("file-1", /*row_count=*/10, /*min_sequence_number=*/0,
                               /*max_sequence_number=*/9);
    auto file2 = NewAppendFile("file-2", /*row_count=*/20, /*min_sequence_number=*/10,
                               /*max_sequence_number=*/29);
    auto file3 = NewAppendFile("file-3", /*row_count=*/30, /*min_sequence_number=*/30,
                               /*max_sequence_number=*/59);
    CompactUnit unit(/*output_level=*/2, {file1, file2, file3}, /*file_rewrite=*/true);

    auto rewriter = std::make_shared<FailingCompactRewriter>(/*fail_on_call=*/2);
    FileRewriteCompactTask task(rewriter, unit, /*drop_delete=*/false,
                                /*metrics_reporter=*/nullptr);

    ASSERT_NOK_WITH_MSG(task.Execute(), "injected rewrite failure");
    ASSERT_EQ(rewriter->call_count(), 2);
    ASSERT_EQ(rewriter->rewritten_files.size(), 2);
    EXPECT_EQ(rewriter->rewritten_files[0], file1);
    EXPECT_EQ(rewriter->rewritten_files[1], file2);
}

}  // namespace paimon::test
