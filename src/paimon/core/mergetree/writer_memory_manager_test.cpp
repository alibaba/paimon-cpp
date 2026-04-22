/*
 * Copyright 2024-present Alibaba Inc.
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

#include "paimon/core/mergetree/writer_memory_manager.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/utils/batch_writer.h"
#include "paimon/core/utils/commit_increment.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

class FakeBatchWriter : public BatchWriter {
 public:
    explicit FakeBatchWriter(std::string name, std::vector<std::string>* flush_history = nullptr)
        : name_(std::move(name)), flush_history_(flush_history) {}

    void SetMemoryUsage(uint64_t memory_usage) {
        memory_usage_ = memory_usage;
    }

    void SetFlushReductions(std::vector<uint64_t> flush_reductions) {
        flush_reductions_ = std::move(flush_reductions);
    }

    int flush_calls() const {
        return flush_calls_;
    }

    uint64_t GetMemoryUsage() const override {
        return memory_usage_;
    }

    Status FlushMemory() override {
        if (flush_history_ != nullptr) {
            flush_history_->push_back(name_);
        }

        uint64_t reduction = memory_usage_;
        if (flush_calls_ < static_cast<int>(flush_reductions_.size())) {
            reduction = flush_reductions_[flush_calls_];
        }
        reduction = std::min(reduction, memory_usage_);
        memory_usage_ -= reduction;
        ++flush_calls_;
        return Status::OK();
    }

    Status Write(std::unique_ptr<RecordBatch>&& batch) override {
        (void)batch;
        return Status::OK();
    }

    Status Compact(bool full_compaction) override {
        (void)full_compaction;
        return Status::OK();
    }

    Result<CommitIncrement> PrepareCommit(bool wait_compaction) override {
        (void)wait_compaction;
        return CommitIncrement(DataIncrement({}, {}, {}), CompactIncrement({}, {}, {}), nullptr);
    }

    Result<bool> CompactNotCompleted() override {
        return false;
    }

    Status Sync() override {
        return Status::OK();
    }

    Status Close() override {
        return Status::OK();
    }

    std::shared_ptr<Metrics> GetMetrics() const override {
        return nullptr;
    }

 private:
    std::string name_;
    std::vector<std::string>* flush_history_;
    std::vector<uint64_t> flush_reductions_;
    uint64_t memory_usage_ = 0;
    int flush_calls_ = 0;
};

}  // namespace

TEST(WriterMemoryManagerTest, DoesNotFlushWhenMemoryIsBelowLimit) {
    WriterMemoryManager manager(/*max_memory=*/100);
    FakeBatchWriter writer("writer");
    writer.SetMemoryUsage(40);

    manager.RegisterWriter(&writer);

    ASSERT_OK(manager.OnWriteCompleted(&writer));
    ASSERT_EQ(writer.flush_calls(), 0);
}

TEST(WriterMemoryManagerTest, UnregisterWriterRemovesMemoryFromLedger) {
    WriterMemoryManager manager(/*max_memory=*/100);
    FakeBatchWriter writer_a("writer_a");
    FakeBatchWriter writer_b("writer_b");
    writer_a.SetMemoryUsage(80);
    writer_b.SetMemoryUsage(30);

    manager.RegisterWriter(&writer_a);
    manager.RegisterWriter(&writer_b);
    manager.UnregisterWriter(&writer_a);

    ASSERT_OK(manager.OnWriteCompleted(&writer_b));
    ASSERT_EQ(writer_b.flush_calls(), 0);
}

TEST(WriterMemoryManagerTest, RefreshWriterMemoryUpdatesLedgerWithoutFlushing) {
    WriterMemoryManager manager(/*max_memory=*/80);
    FakeBatchWriter writer_a("writer_a");
    FakeBatchWriter writer_b("writer_b");
    writer_a.SetMemoryUsage(60);
    writer_b.SetMemoryUsage(30);

    manager.RegisterWriter(&writer_a);
    manager.RegisterWriter(&writer_b);

    writer_a.SetMemoryUsage(10);
    manager.RefreshWriterMemory(&writer_a);

    ASSERT_OK(manager.OnWriteCompleted(&writer_b));
    ASSERT_EQ(writer_a.flush_calls(), 0);
    ASSERT_EQ(writer_b.flush_calls(), 0);
}

TEST(WriterMemoryManagerTest, RegisterWriterOverridesPreviousMemory) {
    WriterMemoryManager manager(/*max_memory=*/70);
    FakeBatchWriter writer_a("writer_a");
    FakeBatchWriter writer_b("writer_b");

    writer_b.SetMemoryUsage(40);
    writer_a.SetMemoryUsage(60);
    manager.RegisterWriter(&writer_a);
    writer_a.SetMemoryUsage(20);
    manager.RegisterWriter(&writer_a);
    manager.RegisterWriter(&writer_b);

    ASSERT_OK(manager.OnWriteCompleted(&writer_b));
    ASSERT_EQ(writer_b.flush_calls(), 0);
}

TEST(WriterMemoryManagerTest, ReclaimsLargestWriterEvenWhenCallerIsDifferent) {
    WriterMemoryManager manager(/*max_memory=*/100);
    std::vector<std::string> flush_history;
    FakeBatchWriter writer_a("writer_a", &flush_history);
    FakeBatchWriter writer_b("writer_b", &flush_history);
    writer_a.SetMemoryUsage(70);
    writer_a.SetFlushReductions({70});
    writer_b.SetMemoryUsage(40);

    manager.RegisterWriter(&writer_a);
    manager.RegisterWriter(&writer_b);

    ASSERT_OK(manager.OnWriteCompleted(&writer_b));
    ASSERT_EQ(flush_history, std::vector<std::string>({"writer_a"}));
    ASSERT_EQ(writer_a.flush_calls(), 1);
    ASSERT_EQ(writer_b.flush_calls(), 0);
}

TEST(WriterMemoryManagerTest, ReclaimsCallerWhenCallerIsLargestWriter) {
    WriterMemoryManager manager(/*max_memory=*/100);
    std::vector<std::string> flush_history;
    FakeBatchWriter writer_a("writer_a", &flush_history);
    FakeBatchWriter writer_b("writer_b", &flush_history);
    writer_a.SetMemoryUsage(20);
    writer_b.SetMemoryUsage(90);
    writer_b.SetFlushReductions({90});

    manager.RegisterWriter(&writer_a);
    manager.RegisterWriter(&writer_b);

    ASSERT_OK(manager.OnWriteCompleted(&writer_b));
    ASSERT_EQ(flush_history, std::vector<std::string>({"writer_b"}));
    ASSERT_EQ(writer_a.flush_calls(), 0);
    ASSERT_EQ(writer_b.flush_calls(), 1);
}

TEST(WriterMemoryManagerTest, ContinuesReclaimingUntilBelowGlobalLimit) {
    WriterMemoryManager manager(/*max_memory=*/61);
    std::vector<std::string> flush_history;
    FakeBatchWriter writer_a("writer_a", &flush_history);
    FakeBatchWriter writer_b("writer_b", &flush_history);
    writer_a.SetMemoryUsage(90);
    writer_a.SetFlushReductions({20, 20, 20});
    writer_b.SetMemoryUsage(60);
    writer_b.SetFlushReductions({30});

    manager.RegisterWriter(&writer_a);
    manager.RegisterWriter(&writer_b);

    ASSERT_OK(manager.OnWriteCompleted(&writer_b));
    ASSERT_EQ(flush_history,
              std::vector<std::string>({"writer_a", "writer_a", "writer_b", "writer_a"}));
    ASSERT_EQ(writer_a.flush_calls(), 3);
    ASSERT_EQ(writer_b.flush_calls(), 1);
    ASSERT_EQ(writer_a.GetMemoryUsage(), 30);
    ASSERT_EQ(writer_b.GetMemoryUsage(), 30);
}

TEST(WriterMemoryManagerTest, ReturnsConfigurationErrorWhenNoWriterCanReleaseEnoughMemory) {
    WriterMemoryManager manager(/*max_memory=*/100);
    std::vector<std::string> flush_history;
    FakeBatchWriter writer_a("writer_a", &flush_history);
    FakeBatchWriter writer_b("writer_b", &flush_history);
    writer_a.SetMemoryUsage(120);
    writer_a.SetFlushReductions({0});
    writer_b.SetMemoryUsage(20);
    writer_b.SetFlushReductions({20});

    manager.RegisterWriter(&writer_a);
    manager.RegisterWriter(&writer_b);

    ASSERT_NOK_WITH_MSG(manager.OnWriteCompleted(&writer_a),
                        "single write batch exceeds global write-buffer-size limit");
    ASSERT_EQ(flush_history, std::vector<std::string>({"writer_a", "writer_b"}));
}

}  // namespace paimon::test
