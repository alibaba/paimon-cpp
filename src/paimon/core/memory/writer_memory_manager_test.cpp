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

#include "paimon/core/memory/writer_memory_manager.h"

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
    FakeBatchWriter(const std::string& name, std::vector<std::string>* flush_history)
        : name_(name), flush_history_(flush_history) {}

    void SetMemoryUsage(uint64_t memory_usage) {
        memory_usage_ = memory_usage;
    }

    void SetFlushReductions(std::vector<uint64_t> flush_reductions) {
        flush_reductions_ = std::move(flush_reductions);
    }

    uint64_t GetMemoryUsage() const override {
        return memory_usage_;
    }

    Status FlushMemory() override {
        uint64_t reduction = memory_usage_;
        if (flush_calls_ < static_cast<int32_t>(flush_reductions_.size())) {
            reduction = flush_reductions_[flush_calls_];
        }
        reduction = std::min(reduction, memory_usage_);
        memory_usage_ -= reduction;

        if (reduction > 0) {
            if (flush_history_ != nullptr) {
                flush_history_->push_back(name_);
            }
            ++flush_calls_;
        }
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
    int32_t flush_calls_ = 0;
};

}  // namespace

TEST(WriterMemoryManagerTest, DoesNotFlushWhenMemoryIsBelowLimit) {
    WriterMemoryManager manager(/*memory_limit=*/100);
    std::vector<std::string> flush_history;
    FakeBatchWriter writer("writer", &flush_history);
    manager.RegisterWriter(&writer);

    writer.SetMemoryUsage(40);
    ASSERT_OK(manager.OnWriteCompleted(&writer));
    ASSERT_TRUE(flush_history.empty());
}

TEST(WriterMemoryManagerTest, UnregisterWriterRemovesMemoryFromLedger) {
    WriterMemoryManager manager(/*memory_limit=*/100);
    std::vector<std::string> flush_history;
    FakeBatchWriter writer_a("writer_a", &flush_history);
    manager.RegisterWriter(&writer_a);
    FakeBatchWriter writer_b("writer_b", &flush_history);
    manager.RegisterWriter(&writer_b);

    writer_a.SetMemoryUsage(80);
    manager.RefreshWriterMemory(&writer_a);
    manager.UnregisterWriter(&writer_a);

    writer_b.SetMemoryUsage(30);
    ASSERT_OK(manager.OnWriteCompleted(&writer_b));
    ASSERT_TRUE(flush_history.empty());
}

TEST(WriterMemoryManagerTest, RefreshWriterMemoryUpdatesLedgerWithoutFlushing) {
    WriterMemoryManager manager(/*memory_limit=*/80);
    std::vector<std::string> flush_history;
    FakeBatchWriter writer_a("writer_a", &flush_history);
    manager.RegisterWriter(&writer_a);
    FakeBatchWriter writer_b("writer_b", &flush_history);
    manager.RegisterWriter(&writer_b);

    writer_a.SetMemoryUsage(60);
    manager.RefreshWriterMemory(&writer_a);
    writer_b.SetMemoryUsage(30);
    manager.RefreshWriterMemory(&writer_b);

    writer_a.SetMemoryUsage(10);
    ASSERT_OK(manager.OnWriteCompleted(&writer_a));
    ASSERT_TRUE(flush_history.empty());
}

TEST(WriterMemoryManagerTest, FlushWriterMemoryWithMultipleWriters) {
    WriterMemoryManager manager(/*memory_limit=*/100);
    std::vector<std::string> flush_history;
    FakeBatchWriter writer_a("writer_a", &flush_history);
    manager.RegisterWriter(&writer_a);
    FakeBatchWriter writer_b("writer_b", &flush_history);
    manager.RegisterWriter(&writer_b);
    FakeBatchWriter writer_c("writer_c", &flush_history);
    manager.RegisterWriter(&writer_c);

    writer_a.SetMemoryUsage(60);
    ASSERT_OK(manager.OnWriteCompleted(&writer_a));
    writer_b.SetMemoryUsage(30);
    ASSERT_OK(manager.OnWriteCompleted(&writer_b));
    writer_c.SetMemoryUsage(50);
    ASSERT_OK(manager.OnWriteCompleted(&writer_c));
    writer_a.SetMemoryUsage(30);
    ASSERT_OK(manager.OnWriteCompleted(&writer_a));
    writer_c.SetMemoryUsage(45);
    ASSERT_OK(manager.OnWriteCompleted(&writer_c));

    ASSERT_EQ(flush_history, std::vector<std::string>({"writer_a", "writer_c", "writer_c"}));
    ASSERT_EQ(writer_a.GetMemoryUsage(), 30);
    ASSERT_EQ(writer_b.GetMemoryUsage(), 30);
    ASSERT_EQ(writer_c.GetMemoryUsage(), 0);
}

TEST(WriterMemoryManagerTest, ReclaimsCallerWhenCallerIsLargestWriter) {
    WriterMemoryManager manager(/*memory_limit=*/100);
    std::vector<std::string> flush_history;
    FakeBatchWriter writer_a("writer_a", &flush_history);
    manager.RegisterWriter(&writer_a);
    FakeBatchWriter writer_b("writer_b", &flush_history);
    manager.RegisterWriter(&writer_b);

    writer_a.SetMemoryUsage(20);
    manager.RefreshWriterMemory(&writer_a);

    writer_b.SetMemoryUsage(90);
    ASSERT_OK(manager.OnWriteCompleted(&writer_b));

    ASSERT_EQ(flush_history, std::vector<std::string>({"writer_b"}));
    ASSERT_EQ(writer_a.GetMemoryUsage(), 20);
    ASSERT_EQ(writer_b.GetMemoryUsage(), 0);
}

TEST(WriterMemoryManagerTest, ContinuesReclaimingUntilBelowGlobalLimit) {
    WriterMemoryManager manager(/*memory_limit=*/61);
    std::vector<std::string> flush_history;
    FakeBatchWriter writer_a("writer_a", &flush_history);
    manager.RegisterWriter(&writer_a);
    FakeBatchWriter writer_b("writer_b", &flush_history);
    manager.RegisterWriter(&writer_b);

    writer_a.SetMemoryUsage(90);
    writer_a.SetFlushReductions({20, 20, 20});
    manager.RefreshWriterMemory(&writer_a);

    writer_b.SetMemoryUsage(60);
    writer_b.SetFlushReductions({30});
    ASSERT_OK(manager.OnWriteCompleted(&writer_b));

    ASSERT_EQ(flush_history,
              std::vector<std::string>({"writer_a", "writer_a", "writer_b", "writer_a"}));
    ASSERT_EQ(writer_a.GetMemoryUsage(), 30);
    ASSERT_EQ(writer_b.GetMemoryUsage(), 30);
}

TEST(WriterMemoryManagerTest, ReturnsConfigurationErrorWhenNoWriterCanReleaseEnoughMemory) {
    WriterMemoryManager manager(/*memory_limit=*/100);
    std::vector<std::string> flush_history;
    FakeBatchWriter writer_a("writer_a", &flush_history);
    manager.RegisterWriter(&writer_a);
    FakeBatchWriter writer_b("writer_b", &flush_history);
    manager.RegisterWriter(&writer_b);

    writer_b.SetMemoryUsage(20);
    manager.RefreshWriterMemory(&writer_b);

    writer_a.SetMemoryUsage(120);
    writer_a.SetFlushReductions({10, 0});
    ASSERT_NOK_WITH_MSG(
        manager.OnWriteCompleted(&writer_a),
        "Before flushing memory, writer had 110 bytes of memory allocated, After flushing memory, "
        "writer still has 110 bytes of memory allocated, this might be a bug.");
    ASSERT_EQ(flush_history, std::vector<std::string>({"writer_a"}));
}

}  // namespace paimon::test
