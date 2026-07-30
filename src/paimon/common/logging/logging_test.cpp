/*
 * Copyright 2025-present Alibaba Inc.
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
#include "paimon/logging.h"

#include <atomic>
#include <cstdarg>
#include <future>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "glog/log_severity.h"
#include "glog/logging.h"
#include "glog/raw_logging.h"
#include "paimon/common/executor/future.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/executor.h"
#include "paimon/fs/file_system.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {
namespace {

std::atomic<int> g_creator_calls{0};

// A Logger that forwards to glog exactly like the built-in adaptor. It is used to
// restore behavior-preserving logging after exercising the custom-creator path,
// because the registry cannot be reset to "unset" through the public API.
class GlogForwardingLogger : public Logger {
 public:
    void LogV(PaimonLogLevel level, const char* fname, int lineno, const char* /*function*/,
              const char* fmt, ...) override {
        va_list args;
        va_start(args, fmt);
        google::RawLog__(ToGlog(level), fname, lineno, fmt, args);
        va_end(args);
    }

    bool IsLevelEnabled(PaimonLogLevel /*level*/) const override {
        return true;
    }

 private:
    static google::LogSeverity ToGlog(PaimonLogLevel level) {
        switch (level) {
            case PAIMON_LOG_LEVEL_WARN:
                return google::GLOG_WARNING;
            case PAIMON_LOG_LEVEL_ERROR:
                return google::GLOG_ERROR;
            default:
                return google::GLOG_INFO;
        }
    }
};

}  // namespace
TEST(LoggerTest, TestMultiThreadGetLogger) {
    ASSERT_OK_AND_ASSIGN(auto executor, CreateDefaultExecutor(/*thread_count=*/4));
    auto get_logger = []() {
        auto logger = Logger::GetLogger("my_log");
        ASSERT_TRUE(logger);
    };

    std::vector<std::future<void>> futures;
    for (int32_t i = 0; i < 1000; ++i) {
        futures.push_back(Via(executor.get(), get_logger));
    }
    Wait(futures);
}

TEST(LoggerTest, TestLogAllSeverities) {
    // The default logger routes every PaimonLogLevel through the severity mapping.
    auto logger = Logger::GetLogger("severity_test");
    ASSERT_TRUE(logger);

    logger->LogV(PAIMON_LOG_LEVEL_DEBUG, __FILE__, __LINE__, __FUNCTION__, "debug severity");
    logger->LogV(PAIMON_LOG_LEVEL_INFO, __FILE__, __LINE__, __FUNCTION__, "info severity");
    logger->LogV(PAIMON_LOG_LEVEL_WARN, __FILE__, __LINE__, __FUNCTION__, "warn severity");
    logger->LogV(PAIMON_LOG_LEVEL_ERROR, __FILE__, __LINE__, __FUNCTION__, "error severity");
    // NONE and MAX fall into the default branch of the mapping.
    logger->LogV(PAIMON_LOG_LEVEL_NONE, __FILE__, __LINE__, __FUNCTION__, "none severity");
    logger->LogV(PAIMON_LOG_LEVEL_MAX, __FILE__, __LINE__, __FUNCTION__, "max severity");
}

// Demonstrates that glog's normal LOG() path actually writes a log file to disk.
// Note: Paimon's Logger/GlogAdaptor uses google::RawLog__, which only goes to
// stderr and never touches disk, so this test drives glog's file sink directly.
TEST(LoggerTest, TestGlogWritesLogFileToDisk) {
    // Save the global glog flags we are about to change so other tests are unaffected.
    const bool prev_logtostderr = FLAGS_logtostderr;
    const bool prev_timestamp_in_name = FLAGS_timestamp_in_logfile_name;
    const int32_t prev_minloglevel = FLAGS_minloglevel;

    // A unique, empty directory so the only file inside is the one glog creates for us.
    // The directory (and everything glog wrote into it) is removed on destruction.
    auto tmp_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(tmp_dir);
    std::shared_ptr<FileSystem> fs = tmp_dir->GetFileSystem();
    const std::string base = tmp_dir->Str() + "/paimon_demo";

    // Restore the process-wide glog state on every exit path, including an early return from
    // a failed ASSERT_*. Declared after tmp_dir so that it runs first and detaches the INFO
    // sink before the directory is deleted.
    ScopeGuard restore_glog_state([&]() {
        google::SetLogDestination(google::GLOG_INFO, "");
        FLAGS_logtostderr = prev_logtostderr;
        FLAGS_timestamp_in_logfile_name = prev_timestamp_in_name;
        FLAGS_minloglevel = prev_minloglevel;
    });

    FLAGS_logtostderr = false;                // must be false, otherwise glog skips the file sink
    FLAGS_timestamp_in_logfile_name = false;  // deterministic file name (no time/pid suffix)
    FLAGS_minloglevel = google::GLOG_INFO;    // do not filter out INFO

    if (!google::IsGoogleLoggingInitialized()) {
        google::InitGoogleLogging("paimon-log-disk-test");
    }
    // Force INFO logs to a file under our unique directory. Disable the
    // "<program>.INFO" symlink glog normally creates next to the log file: it would
    // dangle during recursive deletion and break UniqueTestDirectory cleanup.
    google::SetLogDestination(google::GLOG_INFO, base.c_str());
    google::SetLogSymlink(google::GLOG_INFO, "");

    const std::string token = "PAIMON_DISK_LOG_DEMO_" + std::to_string(RandomNumber(0, 1'000'000));
    LOG(INFO) << "hello from disk logging, token=" << token;
    google::FlushLogFiles(google::GLOG_INFO);

    // Collect the content of whatever file glog created in our directory.
    std::string on_disk_path;
    std::string content;
    std::vector<std::unique_ptr<BasicFileStatus>> entries;
    ASSERT_OK(fs->ListDir(tmp_dir->Str(), &entries));
    for (const auto& entry : entries) {
        if (entry->IsDir()) {
            continue;
        }
        std::string file_content;
        ASSERT_OK(fs->ReadFile(entry->GetPath(), &file_content));
        if (file_content.find(token) != std::string::npos) {
            on_disk_path = entry->GetPath();
            content = std::move(file_content);
            break;
        }
    }

    // Show the real on-disk file and its raw content so it can be eyeballed in test output.
    std::cout << "\n===== on-disk glog file: " << on_disk_path << " =====\n"
              << content << "===== end of file =====\n";

    ASSERT_FALSE(on_disk_path.empty())
        << "no glog file containing the token was written to " << tmp_dir->Str();
    ASSERT_NE(content.find(token), std::string::npos);
}

// Keep this test last: it installs a process-wide logger creator that cannot be
// unset, so it must not run before tests that rely on the default logger.
TEST(LoggerTest, TestRegisterCustomLoggerCreator) {
    g_creator_calls.store(0);
    Logger::RegisterLogger([](const std::string& /*path*/) -> std::unique_ptr<Logger> {
        g_creator_calls.fetch_add(1);
        return std::make_unique<GlogForwardingLogger>();
    });

    auto logger = Logger::GetLogger("custom_path");
    ASSERT_TRUE(logger);
    // GetLogger must have gone through the registered creator branch.
    ASSERT_EQ(1, g_creator_calls.load());
    ASSERT_TRUE(logger->IsLevelEnabled(PAIMON_LOG_LEVEL_INFO));
}
}  // namespace paimon::test
