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

#include "paimon/logging.h"

#include <cerrno>
#include <cstdarg>
#include <mutex>
#include <optional>
#include <shared_mutex>

#include "glog/log_severity.h"
#include "glog/logging.h"
#include "glog/raw_logging.h"

namespace paimon {

static std::optional<Logger::LoggerCreator>& getLoggerCreator() {
    static std::optional<Logger::LoggerCreator> _loggerCreator;
    return _loggerCreator;
}

static std::shared_mutex& getRegistryLock() {
    static std::shared_mutex registryMutex;
    return registryMutex;
}

void Logger::RegisterLogger(LoggerCreator creator) {
    std::unique_lock<std::shared_mutex> lock(getRegistryLock());
    getLoggerCreator() = creator;
}

static google::LogSeverity ToGlogLevel(PaimonLogLevel level) {
    switch (level) {
        case PAIMON_LOG_LEVEL_DEBUG:
            return google::GLOG_INFO;
        case PAIMON_LOG_LEVEL_INFO:
            return google::GLOG_INFO;
        case PAIMON_LOG_LEVEL_WARN:
            return google::GLOG_WARNING;
        case PAIMON_LOG_LEVEL_ERROR:
            return google::GLOG_ERROR;
        case PAIMON_LOG_LEVEL_NONE:
        case PAIMON_LOG_LEVEL_MAX:
        default:
            return google::GLOG_INFO;
    }
}

class GlogAdaptor : public Logger {
 public:
    void LogV(PaimonLogLevel level, const char* fname, int lineno, const char* function,
              const char* fmt, ...) override {
        va_list args;
        va_start(args, fmt);
        google::RawLog__(ToGlogLevel(level), fname, lineno, fmt, args);
        va_end(args);
    }

    bool IsLevelEnabled(PaimonLogLevel level) const override {
        return true;
    }
};

std::unique_ptr<Logger> Logger::GetLogger(const std::string& path) {
    auto& creator = getLoggerCreator();
    if (creator) {
        std::shared_lock<std::shared_mutex> lock(getRegistryLock());
        return creator.value()(path);
    }
    std::unique_lock<std::shared_mutex> ulock(getRegistryLock());
    if (!google::IsGoogleLoggingInitialized()) {
        google::InitGoogleLogging(program_invocation_name);
    }
    return std::make_unique<GlogAdaptor>();
}

}  // namespace paimon
