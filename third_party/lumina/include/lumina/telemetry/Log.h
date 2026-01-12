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

#pragma once
#include <atomic>
#include <chrono>
#include <cstdarg>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <mutex>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace lumina::telemetry {

// ---- Levels ----
enum class LogLevel : int { Trace = 0, Debug = 1, Info = 2, Warn = 3, Error = 4 };

// ---- Record ----
struct LogRecord {
    LogLevel level {LogLevel::Info};
    std::string module;
    std::string message;
    const char* file {""};
    int line {0};
    std::uint64_t threadId {0};
    std::chrono::system_clock::time_point ts {};
};

// ---- Sink interface ----
class LogSink
{
public:
    virtual ~LogSink() = default;
    virtual void Write(const LogRecord& rec) = 0;
};

// Default stdout sink
class StdoutSink final : public LogSink
{
public:
    void Write(const LogRecord& rec) override;
};

// ---- Logger singleton ----
class Logger
{
public:
    static Logger& Instance();

    void SetLevel(LogLevel lvl) noexcept { _level.store(lvl, std::memory_order_relaxed); }
    LogLevel Level() const noexcept { return _level.load(std::memory_order_relaxed); }

    void AddSink(std::unique_ptr<LogSink> sink);
    void ClearSinks();

    // Submit a log record (used by internal RAII)
    void Submit(LogLevel lvl, std::string_view module, std::string&& msg, const char* file, int line);

    // Check at runtime whether to log
    bool ShouldLog(LogLevel lvl) const noexcept { return static_cast<int>(lvl) >= static_cast<int>(Level()); }

private:
    Logger(); // Defined in .cpp
    std::vector<std::unique_ptr<LogSink>> _sinks;
    mutable std::mutex _mu;
    std::atomic<LogLevel> _level {LogLevel::Info};
};

// ---- RAII line builder: supports << style ----
class LogLine
{
public:
    LogLine(LogLevel lvl, std::string_view module, const char* file, int line)
        : _level(lvl)
        , _module(module)
        , _file(file)
        , _line(line)
    {
    }

    ~LogLine()
    {
        if (Logger::Instance().ShouldLog(_level)) {
            Logger::Instance().Submit(_level, _module, _oss.str(), _file, _line);
        }
    }

    template <class T>
    LogLine& operator<<(const T& v)
    {
        _oss << v;
        return *this;
    }

private:
    LogLevel _level;
    std::string _module;
    const char* _file;
    int _line;
    std::ostringstream _oss;
};

// ---- Compile-time default level (override via -D) ----
#ifndef LUMINA_LOG_COMPILE_LEVEL
#define LUMINA_LOG_COMPILE_LEVEL ::lumina::telemetry::LogLevel::Info
#endif

namespace detail {

constexpr std::string_view Basename(std::string_view path)
{
    std::size_t pos = path.find_last_of("/\\");
    if (pos == std::string_view::npos) {
        return path;
    }
    return path.substr(pos + 1);
}

// Printf-style formatting helper
#ifdef __GNUC__
inline std::string FormatString(const char* fmt, ...) __attribute__((format(printf, 1, 2)));
#endif

inline std::string FormatString(const char* fmt, ...)
{
    va_list args;
    va_start(args, fmt);

    // Calculate required buffer size
    va_list argsCopy;
    va_copy(argsCopy, args);
    int size = std::vsnprintf(nullptr, 0, fmt, argsCopy);
    va_end(argsCopy);

    if (size < 0) {
        va_end(args);
        return "[format error]";
    }

    // Format the string (reserve +1 for terminating null)
    std::string result(static_cast<std::size_t>(size) + 1, '\0');
    int written = std::vsnprintf(result.data(), result.size(), fmt, args);
    va_end(args);
    if (written < 0) {
        return "[format error]";
    }
    result.resize(static_cast<std::size_t>(written));
    return result;
}

} // namespace detail

// ---- Macros (runtime level check; compiler elides disabled branches) ----
#define LUMINA_LOG_TRACE()                                                                                             \
    if (static_cast<int>(::lumina::telemetry::LogLevel::Trace) < static_cast<int>(LUMINA_LOG_COMPILE_LEVEL)) {         \
    } else if (!::lumina::telemetry::Logger::Instance().ShouldLog(::lumina::telemetry::LogLevel::Trace)) {             \
    } else                                                                                                             \
        ::lumina::telemetry::LogLine(::lumina::telemetry::LogLevel::Trace,                                             \
                                     ::lumina::telemetry::detail::Basename(__FILE__), __FILE__, __LINE__)

#define LUMINA_LOG_DEBUG()                                                                                             \
    if (static_cast<int>(::lumina::telemetry::LogLevel::Debug) < static_cast<int>(LUMINA_LOG_COMPILE_LEVEL)) {         \
    } else if (!::lumina::telemetry::Logger::Instance().ShouldLog(::lumina::telemetry::LogLevel::Debug)) {             \
    } else                                                                                                             \
        ::lumina::telemetry::LogLine(::lumina::telemetry::LogLevel::Debug,                                             \
                                     ::lumina::telemetry::detail::Basename(__FILE__), __FILE__, __LINE__)

#define LUMINA_LOG_INFO()                                                                                              \
    if (static_cast<int>(::lumina::telemetry::LogLevel::Info) < static_cast<int>(LUMINA_LOG_COMPILE_LEVEL)) {          \
    } else if (!::lumina::telemetry::Logger::Instance().ShouldLog(::lumina::telemetry::LogLevel::Info)) {              \
    } else                                                                                                             \
        ::lumina::telemetry::LogLine(::lumina::telemetry::LogLevel::Info,                                              \
                                     ::lumina::telemetry::detail::Basename(__FILE__), __FILE__, __LINE__)

#define LUMINA_LOG_WARN()                                                                                              \
    if (static_cast<int>(::lumina::telemetry::LogLevel::Warn) < static_cast<int>(LUMINA_LOG_COMPILE_LEVEL)) {          \
    } else if (!::lumina::telemetry::Logger::Instance().ShouldLog(::lumina::telemetry::LogLevel::Warn)) {              \
    } else                                                                                                             \
        ::lumina::telemetry::LogLine(::lumina::telemetry::LogLevel::Warn,                                              \
                                     ::lumina::telemetry::detail::Basename(__FILE__), __FILE__, __LINE__)

#define LUMINA_LOG_ERROR()                                                                                             \
    if (static_cast<int>(::lumina::telemetry::LogLevel::Error) < static_cast<int>(LUMINA_LOG_COMPILE_LEVEL)) {         \
    } else if (!::lumina::telemetry::Logger::Instance().ShouldLog(::lumina::telemetry::LogLevel::Error)) {             \
    } else                                                                                                             \
        ::lumina::telemetry::LogLine(::lumina::telemetry::LogLevel::Error,                                             \
                                     ::lumina::telemetry::detail::Basename(__FILE__), __FILE__, __LINE__)

// ---- Printf-style formatting macros ----
#define LUMINA_LOG(level, fmt, ...)                                                                                    \
    do {                                                                                                               \
        if (::lumina::telemetry::Logger::Instance().ShouldLog(level)) {                                                \
            ::lumina::telemetry::Logger::Instance().Submit(                                                            \
                level, ::lumina::telemetry::detail::Basename(__FILE__),                                                \
                ::lumina::telemetry::detail::FormatString(fmt, ##__VA_ARGS__), __FILE__, __LINE__);                    \
        }                                                                                                              \
    } while (0)

#define LUMINA_LOG_TRACE_F(fmt, ...) LUMINA_LOG(::lumina::telemetry::LogLevel::Trace, fmt, ##__VA_ARGS__)
#define LUMINA_LOG_DEBUG_F(fmt, ...) LUMINA_LOG(::lumina::telemetry::LogLevel::Debug, fmt, ##__VA_ARGS__)
#define LUMINA_LOG_INFO_F(fmt, ...) LUMINA_LOG(::lumina::telemetry::LogLevel::Info, fmt, ##__VA_ARGS__)
#define LUMINA_LOG_WARN_F(fmt, ...) LUMINA_LOG(::lumina::telemetry::LogLevel::Warn, fmt, ##__VA_ARGS__)
#define LUMINA_LOG_ERROR_F(fmt, ...) LUMINA_LOG(::lumina::telemetry::LogLevel::Error, fmt, ##__VA_ARGS__)

// Convenience: log Status errors
#define LUMINA_LOG_IF_ERROR(status_expr)                                                                               \
    do {                                                                                                               \
        const auto& _s = (status_expr);                                                                                \
        if (!_s.IsOk()) {                                                                                              \
            LUMINA_LOG_WARN() << "Status=" << static_cast<int>(_s.Code()) << " Msg=" << _s.Message();                  \
        }                                                                                                              \
    } while (0)

#define LUMINA_LOG_IF_ERROR_F(status_expr, fmt, ...)                                                                   \
    do {                                                                                                               \
        const auto& _s = (status_expr);                                                                                \
        if (!_s.IsOk()) {                                                                                              \
            LUMINA_LOG_WARN_F("Status=%d Msg=%s: " fmt, static_cast<int>(_s.Code()), _s.Message().c_str(),             \
                              ##__VA_ARGS__);                                                                          \
        }                                                                                                              \
    } while (0)

} // namespace lumina::telemetry
