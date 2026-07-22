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

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>

#include "paimon/logging.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

/// HTTP status codes used by the REST catalog.
struct HttpStatus {
    static constexpr int64_t kOk = 200;
    static constexpr int64_t kAccepted = 202;
    static constexpr int64_t kNoContent = 204;
    static constexpr int64_t kBadRequest = 400;
    static constexpr int64_t kUnauthorized = 401;
    static constexpr int64_t kForbidden = 403;
    static constexpr int64_t kNotFound = 404;
    static constexpr int64_t kConflict = 409;
    static constexpr int64_t kTooManyRequests = 429;
    static constexpr int64_t kInternalServerError = 500;
    static constexpr int64_t kNotImplemented = 501;
    static constexpr int64_t kServiceUnavailable = 503;
};

/// A blocking HTTP client for the REST catalog based on libcurl. HTTP 429/503
/// responses are retried for all methods, transient transport errors only for
/// idempotent ones, with exponential backoff honoring a positive `Retry-After`
/// response header (delta-seconds or HTTP-date form). A backoff sleep is bounded by
/// `retry_max_delay_ms` and the whole request by `retry_timeout_ms`; a `Retry-After`
/// beyond the remaining budget stops retrying rather than shortening the sleep.
/// Redirects to http(s) targets are followed transparently, keeping the method and
/// body of POST/DELETE requests.
class RestHttpClient {
 public:
    struct Config {
        int64_t connect_timeout_ms = 180 * 1000;
        int64_t request_timeout_ms = 180 * 1000;
        int32_t max_retries = 5;
        int64_t retry_base_delay_ms = 1000;
        /// Upper bound for a single backoff sleep; a longer `Retry-After` is still
        /// honored, bounded only by `retry_timeout_ms`.
        int64_t retry_max_delay_ms = 64 * 1000;
        /// Overall budget for one `Execute` call, covering all attempts and retry
        /// sleeps; a retry whose delay does not fit the remaining budget is not
        /// attempted.
        int64_t retry_timeout_ms = 5 * 60 * 1000;
    };

    struct Response {
        int64_t code = 0;
        std::string body;
        /// Response headers with lower-cased names.
        std::map<std::string, std::string> headers;

        bool IsSuccessful() const {
            return code == HttpStatus::kOk || code == HttpStatus::kAccepted ||
                   code == HttpStatus::kNoContent;
        }
    };

    /// Creates a client against `base_uri`. The uri is normalized: surrounding
    /// whitespace and all trailing '/' are stripped and "http://" is prepended when no
    /// scheme is present.
    static Result<std::unique_ptr<RestHttpClient>> Create(const std::string& base_uri);
    static Result<std::unique_ptr<RestHttpClient>> Create(const std::string& base_uri,
                                                          const Config& config);

    ~RestHttpClient();

    /// Executes `method` ("GET", "POST" or "DELETE") on `path` (already url-encoded,
    /// starting with '/'); query parameter keys and values are url-encoded internally.
    /// Header names must be valid HTTP tokens and values must not contain CR, LF or
    /// NUL; a violating header fails the request before anything is sent. Returns the
    /// final response, which may carry a non-2xx code, or an error status when the
    /// request could not be transported at all. Only transient transport errors (an
    /// established connection breaking mid-request or a truncated response body) are
    /// retried; every other transport failure fails immediately.
    Result<Response> Execute(const std::string& method, const std::string& path,
                             const std::map<std::string, std::string>& query_params,
                             const std::map<std::string, std::string>& headers,
                             const std::string& body) const;

    const std::string& GetBaseUri() const {
        return base_uri_;
    }

    static std::string NormalizeUri(const std::string& uri);

    /// Builds "k1=v1&k2=v2" with url-encoded keys and values.
    static std::string BuildQueryString(const std::map<std::string, std::string>& query_params);

    /// Computes the delay before the retry following the `execution_count`-th attempt
    /// (counted from 1), in ms, or `nullopt` when retrying should stop. A positive
    /// `Retry-After` header of `response` (delta-seconds, or HTTP-date resolved against
    /// `now_epoch_seconds`) wins and is never shortened: beyond `remaining_budget_ms`
    /// it yields `nullopt`. Otherwise exponential backoff with up to 10% jitter
    /// applies, clamped to `retry_max_delay_ms`, yielding `nullopt` when even the
    /// clamped delay exceeds `remaining_budget_ms`. `response` is null when the attempt
    /// failed with a transport error.
    static std::optional<int64_t> ComputeRetryDelayMs(const Config& config, int32_t execution_count,
                                                      const Response* response,
                                                      int64_t now_epoch_seconds,
                                                      int64_t remaining_budget_ms);

 private:
    RestHttpClient(const std::string& base_uri, const Config& config);

    /// Pool of the libcurl easy handles used by `ExecuteOnce`, also keeping libcurl's
    /// global state alive for the lifetime of the client.
    class HandlePool;

    /// On a transport failure, `transport_retriable` reports whether the failure kind
    /// may be retried; see `Execute` for which kinds are not.
    Result<Response> ExecuteOnce(const std::string& method, const std::string& url,
                                 const std::map<std::string, std::string>& headers,
                                 const std::string& body, bool* transport_retriable) const;

    std::optional<int64_t> GetRetryDelayMs(int32_t execution_count, const Response* response,
                                           int64_t remaining_budget_ms) const;

    std::unique_ptr<HandlePool> handle_pool_;
    std::string base_uri_;
    Config config_;
    std::shared_ptr<Logger> logger_;
};

}  // namespace paimon
