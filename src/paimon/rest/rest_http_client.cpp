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

#include "paimon/rest/rest_http_client.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstring>
#include <ctime>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <thread>
#include <utility>
#include <vector>

#include "curl/curl.h"
#include "fmt/format.h"
#include "paimon/common/utils/http_client.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/common/utils/url_utils.h"
#include "paimon/rest/rest_util.h"

namespace paimon {

namespace {

size_t WriteBodyCallback(char* data, size_t size, size_t nmemb, void* user_data) {
    auto* body = static_cast<std::string*>(user_data);
    body->append(data, size * nmemb);
    return size * nmemb;
}

size_t WriteHeaderCallback(char* data, size_t size, size_t nmemb, void* user_data) {
    auto* headers = static_cast<HttpHeaders*>(user_data);
    size_t total = size * nmemb;
    // A status line starts the header block of the next response on the connection
    // (e.g. after a followed redirect); dropping the previous headers leaves only the
    // final response's.
    if (total >= 5 && std::strncmp(data, "HTTP/", 5) == 0) {
        headers->clear();
    }
    ParseHttpHeaderLine(data, total, headers);
    return total;
}

// Only the methods `Execute` supports are classified; POST is the sole non-idempotent
// one among them.
bool IsIdempotent(const std::string& method) {
    return method == "GET" || method == "DELETE";
}

bool IsRetriableCode(int64_t code) {
    return code == HttpStatus::kTooManyRequests || code == HttpStatus::kServiceUnavailable;
}

// Parses the RFC 1123 form of an http date, e.g. "Wed, 21 Oct 2015 07:28:00 GMT", into
// unix epoch seconds. Month names are matched against a fixed English table so the
// result does not depend on the process locale.
std::optional<int64_t> ParseHttpDateSeconds(const std::string& value) {
    std::vector<std::string> parts = StringUtils::Split(value, " ", /*ignore_empty=*/true);
    if (parts.size() != 6 || parts[5] != "GMT" || parts[0].size() != 4 || parts[0][3] != ',') {
        return std::nullopt;
    }
    static constexpr const char* kMonths[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
    int64_t month = 0;
    for (size_t i = 0; i < 12; i++) {
        if (parts[2] == kMonths[i]) {
            month = static_cast<int64_t>(i) + 1;
            break;
        }
    }
    std::optional<int64_t> day = StringUtils::StringToValue<int64_t>(parts[1]);
    std::optional<int64_t> year = StringUtils::StringToValue<int64_t>(parts[3]);
    std::vector<std::string> time_parts = StringUtils::Split(parts[4], ":",
                                                             /*ignore_empty=*/false);
    if (month == 0 || !day || !year || time_parts.size() != 3) {
        return std::nullopt;
    }
    std::optional<int64_t> hour = StringUtils::StringToValue<int64_t>(time_parts[0]);
    std::optional<int64_t> minute = StringUtils::StringToValue<int64_t>(time_parts[1]);
    std::optional<int64_t> second = StringUtils::StringToValue<int64_t>(time_parts[2]);
    if (!hour || !minute || !second || day.value() < 1 || day.value() > 31 || year.value() < 1970 ||
        year.value() > 9999 || hour.value() > 23 || minute.value() > 59 || second.value() > 60) {
        return std::nullopt;
    }
    // Days between 1970-01-01 and the given civil date ("days from civil" algorithm).
    int64_t y = year.value() - (month <= 2 ? 1 : 0);
    int64_t era = y / 400;
    int64_t yoe = y - era * 400;
    int64_t doy = (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day.value() - 1;
    int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    int64_t days = era * 146097 + doe - 719468;
    return days * 86400 + hour.value() * 3600 + minute.value() * 60 + second.value();
}

// Allowlist of the transient transport failures: an established connection breaking
// mid-request (send/receive failures, HTTP/2 stream errors) or a response truncated
// mid-body. The request reached a live server, so retrying an idempotent method is
// likely to succeed. Every other CURLcode (resolution/connect failures, timeouts, TLS
// failures, malformed urls, redirect loops, ...) is permanent and costs one attempt.
bool IsRetriableTransportError(CURLcode code) {
    switch (code) {
        case CURLE_SEND_ERROR:
        case CURLE_RECV_ERROR:
        case CURLE_PARTIAL_FILE:
        case CURLE_HTTP2:
        case CURLE_HTTP2_STREAM:
            return true;
        default:
            return false;
    }
}

// libcurl sends no User-Agent of its own, so requests would reach the server unnamed. A
// "header.User-Agent" option still wins: an explicit request header overrides
// CURLOPT_USERAGENT.
constexpr const char kDefaultUserAgent[] = "paimon-cpp";

// A valid HTTP header field name per RFC 7230: one or more tchar.
bool IsValidHeaderName(const std::string& name) {
    if (name.empty()) {
        return false;
    }
    static constexpr char kTcharSymbols[] = "!#$%&'*+-.^_`|~";
    for (char c : name) {
        if (c != '\0' && (std::isalnum(static_cast<unsigned char>(c)) ||
                          std::strchr(kTcharSymbols, c) != nullptr)) {
            continue;
        }
        return false;
    }
    return true;
}

// libcurl sends header strings verbatim, so a value containing CR, LF or NUL could
// inject additional header lines into the request.
bool IsValidHeaderValue(const std::string& value) {
    return value.find_first_of("\r\n") == std::string::npos &&
           value.find('\0') == std::string::npos;
}

}  // namespace

// A libcurl easy handle owns the connection cache of the connections it opened, so
// returning it to the pool after a request keeps the connection alive for the next one:
// re-creating a handle per request would pay a TCP and TLS handshake every time. A handle
// must not be used by two threads at once, hence handing it out exclusively.
class RestHttpClient::HandlePool {
 public:
    HandlePool() : curl_guard_(EnsureCurlGlobalInit()) {}

    ~HandlePool() {
        for (CURL* handle : handles_) {
            curl_easy_cleanup(handle);
        }
    }

    CURL* Acquire() const {
        std::scoped_lock lock(mutex_);
        if (handles_.empty()) {
            return curl_easy_init();
        }
        CURL* handle = handles_.back();
        handles_.pop_back();
        return handle;
    }

    void Release(CURL* handle) const {
        // Resetting drops the options of the finished request while keeping the
        // connection cache, so the next request starts from a clean handle.
        curl_easy_reset(handle);
        std::scoped_lock lock(mutex_);
        handles_.push_back(handle);
    }

 private:
    std::shared_ptr<void> curl_guard_;
    mutable std::mutex mutex_;
    mutable std::vector<CURL*> handles_;
};

RestHttpClient::RestHttpClient(const std::string& base_uri, const Config& config)
    : handle_pool_(std::make_unique<HandlePool>()),
      base_uri_(base_uri),
      config_(config),
      logger_(Logger::GetLogger("RestHttpClient")) {}

RestHttpClient::~RestHttpClient() = default;

Result<std::unique_ptr<RestHttpClient>> RestHttpClient::Create(const std::string& base_uri) {
    return Create(base_uri, Config());
}

Result<std::unique_ptr<RestHttpClient>> RestHttpClient::Create(const std::string& base_uri,
                                                               const Config& config) {
    if (base_uri.empty()) {
        return Status::Invalid("uri of the http client is empty");
    }
    return std::unique_ptr<RestHttpClient>(new RestHttpClient(NormalizeUri(base_uri), config));
}

std::string RestHttpClient::NormalizeUri(const std::string& uri) {
    std::string normalized = uri;
    StringUtils::Trim(&normalized);
    while (!normalized.empty() && normalized.back() == '/') {
        normalized.pop_back();
    }
    if (normalized.rfind("http://", 0) != 0 && normalized.rfind("https://", 0) != 0) {
        normalized = "http://" + normalized;
    }
    return normalized;
}

std::string RestHttpClient::BuildQueryString(
    const std::map<std::string, std::string>& query_params) {
    std::string query;
    for (const auto& [key, value] : query_params) {
        if (!query.empty()) {
            query.push_back('&');
        }
        query.append(UrlUtils::EncodeString(key));
        query.push_back('=');
        query.append(UrlUtils::EncodeString(value));
    }
    return query;
}

Result<RestHttpClient::Response> RestHttpClient::ExecuteOnce(
    const std::string& method, const std::string& url,
    const std::map<std::string, std::string>& headers, const std::string& body,
    bool* transport_retriable) const {
    CURL* curl = handle_pool_->Acquire();
    if (curl == nullptr) {
        return Status::IOError("failed to create curl handle");
    }
    ScopeGuard release_curl([this, curl] { handle_pool_->Release(curl); });
    Response response;
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, kDefaultUserAgent);
    // TLS 1.0/1.1 are not accepted, as in the Java client. The curl constants of the
    // options below are `int`, while curl reads a `long` from the variadic argument.
    curl_easy_setopt(curl, CURLOPT_SSLVERSION,
                     static_cast<long>(CURL_SSLVERSION_TLSv1_2));  // NOLINT(runtime/int)
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS,
                     static_cast<long>(config_.connect_timeout_ms));  // NOLINT(runtime/int)
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS,
                     static_cast<long>(config_.request_timeout_ms));  // NOLINT(runtime/int)
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteBodyCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response.body);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, WriteHeaderCallback);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &response.headers);
    // Follow redirects transparently, restricted to http(s) targets. Without
    // CURLOPT_POSTREDIR a 301/302 would replay a body-carrying request as a bodyless
    // GET. A 303 is left to become a GET, which is what it is defined to mean.
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 50L);
    curl_easy_setopt(curl, CURLOPT_POSTREDIR,
                     static_cast<long>(CURL_REDIR_POST_301 |  // NOLINT(runtime/int)
                                       CURL_REDIR_POST_302));
#if CURL_AT_LEAST_VERSION(7, 85, 0)
    curl_easy_setopt(curl, CURLOPT_REDIR_PROTOCOLS_STR, "http,https");
#else
    curl_easy_setopt(curl, CURLOPT_REDIR_PROTOCOLS,
                     static_cast<long>(CURLPROTO_HTTP | CURLPROTO_HTTPS));  // NOLINT(runtime/int)
#endif

    if (method == "POST") {
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE,
                         static_cast<long>(body.size()));  // NOLINT(runtime/int)
    } else if (method == "DELETE") {
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
        if (!body.empty()) {
            curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
            curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE,
                             static_cast<long>(body.size()));  // NOLINT(runtime/int)
        }
    }

    struct curl_slist* header_list = nullptr;
    ScopeGuard free_headers([&header_list] { curl_slist_free_all(header_list); });
    auto append_header = [&header_list](const std::string& line) -> bool {
        struct curl_slist* updated_list = curl_slist_append(header_list, line.c_str());
        if (updated_list == nullptr) {
            return false;
        }
        header_list = updated_list;
        return true;
    };
    // An empty "Expect:" header disables libcurl's automatic "Expect: 100-continue".
    bool headers_ok = append_header("Expect:");
    for (const auto& [name, value] : headers) {
        headers_ok = headers_ok && append_header(name + ": " + value);
    }
    if (!headers_ok) {
        return Status::IOError("failed to create http headers");
    }
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, header_list);

    CURLcode curl_code = curl_easy_perform(curl);
    if (curl_code != CURLE_OK) {
        *transport_retriable = IsRetriableTransportError(curl_code);
        // Neither the url nor the response body is echoed in errors: both may carry
        // credentials.
        return Status::IOError(
            fmt::format("http {} request failed: {}", method, curl_easy_strerror(curl_code)));
    }
    // curl writes a `long` through the pointer, so the type cannot be narrowed here.
    // NOLINTNEXTLINE(google-runtime-int)
    long http_code = 0;  // NOLINT(runtime/int)
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    response.code = http_code;
    return response;
}

std::optional<int64_t> RestHttpClient::ComputeRetryDelayMs(const Config& config,
                                                           int32_t execution_count,
                                                           const Response* response,
                                                           int64_t now_epoch_seconds,
                                                           int64_t remaining_budget_ms) {
    if (remaining_budget_ms <= 0) {
        return std::nullopt;
    }
    if (response != nullptr) {
        auto iter = response->headers.find("retry-after");
        if (iter != response->headers.end()) {
            std::optional<int64_t> retry_after_ms;
            std::optional<int64_t> retry_after_seconds =
                StringUtils::StringToValue<int64_t>(iter->second);
            if (retry_after_seconds) {
                // Clamped so that a misbehaving server cannot overflow the conversion.
                constexpr int64_t kMaxSeconds = std::numeric_limits<int64_t>::max() / 1000;
                retry_after_ms =
                    std::clamp(retry_after_seconds.value(), -kMaxSeconds, kMaxSeconds) * 1000;
            } else {
                std::optional<int64_t> date_seconds = ParseHttpDateSeconds(iter->second);
                if (date_seconds) {
                    retry_after_ms = (date_seconds.value() - now_epoch_seconds) * 1000;
                }
            }
            // Non-positive values fall through to the exponential backoff. A delay
            // beyond the remaining budget stops retrying entirely: sleeping less than
            // requested would just hit the throttle again. `retry_max_delay_ms` is
            // deliberately not applied here, as in the Java client, so a longer
            // `Retry-After` is honored whenever it fits the budget.
            if (retry_after_ms && retry_after_ms.value() > 0) {
                if (retry_after_ms.value() > remaining_budget_ms) {
                    return std::nullopt;
                }
                return retry_after_ms.value();
            }
        }
    }
    int64_t multiplier = static_cast<int64_t>(1) << std::clamp(execution_count - 1, 0, 6);
    int64_t delay_ms = config.retry_base_delay_ms * multiplier;
    if (delay_ms > 0) {
        static thread_local std::mt19937 generator(
            std::random_device{}());  // NOLINT(whitespace/braces)
        std::uniform_int_distribution<int64_t> jitter(0, delay_ms / 10);
        delay_ms += jitter(generator);
    }
    delay_ms = std::min(delay_ms, config.retry_max_delay_ms);
    if (delay_ms > remaining_budget_ms) {
        return std::nullopt;
    }
    return delay_ms;
}

std::optional<int64_t> RestHttpClient::GetRetryDelayMs(int32_t execution_count,
                                                       const Response* response,
                                                       int64_t remaining_budget_ms) const {
    return ComputeRetryDelayMs(config_, execution_count, response,
                               static_cast<int64_t>(std::time(nullptr)), remaining_budget_ms);
}

Result<RestHttpClient::Response> RestHttpClient::Execute(
    const std::string& method, const std::string& path,
    const std::map<std::string, std::string>& query_params,
    const std::map<std::string, std::string>& headers, const std::string& body) const {
    if (method != "GET" && method != "POST" && method != "DELETE") {
        return Status::Invalid(fmt::format("unsupported http method: {}", method));
    }
    for (const auto& [name, value] : headers) {
        // Neither the name nor the value is echoed in errors: the name may contain
        // the very control bytes this check rejects, the value may carry credentials.
        if (!IsValidHeaderName(name)) {
            return Status::Invalid("invalid http header name");
        }
        if (!IsValidHeaderValue(value)) {
            return Status::Invalid(fmt::format("invalid http header value for '{}'", name));
        }
    }
    std::string url = base_uri_ + path;
    if (!query_params.empty()) {
        url += "?" + BuildQueryString(query_params);
    }
    bool idempotent = IsIdempotent(method);
    std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
    int32_t execution_count = 0;
    while (true) {
        execution_count++;
        bool transport_retriable = false;
        Result<Response> result = ExecuteOnce(method, url, headers, body, &transport_retriable);
        bool retriable;
        if (result.ok()) {
            retriable = IsRetriableCode(result.value().code);
        } else {
            retriable = idempotent && transport_retriable;
        }
        int64_t elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::steady_clock::now() - start)
                                 .count();
        std::optional<int64_t> delay_ms;
        if (retriable && execution_count <= config_.max_retries) {
            delay_ms = GetRetryDelayMs(execution_count, result.ok() ? &result.value() : nullptr,
                                       config_.retry_timeout_ms - elapsed_ms);
        }
        if (!delay_ms) {
            if (result.ok()) {
                const std::string request_id = RestUtil::ExtractRequestId(result.value().headers);
                PAIMON_LOG_DEBUG(
                    logger_, "[rest] requestId:%s method:%s path:%s status:%lld duration:%lldms",
                    request_id.c_str(), method.c_str(), path.c_str(),
                    static_cast<long long>(result.value().code),  // NOLINT(runtime/int)
                    static_cast<long long>(elapsed_ms));          // NOLINT(runtime/int)
            }
            return result;
        }
        std::string reason = result.ok() ? fmt::format("http status {}", result.value().code)
                                         : result.status().ToString();
        PAIMON_LOG_WARN(logger_, "[rest] retrying method:%s path:%s in %lld ms (retry %d/%d): %s",
                        method.c_str(), path.c_str(),
                        static_cast<long long>(delay_ms.value()),  // NOLINT(runtime/int)
                        execution_count, config_.max_retries, reason.c_str());
        if (delay_ms.value() > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms.value()));
        }
    }
}

}  // namespace paimon
