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

#include "paimon/common/fs/http_client.h"

#include <curl/curl.h>

#include <chrono>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/common/utils/string_utils.h"

namespace paimon {

namespace {

constexpr int32_t kMaxAttempts = 3;

class CurlGlobalGuard {
 public:
    CurlGlobalGuard() {
        curl_global_init(CURL_GLOBAL_DEFAULT);
    }
    ~CurlGlobalGuard() {
        curl_global_cleanup();
    }
};

std::shared_ptr<CurlGlobalGuard> GetCurlGlobalGuard() {
    static auto guard = std::make_shared<CurlGlobalGuard>();
    return guard;
}

void TrimHttpWhitespace(std::string* value) {
    constexpr char kHttpWhitespace[] = " \t\r\n";
    size_t begin = value->find_first_not_of(kHttpWhitespace);
    if (begin == std::string::npos) {
        value->clear();
        return;
    }
    size_t end = value->find_last_not_of(kHttpWhitespace);
    value->erase(end + 1);
    value->erase(0, begin);
}

struct TransferContext {
    CURL* handle;
    const HttpBodyConsumer* consumer;
    HttpResponse response;
    Status status = Status::OK();
};

size_t WriteCallback(char* data, size_t size, size_t count, void* user_data) {
    auto* context = static_cast<TransferContext*>(user_data);
    size_t bytes = size * count;
    if (!context->status.ok()) {
        return 0;
    }
    long status_code = 0;  // NOLINT(runtime/int, google-runtime-int): required by curl.
    curl_easy_getinfo(context->handle, CURLINFO_RESPONSE_CODE, &status_code);
    context->response.status_code = static_cast<int32_t>(status_code);
    if (status_code >= 300) {
        context->response.body_size += static_cast<int64_t>(bytes);
        return bytes;
    }
    context->status = (*context->consumer)(data, static_cast<int64_t>(bytes));
    if (!context->status.ok()) {
        return 0;
    }
    context->response.body_size += static_cast<int64_t>(bytes);
    return bytes;
}

size_t HeaderCallback(char* data, size_t size, size_t count, void* user_data) {
    auto* context = static_cast<TransferContext*>(user_data);
    size_t bytes = size * count;
    std::string line(data, bytes);
    size_t colon = line.find(':');
    if (colon != std::string::npos) {
        std::string name = line.substr(0, colon);
        TrimHttpWhitespace(&name);
        name = StringUtils::ToLowerCase(name);
        std::string value = line.substr(colon + 1);
        TrimHttpWhitespace(&value);
        context->response.headers[name] = std::move(value);
    }
    return bytes;
}

bool IsRetryable(CURLcode code, int64_t status_code) {
    if (code == CURLE_COULDNT_RESOLVE_HOST || code == CURLE_COULDNT_CONNECT ||
        code == CURLE_OPERATION_TIMEDOUT || code == CURLE_SEND_ERROR || code == CURLE_RECV_ERROR ||
        code == CURLE_PARTIAL_FILE) {
        return true;
    }
    return status_code == 429 || status_code >= 500;
}

}  // namespace

class CurlHttpClient::Impl {
 public:
    Impl() : guard_(GetCurlGlobalGuard()) {}

    ~Impl() {
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
        curl_easy_reset(handle);
        std::scoped_lock lock(mutex_);
        handles_.push_back(handle);
    }

 private:
    std::shared_ptr<CurlGlobalGuard> guard_;
    mutable std::mutex mutex_;
    mutable std::vector<CURL*> handles_;
};

CurlHttpClient::CurlHttpClient() : impl_(std::make_unique<Impl>()) {}
CurlHttpClient::~CurlHttpClient() = default;

Result<HttpResponse> CurlHttpClient::Execute(const HttpRequest& request,
                                             const HttpBodyConsumer& consumer) const {
    for (int32_t attempt = 0; attempt < kMaxAttempts; ++attempt) {
        CURL* handle = impl_->Acquire();
        if (handle == nullptr) {
            return Status::IOError("failed to create curl easy handle");
        }
        ScopeGuard release_handle([this, handle] { impl_->Release(handle); });
        TransferContext context{handle, &consumer, {}, Status::OK()};
        curl_slist* headers = nullptr;
        ScopeGuard release_headers([&headers] { curl_slist_free_all(headers); });
        for (const auto& [name, value] : request.headers) {
            curl_slist* updated_headers = curl_slist_append(headers, (name + ": " + value).c_str());
            if (updated_headers == nullptr) {
                return Status::IOError("failed to create HTTP headers");
            }
            headers = updated_headers;
        }
        curl_easy_setopt(handle, CURLOPT_URL, request.url.c_str());
        curl_easy_setopt(handle, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(handle, CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(handle, CURLOPT_CONNECTTIMEOUT_MS, 30000L);
        curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(handle, CURLOPT_WRITEDATA, &context);
        curl_easy_setopt(handle, CURLOPT_HEADERFUNCTION, HeaderCallback);
        curl_easy_setopt(handle, CURLOPT_HEADERDATA, &context);
        curl_easy_setopt(handle, CURLOPT_FOLLOWLOCATION, 0L);
        if (request.method == HttpMethod::HEAD) {
            curl_easy_setopt(handle, CURLOPT_NOBODY, 1L);
        }
        CURLcode code = curl_easy_perform(handle);
        long response_code = 0;  // NOLINT(runtime/int, google-runtime-int): required by curl.
        curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &response_code);
        context.response.status_code = static_cast<int32_t>(response_code);

        if (!context.status.ok()) {
            return context.status;
        }
        if (code == CURLE_OK && !IsRetryable(code, response_code)) {
            return context.response;
        }
        if (!IsRetryable(code, response_code) ||
            (context.response.body_size > 0 && response_code < 300) ||
            attempt + 1 == kMaxAttempts) {
            if (code == CURLE_OK) {
                return Status::IOError(fmt::format("HTTP request to {} returned status {}",
                                                   request.url, response_code));
            }
            return Status::IOError(fmt::format("HTTP request to {} failed: {} (status {})",
                                               request.url, curl_easy_strerror(code),
                                               response_code));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100 * (1 << attempt)));
    }
    return Status::IOError("HTTP request failed");
}

}  // namespace paimon
