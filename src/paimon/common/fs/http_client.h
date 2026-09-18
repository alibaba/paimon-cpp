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
#include <functional>
#include <map>
#include <memory>
#include <string>

#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {

enum class HttpMethod { HEAD, GET };
using HttpHeaders = std::map<std::string, std::string>;
using HttpBodyConsumer = std::function<Status(const char*, int64_t)>;

struct HttpRequest {
    HttpMethod method = HttpMethod::GET;
    std::string url;
    HttpHeaders headers;
};

struct HttpResponse {
    int32_t status_code = 0;
    HttpHeaders headers;
    int64_t body_size = 0;
    std::string error_body;
};

class PAIMON_EXPORT HttpClient {
 public:
    virtual ~HttpClient() = default;
    virtual Result<HttpResponse> Execute(const HttpRequest& request,
                                         const HttpBodyConsumer& consumer) const = 0;
};

struct CurlHttpClientOptions {
    int64_t connect_timeout_ms = 30000;
    int64_t request_timeout_ms = 300000;
    int64_t low_speed_limit_bytes_per_second = 1;
    int64_t low_speed_time_seconds = 30;
};

class PAIMON_EXPORT CurlHttpClient : public HttpClient {
 public:
    explicit CurlHttpClient(CurlHttpClientOptions options = {});
    ~CurlHttpClient() override;

    Result<HttpResponse> Execute(const HttpRequest& request,
                                 const HttpBodyConsumer& consumer) const override;

 private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace paimon
