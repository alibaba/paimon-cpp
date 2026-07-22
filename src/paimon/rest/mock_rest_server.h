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

#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <thread>

#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

/// A minimal blocking HTTP/1.1 server for REST catalog unit tests. It listens on a
/// random port of 127.0.0.1, parses one request per connection and answers it with the
/// response returned by the registered handler. Test only, not production code.
class MockRestServer {
 public:
    struct Request {
        std::string method;
        /// Url-decoded path, e.g. "/v1/config".
        std::string path;
        /// Url-decoded query parameters.
        std::map<std::string, std::string> query_params;
        /// Headers with lower-cased names.
        std::map<std::string, std::string> headers;
        std::string body;
    };

    struct Response {
        int32_t code = 200;
        std::string body;
        std::string content_type = "application/json";
        std::map<std::string, std::string> headers;
        /// Close without answering; the client observes CURLE_GOT_NOTHING, a transport
        /// error that is never retried.
        bool close_without_response = false;
        /// Advertise this many bytes beyond the actual body in `Content-Length`, then
        /// close: the client sees the response cut off mid-body (CURLE_PARTIAL_FILE), a
        /// retriable transport error.
        size_t missing_body_bytes = 0;
    };

    using Handler = std::function<Response(const Request&)>;

    /// Starts the server; `handler` runs on the accept thread, so any state it touches
    /// must be thread-safe.
    static Result<std::unique_ptr<MockRestServer>> Start(Handler handler);

    ~MockRestServer();

    void Stop();

    int32_t GetPort() const {
        return port_;
    }

    /// "http://127.0.0.1:<port>"
    std::string GetBaseUri() const;

 private:
    MockRestServer(Handler handler, int32_t listen_fd, int32_t port);

    void AcceptLoop();
    void HandleConnection(int32_t connection_fd);

    Handler handler_;
    int32_t listen_fd_ = -1;
    int32_t port_ = 0;
    std::atomic<bool> stopped_{false};
    std::thread accept_thread_;
};

}  // namespace paimon
