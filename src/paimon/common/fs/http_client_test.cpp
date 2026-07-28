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

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

class ScopedEnvironmentVariable {
 public:
    ScopedEnvironmentVariable(const char* name, const char* value) : name_(name) {
        const char* previous = std::getenv(name);
        if (previous != nullptr) {
            previous_ = previous;
        }
        setenv(name, value, 1);
    }

    ~ScopedEnvironmentVariable() {
        if (previous_) {
            setenv(name_.c_str(), previous_->c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

 private:
    std::string name_;
    std::optional<std::string> previous_;
};

class HttpTestServer {
 public:
    explicit HttpTestServer(std::vector<std::string> responses,
                            std::chrono::milliseconds response_delay = {})
        : responses_(std::move(responses)), response_delay_(response_delay) {
        socket_ = socket(AF_INET, SOCK_STREAM, 0);
        if (socket_ < 0) {
            return;
        }
        int32_t reuse_address = 1;
        setsockopt(socket_, SOL_SOCKET, SO_REUSEADDR, &reuse_address, sizeof(reuse_address));
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        if (bind(socket_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
            listen(socket_, 4) != 0) {
            close(socket_);
            socket_ = -1;
            return;
        }
        socklen_t address_size = sizeof(address);
        if (getsockname(socket_, reinterpret_cast<sockaddr*>(&address), &address_size) != 0) {
            close(socket_);
            socket_ = -1;
            return;
        }
        port_ = ntohs(address.sin_port);
        thread_ = std::thread([this] { Serve(); });
    }

    ~HttpTestServer() {
        if (socket_ >= 0) {
            shutdown(socket_, SHUT_RDWR);
            close(socket_);
        }
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    bool ok() const {
        return socket_ >= 0;
    }

    std::string url() const {
        return fmt::format("http://127.0.0.1:{}/test", port_);
    }

 private:
    void Serve() const {
        for (const std::string& response : responses_) {
            int32_t client = accept(socket_, nullptr, nullptr);
            if (client < 0) {
                return;
            }
            char request[4096];
            recv(client, request, sizeof(request), 0);
            if (response_delay_.count() > 0) {
                std::this_thread::sleep_for(response_delay_);
            }
            size_t sent = 0;
            while (sent < response.size()) {
                ssize_t result = send(client, response.data() + sent, response.size() - sent, 0);
                if (result <= 0) {
                    break;
                }
                sent += static_cast<size_t>(result);
            }
            shutdown(client, SHUT_RDWR);
            close(client);
        }
    }

    int32_t socket_ = -1;
    uint16_t port_ = 0;
    std::vector<std::string> responses_;
    std::chrono::milliseconds response_delay_;
    std::thread thread_;
};

TEST(CurlHttpClientTest, TestCapturesBoundedErrorBodyAndHeaders) {
    ScopedEnvironmentVariable no_proxy("NO_PROXY", "127.0.0.1");
    ScopedEnvironmentVariable lowercase_no_proxy("no_proxy", "127.0.0.1");
    std::string body(5000, 'x');
    HttpTestServer server(
        {"HTTP/1.1 403 Forbidden\r\nContent-Length: 5000\r\n"
         "X-Amz-Bucket-Region: eu-west-1\r\nConnection: close\r\n\r\n" +
         body});
    ASSERT_TRUE(server.ok());

    CurlHttpClient client;
    bool consumer_called = false;
    ASSERT_OK_AND_ASSIGN(HttpResponse response,
                         client.Execute({HttpMethod::GET, server.url(), {}},
                                        [&consumer_called](const char*, int64_t) {
                                            consumer_called = true;
                                            return Status::OK();
                                        }));
    ASSERT_EQ(response.status_code, 403);
    ASSERT_EQ(response.body_size, 5000);
    ASSERT_EQ(response.error_body.size(), 4096);
    ASSERT_EQ(response.headers["x-amz-bucket-region"], "eu-west-1");
    ASSERT_FALSE(consumer_called);
}

TEST(CurlHttpClientTest, TestConfiguredRequestTimeout) {
    ScopedEnvironmentVariable no_proxy("NO_PROXY", "127.0.0.1");
    ScopedEnvironmentVariable lowercase_no_proxy("no_proxy", "127.0.0.1");
    HttpTestServer server({"", "", ""}, std::chrono::milliseconds(100));
    ASSERT_TRUE(server.ok());
    CurlHttpClientOptions options;
    options.connect_timeout_ms = 100;
    options.request_timeout_ms = 50;
    options.low_speed_limit_bytes_per_second = 0;
    options.low_speed_time_seconds = 0;
    CurlHttpClient client(options);

    auto start = std::chrono::steady_clock::now();
    Result<HttpResponse> response = client.Execute(
        {HttpMethod::GET, server.url(), {}}, [](const char*, int64_t) { return Status::OK(); });
    auto elapsed = std::chrono::steady_clock::now() - start;
    ASSERT_TRUE(response.status().IsIOError());
    ASSERT_LT(elapsed, std::chrono::seconds(3));
}

TEST(CurlHttpClientTest, TestRetriesServerErrors) {
    ScopedEnvironmentVariable no_proxy("NO_PROXY", "127.0.0.1");
    ScopedEnvironmentVariable lowercase_no_proxy("no_proxy", "127.0.0.1");
    std::string server_error =
        "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 5\r\n"
        "Connection: close\r\n\r\nerror";
    HttpTestServer server(
        {server_error, server_error,
         "HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\ndata"});
    ASSERT_TRUE(server.ok());

    CurlHttpClient client;
    std::string body;
    ASSERT_OK_AND_ASSIGN(HttpResponse response,
                         client.Execute({HttpMethod::GET, server.url(), {}},
                                        [&body](const char* data, int64_t size) {
                                            body.append(data, static_cast<size_t>(size));
                                            return Status::OK();
                                        }));
    ASSERT_EQ(response.status_code, 200);
    ASSERT_EQ(body, "data");
}

}  // namespace
}  // namespace paimon::test
