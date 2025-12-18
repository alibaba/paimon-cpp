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

#include "paimon/common/factories/io_hook.h"

#include <atomic>
#include <stdexcept>

#include "fmt/format.h"
#include "paimon/status.h"

namespace paimon {

class IOHook::Impl {
 public:
    Status Try(const std::string& path) {
        if (io_count_.fetch_add(1) < pos_.load()) {
            return Status::OK();
        } else {
            switch (mode_) {
                case IOHook::Mode::SILENT:
                    return Status::OK();
                case IOHook::Mode::RETURN_ERROR:
                    return Status::IOError(fmt::format(
                        "io hook triggered io error at position {}, path {}", pos_.load(), path));
                case IOHook::Mode::THROW_EXCEPTION:
                    throw std::runtime_error(fmt::format(
                        "io hook throw io exception at position {}, path {}", pos_.load(), path));
                    return Status::OK();
                default:
                    return Status::OK();
            }
        }
    }

    inline void Reset(int64_t pos, IOHook::Mode mode) {
        pos_ = pos;
        io_count_ = 0;
        mode_ = mode;
    }

    int64_t IOCount() const {
        return io_count_.load();
    }

    void Clear() {
        Reset(-1, IOHook::Mode::SILENT);
    }

 private:
    std::atomic<int64_t> io_count_ = {0};
    std::atomic<int64_t> pos_ = {-1};
    IOHook::Mode mode_ = IOHook::Mode::SILENT;
};

IOHook::IOHook() : impl_(std::make_unique<IOHook::Impl>()) {}
IOHook::~IOHook() = default;

Status IOHook::Try(const std::string& path) {
    return impl_->Try(path);
}

int64_t IOHook::IOCount() const {
    return impl_->IOCount();
}

void IOHook::Clear() {
    return impl_->Clear();
}

void IOHook::Reset(int64_t pos, IOHook::Mode mode) {
    return impl_->Reset(pos, mode);
}

}  // namespace paimon
