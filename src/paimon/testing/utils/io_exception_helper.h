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
#pragma once

#include <string>

namespace paimon::test {

// use for io exception cases:
// bool run_complete = false;
// auto io_hook = IOHook::GetInstance();
// for (size_t i = 0; i < turn_count; i++) {
//     ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
//     io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
//     // execute some funcs with io exception triggered
//     // check the error msg to make sure error status caused by io exception
//     CHECK_HOOK_STATUS(Func1(), i);
//     CHECK_HOOK_STATUS(Func2(), i);
//     CHECK_HOOK_STATUS(Func3(), i);
//     run_complete = true;
//     // when all func finished, check the result
//     io_hook->Clear();
//     Check();
//     break;
// }
// // make sure all funcs run complete
// ASSERT_TRUE(run_complete);

#define CHECK_HOOK_STATUS(status, io_count)                                                  \
    {                                                                                        \
        auto __s = (status);                                                                 \
        if (!__s.ok()) {                                                                     \
            if (__s.ToString().find(fmt::format("io hook triggered io error at position {}", \
                                                io_count)) != std::string::npos) {           \
                continue;                                                                    \
            } else {                                                                         \
                FAIL() << __s.ToString();                                                    \
            }                                                                                \
        }                                                                                    \
    }

#define CHECK_HOOK_STATUS_WITHOUT_MESSAGE_CHECK(status) \
    {                                                   \
        auto __s = (status);                            \
        if (!__s.ok()) {                                \
            continue;                                   \
        }                                               \
    }
}  // namespace paimon::test
