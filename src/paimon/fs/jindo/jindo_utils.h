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

#include "JdoStatus.hpp"  // NOLINT(build/include_subdir)
#include "paimon/status.h"

namespace paimon::jindo {
#define PAIMON_RETURN_NOT_OK_FROM_JINDO(JINDO_STATUS) \
    do {                                              \
        auto __s = (JINDO_STATUS);                    \
        if (PAIMON_UNLIKELY(!(__s).ok())) {           \
            return Status::IOError(__s.errMsg());     \
        }                                             \
    } while (false)

}  // namespace paimon::jindo
