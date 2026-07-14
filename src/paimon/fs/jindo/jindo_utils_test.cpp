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

#include "paimon/fs/jindo/jindo_utils.h"

#include "gtest/gtest.h"
#include "jdo_error.h"  // NOLINT(build/include_subdir)
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::jindo::test {

namespace {

Status ConvertWithNotExist(const JdoStatus& jdo_status) {
    PAIMON_RETURN_NOT_OK_FROM_JINDO_WITH_NOT_EXIST(jdo_status);
    return Status::OK();
}

Status Convert(const JdoStatus& jdo_status) {
    PAIMON_RETURN_NOT_OK_FROM_JINDO(jdo_status);
    return Status::OK();
}

}  // namespace

TEST(JindoUtilsTest, TestNotExistMacroMapsStatusByErrorCode) {
    ASSERT_OK(ConvertWithNotExist(JdoStatus()));

    Status not_found = ConvertWithNotExist(JdoStatus(JDO_FILE_NOT_FOUND_ERROR, "file not found"));
    ASSERT_TRUE(not_found.IsNotExist());
    ASSERT_NOK_WITH_MSG(not_found, "file not found");

    Status other = ConvertWithNotExist(JdoStatus(JDO_FILE_NOT_FOUND_ERROR + 1, "some other error"));
    ASSERT_TRUE(other.IsIOError());
    ASSERT_NOK_WITH_MSG(other, "some other error");
}

TEST(JindoUtilsTest, TestPlainMacroKeepsIOErrorForNotFound) {
    Status status = Convert(JdoStatus(JDO_FILE_NOT_FOUND_ERROR, "file not found"));
    ASSERT_TRUE(status.IsIOError());
}

}  // namespace paimon::jindo::test
