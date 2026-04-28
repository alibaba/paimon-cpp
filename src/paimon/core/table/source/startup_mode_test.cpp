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

#include "paimon/table/source/startup_mode.h"

#include "gtest/gtest.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(StartupModeTest, FromString) {
    ASSERT_OK_AND_ASSIGN(StartupMode mode, StartupMode::FromString("default"));
    ASSERT_EQ(StartupMode::Default(), mode);
    ASSERT_OK_AND_ASSIGN(mode, StartupMode::FromString("latest-full"));
    ASSERT_EQ(StartupMode::LatestFull(), mode);
    ASSERT_OK_AND_ASSIGN(mode, StartupMode::FromString("latest"));
    ASSERT_EQ(StartupMode::Latest(), mode);
    ASSERT_OK_AND_ASSIGN(mode, StartupMode::FromString("from-snapshot"));
    ASSERT_EQ(StartupMode::FromSnapshot(), mode);
    ASSERT_OK_AND_ASSIGN(mode, StartupMode::FromString("from-snapshot-full"));
    ASSERT_EQ(StartupMode::FromSnapshotFull(), mode);
    ASSERT_OK_AND_ASSIGN(mode, StartupMode::FromString("from-timestamp"));
    ASSERT_EQ(StartupMode::FromTimestamp(), mode);
    ASSERT_NOK(StartupMode::FromString("unknown"));
}

}  // namespace paimon::test
