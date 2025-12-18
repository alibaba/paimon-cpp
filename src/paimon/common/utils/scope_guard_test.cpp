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

#include "paimon/common/utils/scope_guard.h"

#include <memory>

#include "gtest/gtest.h"

namespace paimon::test {

// Test case: ScopeGuard triggers the function when going out of scope
TEST(ScopeGuardTest, ScopeGuardTriggersOnDestruction) {
    bool triggered = false;

    {
        ScopeGuard guard([&triggered]() { triggered = true; });
    }

    // At this point, the scope guard should have triggered the lambda
    EXPECT_TRUE(triggered);
}

// Test case: ScopeGuard does not trigger after calling Release()
TEST(ScopeGuardTest, ScopeGuardDoesNotTriggerAfterRelease) {
    bool triggered = false;

    {
        ScopeGuard guard([&triggered]() { triggered = true; });
        guard.Release();  // Release the guard, it should not trigger
    }

    // The scope guard should not trigger the lambda after release
    EXPECT_FALSE(triggered);
}

// Test case: ScopeGuard without any function (empty guard)
TEST(ScopeGuardTest, ScopeGuardEmptyGuardDoesNothing) {
    bool triggered = false;

    {
        ScopeGuard guard([]() {});  // Empty function
    }

    // Nothing should happen
    EXPECT_FALSE(triggered);
}

// Test case: ScopeGuard with a complex action
TEST(ScopeGuardTest, ScopeGuardComplexAction) {
    bool triggered = false;

    {
        ScopeGuard guard([&triggered]() {
            triggered = true;
            // Simulate some complex cleanup or action
        });
    }

    // The scope guard should trigger the complex action
    EXPECT_TRUE(triggered);
}

}  // namespace paimon::test
