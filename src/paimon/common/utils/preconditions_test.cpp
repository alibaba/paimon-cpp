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

#include "paimon/common/utils/preconditions.h"

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

// Test case: Test CheckState with a valid condition
TEST(PreconditionsTest, CheckStateValidCondition) {
    ASSERT_OK(Preconditions::CheckState(true, "Condition failed: {}", "Some error details"));
}

// Test case: Test CheckState with an invalid condition
TEST(PreconditionsTest, CheckStateInvalidCondition) {
    ASSERT_NOK_WITH_MSG(
        Preconditions::CheckState(false, "Condition failed: {}", "Some error details"),
        "Condition failed: Some error details");
}

// Test case: Test CheckNotNull with a non-null reference
TEST(PreconditionsTest, CheckNotNullNonNullReference) {
    int x = 10;
    ASSERT_OK(Preconditions::CheckNotNull(x));
}

// Test case: Test CheckNotNull with a non-null reference with msg
TEST(PreconditionsTest, CheckNotNullNonNullReference2) {
    int x = 10;
    ASSERT_OK(Preconditions::CheckNotNull(x, "Condition failed"));
}

// Test case: Test CheckNotNull with a null reference (null pointer)
TEST(PreconditionsTest, CheckNotNullNullReference) {
    int* ptr = nullptr;
    ASSERT_NOK_WITH_MSG(Preconditions::CheckNotNull(ptr), "check null failed");
}

// Test case: Test CheckNotNull with a null reference and a custom message
TEST(PreconditionsTest, CheckNotNullNullReferenceWithMessage) {
    int* ptr = nullptr;
    ASSERT_NOK_WITH_MSG(Preconditions::CheckNotNull(ptr, "Custom error: pointer is null"),
                        "Custom error: pointer is null");
}

// Test case: Test CheckArgument with a valid condition
TEST(PreconditionsTest, CheckArgumentValidCondition) {
    ASSERT_OK(Preconditions::CheckArgument(true));
}

// Test case: Test CheckArgument with an invalid condition
TEST(PreconditionsTest, CheckArgumentInvalidCondition) {
    ASSERT_NOK_WITH_MSG(Preconditions::CheckArgument(false), "invalid argument");
}

}  // namespace paimon::test
