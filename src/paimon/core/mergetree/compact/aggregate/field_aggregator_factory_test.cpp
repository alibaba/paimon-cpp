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

#include "paimon/core/mergetree/compact/aggregate/field_aggregator_factory.h"

#include <map>

#include "arrow/type_fwd.h"
#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(FieldAggregatorFactoryTest, TestSimple) {
    {
        ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldAggregator> agg,
                             FieldAggregatorFactory::CreateFieldAggregator("f0", arrow::int32(),
                                                                           "primary-key", options));
        ASSERT_TRUE(dynamic_cast<FieldPrimaryKeyAgg*>(agg.get()));
    }
    {
        ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FieldAggregator> agg,
            FieldAggregatorFactory::CreateFieldAggregator("f0", arrow::int32(), "sum", options));
        ASSERT_TRUE(dynamic_cast<FieldSumAgg*>(agg.get()));
    }
    {
        ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FieldAggregator> agg,
            FieldAggregatorFactory::CreateFieldAggregator("f0", arrow::int32(), "min", options));
        ASSERT_TRUE(dynamic_cast<FieldMinAgg*>(agg.get()));
    }
    {
        ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FieldAggregator> agg,
            FieldAggregatorFactory::CreateFieldAggregator("f0", arrow::int32(), "max", options));
        ASSERT_TRUE(dynamic_cast<FieldMaxAgg*>(agg.get()));
    }
    {
        ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldAggregator> agg,
                             FieldAggregatorFactory::CreateFieldAggregator("f0", arrow::boolean(),
                                                                           "bool_and", options));
        ASSERT_TRUE(dynamic_cast<FieldBoolAndAgg*>(agg.get()));
    }
    {
        ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldAggregator> agg,
                             FieldAggregatorFactory::CreateFieldAggregator("f0", arrow::boolean(),
                                                                           "bool_or", options));
        ASSERT_TRUE(dynamic_cast<FieldBoolOrAgg*>(agg.get()));
    }
    {
        ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldAggregator> agg,
                             FieldAggregatorFactory::CreateFieldAggregator(
                                 "f0", arrow::int32(), "last_non_null_value", options));
        ASSERT_TRUE(dynamic_cast<FieldLastNonNullValueAgg*>(agg.get()));
    }
    {
        ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldAggregator> agg,
                             FieldAggregatorFactory::CreateFieldAggregator(
                                 "f0", arrow::int32(), "first_non_null_value", options));
        ASSERT_TRUE(dynamic_cast<FieldFirstNonNullValueAgg*>(agg.get()));
    }
    {
        ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldAggregator> agg,
                             FieldAggregatorFactory::CreateFieldAggregator("f0", arrow::int32(),
                                                                           "last_value", options));
        ASSERT_TRUE(dynamic_cast<FieldLastValueAgg*>(agg.get()));
    }
    {
        ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldAggregator> agg,
                             FieldAggregatorFactory::CreateFieldAggregator("f0", arrow::int32(),
                                                                           "first_value", options));
        ASSERT_TRUE(dynamic_cast<FieldFirstValueAgg*>(agg.get()));
    }
    {
        // test ignore_retract is true
        ASSERT_OK_AND_ASSIGN(CoreOptions options,
                             CoreOptions::FromMap({{"fields.f0.ignore-retract", "true"}}));
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FieldAggregator> agg,
            FieldAggregatorFactory::CreateFieldAggregator("f0", arrow::int32(), "sum", options));
        auto ignore_retract_agg = dynamic_cast<FieldIgnoreRetractAgg*>(agg.get());
        ASSERT_TRUE(ignore_retract_agg);
        ASSERT_TRUE(dynamic_cast<FieldSumAgg*>(ignore_retract_agg->agg_.get()));
    }
    {
        // test non exist agg
        ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
        auto agg = FieldAggregatorFactory::CreateFieldAggregator("f0", arrow::int32(),
                                                                 "non-exist-agg", options);
        ASSERT_FALSE(agg.ok());
    }
}

TEST(FieldAggregatorFactoryTest, TestRemoveRecordOnDeleteConflictsWithIgnoreRetract) {
    ASSERT_OK_AND_ASSIGN(
        CoreOptions options,
        CoreOptions::FromMap({{Options::AGGREGATION_REMOVE_RECORD_ON_DELETE, "true"},
                              {"fields.f0.ignore-retract", "true"}}));
    ASSERT_NOK_WITH_MSG(
        FieldAggregatorFactory::CreateFieldAggregator("f0", arrow::int32(), "sum", options),
        "conflicting behavior");
}

}  // namespace paimon::test
