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

#include "paimon/predicate/predicate.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_nested.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/defs.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/function.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace arrow {
class Array;
}  // namespace arrow

namespace paimon::test {
class PredicateTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}
    struct FieldStats {
        FieldStats(const std::optional<int64_t>& _min_value,
                   const std::optional<int64_t>& _max_value, int64_t _null_count)
            : min_value(_min_value), max_value(_max_value), null_count(_null_count) {}
        std::optional<int64_t> min_value;
        std::optional<int64_t> max_value;
        int64_t null_count;
    };

    bool StatsCheck(const PredicateFilter& predicate, int64_t row_count,
                    const std::vector<FieldStats>& field_stats) const {
        auto pool = GetDefaultPool();
        BinaryRow min_row(/*arity=*/field_stats.size());
        BinaryRowWriter min_row_writer(&min_row, 0, pool.get());
        BinaryRow max_row(/*arity=*/field_stats.size());
        BinaryRowWriter max_row_writer(&max_row, 0, pool.get());
        std::vector<int64_t> nulls;
        arrow::FieldVector fields;
        for (uint32_t i = 0; i < field_stats.size(); i++) {
            const auto& stats = field_stats[i];
            if (stats.min_value == std::nullopt) {
                min_row_writer.SetNullAt(i);
            } else {
                min_row_writer.WriteLong(i, stats.min_value.value());
            }
            if (stats.max_value == std::nullopt) {
                max_row_writer.SetNullAt(i);
            } else {
                max_row_writer.WriteLong(i, stats.max_value.value());
            }
            nulls.emplace_back(stats.null_count);
            fields.emplace_back(arrow::field("f" + std::to_string(i), arrow::int64()));
        }
        min_row_writer.Complete();
        max_row_writer.Complete();
        auto null_counts = BinaryArray::FromLongArray(nulls, pool.get());
        auto arrow_schema = arrow::schema(fields);
        EXPECT_OK_AND_ASSIGN(
            auto ret, predicate.Test(arrow_schema, row_count, min_row, max_row, null_counts));
        return ret;
    }

    struct StringFieldStats {
        StringFieldStats(const std::optional<std::string>& _min_value,
                         const std::optional<std::string>& _max_value, int64_t _null_count)
            : min_value(_min_value), max_value(_max_value), null_count(_null_count) {}
        std::optional<std::string> min_value;
        std::optional<std::string> max_value;
        int64_t null_count;
    };

    bool StringStatsCheck(const PredicateFilter& predicate, int64_t row_count,
                          const std::vector<StringFieldStats>& field_stats) const {
        auto pool = GetDefaultPool();
        BinaryRow min_row(/*arity=*/field_stats.size());
        BinaryRowWriter min_row_writer(&min_row, 0, pool.get());
        BinaryRow max_row(/*arity=*/field_stats.size());
        BinaryRowWriter max_row_writer(&max_row, 0, pool.get());
        std::vector<int64_t> nulls;
        arrow::FieldVector fields;
        for (uint32_t i = 0; i < field_stats.size(); i++) {
            const auto& stats = field_stats[i];
            if (stats.min_value == std::nullopt) {
                min_row_writer.SetNullAt(i);
            } else {
                min_row_writer.WriteString(
                    i, BinaryString::FromString(stats.min_value.value(), pool.get()));
            }
            if (stats.max_value == std::nullopt) {
                max_row_writer.SetNullAt(i);
            } else {
                max_row_writer.WriteString(
                    i, BinaryString::FromString(stats.max_value.value(), pool.get()));
            }
            nulls.emplace_back(stats.null_count);
            fields.emplace_back(arrow::field("f" + std::to_string(i), arrow::utf8()));
        }
        min_row_writer.Complete();
        max_row_writer.Complete();
        auto null_counts = BinaryArray::FromLongArray(nulls, pool.get());
        auto arrow_schema = arrow::schema(fields);
        EXPECT_OK_AND_ASSIGN(
            auto ret, predicate.Test(arrow_schema, row_count, min_row, max_row, null_counts));
        return ret;
    }

    BinaryRow CreateBigIntRow(const std::vector<std::optional<int64_t>>& value) const {
        auto pool = GetDefaultPool();
        BinaryRow row(/*arity=*/value.size());
        BinaryRowWriter row_writer(&row, 0, pool.get());
        for (size_t i = 0; i < value.size(); ++i) {
            if (value[i] == std::nullopt) {
                row_writer.SetNullAt(i);
            } else {
                row_writer.WriteLong(i, value[i].value());
            }
        }
        row_writer.Complete();
        return row;
    }

    BinaryRow CreateStringRow(const std::vector<std::optional<std::string>>& value) const {
        auto pool = GetDefaultPool();
        BinaryRow row(/*arity=*/value.size());
        BinaryRowWriter row_writer(&row, 0, pool.get());
        for (size_t i = 0; i < value.size(); ++i) {
            if (value[i] == std::nullopt) {
                row_writer.SetNullAt(i);
            } else {
                row_writer.WriteString(i, BinaryString::FromString(value[i].value(), pool.get()));
            }
        }
        row_writer.Complete();
        return row;
    }
};

TEST_F(PredicateTest, TestInvalidFieldIndex) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f0",
                                                  FieldType::BIGINT, Literal(5l));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);

    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, 5, null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});
    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    // with array
    ASSERT_NOK_WITH_MSG(predicate->Test(*struct_array),
                        "field index 2 exceed field count 2 in struct array");

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_NOK_WITH_MSG(predicate->Test(arrow_schema, CreateBigIntRow({4})),
                        "field index 2 exceed field count 1 in row");
}

TEST_F(PredicateTest, TestEqual) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                  FieldType::BIGINT, Literal(5l));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, 5, null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({0, 1, 0}));

    ASSERT_EQ(*predicate->Negate(),
              *PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                          Literal(5l)));

    ASSERT_FALSE(*predicate->Negate() ==
                 *PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0",
                                             FieldType::BIGINT, Literal(10l)));
    ASSERT_FALSE(*predicate->Negate() == *PredicateBuilder::Equal(/*field_index=*/0,
                                                                  /*field_name=*/"f0",
                                                                  FieldType::BIGINT, Literal(10l)));
    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({5})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());

    // with stats
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 6ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestEqualNull) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                  FieldType::BIGINT, Literal(FieldType::BIGINT));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());

    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestNotEqual) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0",
                                                     FieldType::BIGINT, Literal(5l));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, 5, null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({1, 0, 0}));

    auto predicate_negate = std::dynamic_pointer_cast<PredicateFilter>(predicate->Negate());
    ASSERT_EQ(*predicate_negate, *PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                          FieldType::BIGINT, Literal(5l)));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({5})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    ASSERT_TRUE(predicate_negate->Test(arrow_schema, CreateBigIntRow({5})).value());

    // with stats
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 6ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(5ll, 5ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestNotEqualNull) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0",
                                                     FieldType::BIGINT, Literal(FieldType::BIGINT));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0}));
    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());

    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestGreater) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"f0",
                                                        FieldType::BIGINT, Literal(5l));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, 5, 6, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1, 0])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0, 1, 0}));

    ASSERT_EQ(*predicate->Negate(),
              *PredicateBuilder::LessOrEqual(/*field_index=*/0, /*field_name=*/"f0",
                                             FieldType::BIGINT, Literal(5l)));
    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({5})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({6})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());

    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 4ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 6ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestGreaterNull) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::GreaterThan(
        /*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT, Literal(FieldType::BIGINT));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 4ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestGreaterOrEqual) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::GreaterOrEqual(
        /*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT, Literal(5l));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    ASSERT_EQ(predicate->GetFunction().ToString(), "GreaterOrEqual");
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, 5, 6, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1, 0])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({0, 1, 1, 0}));

    ASSERT_EQ(*predicate->Negate(),
              *PredicateBuilder::LessThan(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                          Literal(5l)));
    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({5})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({6})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());

    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 4ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 6ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestGreaterOrEqualNull) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::GreaterOrEqual(
        /*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT, Literal(FieldType::BIGINT));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 4ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestLess) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::LessThan(/*field_index=*/0, /*field_name=*/"f0",
                                                     FieldType::BIGINT, Literal(5l));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, 5, 6, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1, 0])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({1, 0, 0, 0}));

    ASSERT_EQ(*predicate->Negate(),
              *PredicateBuilder::GreaterOrEqual(
                  /*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT, Literal(5l)));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({5})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({6})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(5ll, 7ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(4ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestLessNull) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::LessThan(/*field_index=*/0, /*field_name=*/"f0",
                                                     FieldType::BIGINT, Literal(FieldType::BIGINT));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestLessOrEqual) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::LessOrEqual(/*field_index=*/0, /*field_name=*/"f0",
                                                        FieldType::BIGINT, Literal(5l));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, 5, 6, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1, 0])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({1, 1, 0, 0}));

    ASSERT_EQ(*predicate->Negate(),
              *PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"f0",
                                             FieldType::BIGINT, Literal(5l)));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({5})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({6})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(5ll, 7ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(4ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestLessOrEqualNull) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::LessOrEqual(
        /*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT, Literal(FieldType::BIGINT));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestIsNull) {
    auto bigint_type = arrow::int64();
    auto predicate_base =
        PredicateBuilder::IsNull(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT);
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({0, 1}));

    ASSERT_EQ(*predicate->Negate(), *PredicateBuilder::IsNotNull(
                                        /*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(5ll, 7ll, 1ll)}));
}

TEST_F(PredicateTest, TestIsNotNull) {
    auto bigint_type = arrow::int64();
    auto predicate_base =
        PredicateBuilder::IsNotNull(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT);
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({1, 0}));

    ASSERT_EQ(*predicate->Negate(),
              *PredicateBuilder::IsNull(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(5ll, 7ll, 1ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(std::nullopt, std::nullopt, 3ll)}));
}

TEST_F(PredicateTest, TestIn) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::In(/*field_index=*/0, /*field_name=*/"f0",
                                               FieldType::BIGINT, {Literal(1l), Literal(3l)});
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([1, 2, 3, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1, 0])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({1, 0, 1, 0}));

    ASSERT_EQ(*predicate->Negate(),
              *PredicateBuilder::NotIn(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                       {Literal(1l), Literal(3l)}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({1})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({2})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({3})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestInNull) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::In(
        /*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
        {Literal(1l), Literal(FieldType::BIGINT), Literal(3l)});
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([1, 2, 3, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1, 0])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({1, 0, 1, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({1})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({2})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({3})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestNotIn) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::NotIn(/*field_index=*/0, /*field_name=*/"f0",
                                                  FieldType::BIGINT, {Literal(1l), Literal(3l)});
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([1, 2, 3, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1, 0])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({0, 1, 0, 0}));

    ASSERT_EQ(*predicate->Negate(),
              *PredicateBuilder::In(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                    {Literal(1l), Literal(3l)}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({1})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({2})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({3})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(1ll, 1ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(3ll, 3ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(1ll, 3ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestNotInNull) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::NotIn(
        /*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
        {Literal(1l), Literal(FieldType::BIGINT), Literal(3l)});
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([1, 2, 3, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1, 0])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0, 0, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({1})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({2})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({3})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(1ll, 1ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(3ll, 3ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(1ll, 3ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestLargeIn) {
    auto bigint_type = arrow::int64();
    std::vector<Literal> literals;
    literals.reserve(30);
    literals.emplace_back(1l);
    literals.emplace_back(3l);
    for (int64_t i = 10; i < 30; i++) {
        literals.emplace_back(i);
    }
    auto predicate_base =
        PredicateBuilder::In(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT, literals);
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([1, 2, 3, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1, 0])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({1, 0, 1, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({1})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({2})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({3})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(29ll, 32ll, 0ll)}));
}

TEST_F(PredicateTest, TestLargeInNull) {
    auto bigint_type = arrow::int64();
    std::vector<Literal> literals;
    literals.reserve(30);
    literals.emplace_back(1l);
    literals.emplace_back(FieldType::BIGINT);
    literals.emplace_back(3l);
    for (int64_t i = 10; i < 30; i++) {
        literals.emplace_back(i);
    }
    auto predicate_base =
        PredicateBuilder::In(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT, literals);
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([1, 2, 3, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1, 0])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({1, 0, 1, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({1})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({2})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({3})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(29ll, 32ll, 0ll)}));
}

TEST_F(PredicateTest, TestLargeNotIn) {
    auto bigint_type = arrow::int64();
    std::vector<Literal> literals;
    literals.reserve(30);
    literals.emplace_back(1l);
    literals.emplace_back(3l);
    for (int64_t i = 10; i < 30; i++) {
        literals.emplace_back(i);
    }
    auto predicate_base = PredicateBuilder::NotIn(/*field_index=*/0, /*field_name=*/"f0",
                                                  FieldType::BIGINT, literals);
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([1, 2, 3, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1, 0])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({0, 1, 0, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({1})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({2})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({3})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(1ll, 1ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(3ll, 3ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(1ll, 3ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(29ll, 32ll, 0ll)}));
}

TEST_F(PredicateTest, TestLargeNotInNull) {
    auto bigint_type = arrow::int64();
    std::vector<Literal> literals;
    literals.reserve(30);
    literals.emplace_back(1l);
    literals.emplace_back(FieldType::BIGINT);
    literals.emplace_back(3l);
    for (int64_t i = 10; i < 30; i++) {
        literals.emplace_back(i);
    }
    auto predicate_base = PredicateBuilder::NotIn(/*field_index=*/0, /*field_name=*/"f0",
                                                  FieldType::BIGINT, literals);
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([1, 2, 3, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1, 0])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0, 0, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({1})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({2})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({3})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(1ll, 1ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(3ll, 3ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(1ll, 3ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(29ll, 32ll, 0ll)}));
}

TEST_F(PredicateTest, TestAnd) {
    auto bigint_type = arrow::int64();
    ASSERT_OK_AND_ASSIGN(
        auto predicate_base,
        PredicateBuilder::And({PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                       FieldType::BIGINT, Literal(3l)),
                               PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                       FieldType::BIGINT, Literal(5l))}));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, 3, 3, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([5, 6, 5, 5])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0, 1, 0}));

    ASSERT_OK_AND_ASSIGN(
        auto negate_predicate,
        PredicateBuilder::Or({PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0",
                                                         FieldType::BIGINT, Literal(3l)),
                              PredicateBuilder::NotEqual(/*field_index=*/1, /*field_name=*/"f1",
                                                         FieldType::BIGINT, Literal(5l))}));
    ASSERT_EQ(*predicate->Negate(), *negate_predicate);

    // with internal row
    auto arrow_schema = arrow::schema(
        arrow::FieldVector({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({4, 5})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({3, 6})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({3, 5})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt, 5})).value());
    // with stats
    ASSERT_TRUE(
        StatsCheck(*predicate, 3ll, {FieldStats(3ll, 6ll, 0ll), FieldStats(4ll, 6ll, 0ll)}));
    ASSERT_FALSE(
        StatsCheck(*predicate, 3ll, {FieldStats(3ll, 6ll, 0ll), FieldStats(6ll, 8ll, 0ll)}));
    ASSERT_FALSE(
        StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll), FieldStats(4ll, 6ll, 0ll)}));
}

TEST_F(PredicateTest, TestOr) {
    auto bigint_type = arrow::int64();
    ASSERT_OK_AND_ASSIGN(
        auto predicate_base,
        PredicateBuilder::Or({PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                      FieldType::BIGINT, Literal(3l)),
                              PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                      FieldType::BIGINT, Literal(5l))}));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, 3, 3, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([6, 6, 5, 5])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({0, 1, 1, 1}));

    ASSERT_OK_AND_ASSIGN(
        auto negate_predicate,
        PredicateBuilder::And({PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0",
                                                          FieldType::BIGINT, Literal(3l)),
                               PredicateBuilder::NotEqual(/*field_index=*/1, /*field_name=*/"f1",
                                                          FieldType::BIGINT, Literal(5l))}));
    ASSERT_EQ(*predicate->Negate(), *negate_predicate);

    // with internal row
    auto arrow_schema = arrow::schema(
        arrow::FieldVector({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({4, 6})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({3, 6})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({3, 5})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt, 5})).value());
    // with stats
    ASSERT_TRUE(
        StatsCheck(*predicate, 3ll, {FieldStats(3ll, 6ll, 0ll), FieldStats(4ll, 6ll, 0ll)}));
    ASSERT_TRUE(
        StatsCheck(*predicate, 3ll, {FieldStats(3ll, 6ll, 0ll), FieldStats(6ll, 8ll, 0ll)}));
    ASSERT_FALSE(
        StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll), FieldStats(8ll, 10ll, 0ll)}));
}

TEST_F(PredicateTest, TestBetween) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::Between(/*field_index=*/0, /*field_name=*/"f0",
                                                    FieldType::BIGINT, Literal(3l), Literal(5l));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 4, 5, 100, 1, null])")
                  .ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([1, 2, 3, 4, 5, 6])")
                  .ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({1, 1, 1, 0, 0, 0}));

    auto less_than = PredicateBuilder::LessThan(/*field_index=*/0, /*field_name=*/"f0",
                                                FieldType::BIGINT, Literal(3l));
    auto greater_than = PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"f0",
                                                      FieldType::BIGINT, Literal(5l));
    ASSERT_OK_AND_ASSIGN(auto or_predicate, PredicateBuilder::Or({less_than, greater_than}));

    auto predicate_negate = std::dynamic_pointer_cast<PredicateFilter>(predicate->Negate());
    ASSERT_EQ(*predicate_negate, *or_predicate);
    ASSERT_FALSE(*predicate_negate == *predicate_base);

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({1})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    ASSERT_TRUE(predicate_negate->Test(arrow_schema, CreateBigIntRow({1})).value());

    // with stats
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(1ll, 10ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(3ll, 4ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestBetweenNull) {
    auto bigint_type = arrow::int64();
    auto predicate_base =
        PredicateBuilder::Between(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                  Literal(FieldType::BIGINT), Literal(5l));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([1, 2])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());

    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(1ll, 10ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestStartsWith) {
    auto string_type = arrow::utf8();
    ASSERT_OK_AND_ASSIGN(
        const auto predicate_base,
        PredicateBuilder::StartsWith(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                     Literal(FieldType::STRING, "aab", 3)));
    const auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(string_type,
                                                        R"(["ccddee", "bbccdd", "aabbcc", null])")
                  .ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(
                  string_type, R"(["gghhii", "ffgghh", "eeffgg", "ddeeff"])")
                  .ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", string_type), arrow::field("f1", string_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0, 1, 0}));

    ASSERT_EQ(predicate->Negate(), nullptr);
    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", string_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"ccddee"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"bbccdd"})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"aabbcc"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({std::nullopt})).value());

    // with stats
    // min="aaa", max="aaz" covers prefix "aab"
    ASSERT_TRUE(StringStatsCheck(*predicate, 3ll, {StringFieldStats("aaa", "aaz", 0ll)}));
    // min="aab", max="aab" exact match prefix
    ASSERT_TRUE(StringStatsCheck(*predicate, 3ll, {StringFieldStats("aab", "aab", 0ll)}));
    // min="aabxxx", max="aabzzz" both start with "aab"
    ASSERT_TRUE(StringStatsCheck(*predicate, 3ll, {StringFieldStats("aabxxx", "aabzzz", 0ll)}));
    // min="bbb", max="ccc" entirely above prefix "aab"
    ASSERT_FALSE(StringStatsCheck(*predicate, 3ll, {StringFieldStats("bbb", "ccc", 0ll)}));
    // min="aaa", max="aaa" entirely below prefix "aab"
    ASSERT_FALSE(StringStatsCheck(*predicate, 3ll, {StringFieldStats("aaa", "aaa", 0ll)}));
    // all nulls
    ASSERT_FALSE(
        StringStatsCheck(*predicate, 1ll, {StringFieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestStartsWithNull) {
    const auto string_type = arrow::utf8();
    ASSERT_OK_AND_ASSIGN(
        const auto predicate_base,
        PredicateBuilder::StartsWith(
            /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING, Literal(FieldType::STRING)));
    const auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(string_type, R"(["bbccdd", null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(string_type, R"(["ffgghh", "ccddee"])")
                  .ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", string_type), arrow::field("f1", string_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", string_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"bbccdd"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({std::nullopt})).value());

    // with stats: null literal always returns false
    ASSERT_FALSE(StringStatsCheck(*predicate, 3ll, {StringFieldStats("aaa", "zzz", 0ll)}));
    ASSERT_FALSE(
        StringStatsCheck(*predicate, 1ll, {StringFieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestEndsWith) {
    auto string_type = arrow::utf8();
    ASSERT_OK_AND_ASSIGN(
        const auto predicate_base,
        PredicateBuilder::EndsWith(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                   Literal(FieldType::STRING, "bcc", 3)));
    const auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(string_type,
                                                        R"(["ccddee", "bbccdd", "aabbcc", null])")
                  .ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(
                  string_type, R"(["gghhii", "ffgghh", "eeffgg", "ddeeff"])")
                  .ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", string_type), arrow::field("f1", string_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0, 1, 0}));

    ASSERT_EQ(predicate->Negate(), nullptr);
    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", string_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"ccddee"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"bbccdd"})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"aabbcc"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({std::nullopt})).value());

    // with stats: EndsWith base class always returns true for non-null stats
    ASSERT_TRUE(StringStatsCheck(*predicate, 3ll, {StringFieldStats("aaa", "zzz", 0ll)}));
    ASSERT_TRUE(StringStatsCheck(*predicate, 3ll, {StringFieldStats("xxx", "yyy", 0ll)}));
    // all nulls
    ASSERT_FALSE(
        StringStatsCheck(*predicate, 1ll, {StringFieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestEndsWithNull) {
    const auto string_type = arrow::utf8();
    ASSERT_OK_AND_ASSIGN(
        const auto predicate_base,
        PredicateBuilder::EndsWith(
            /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING, Literal(FieldType::STRING)));
    const auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(string_type, R"(["bbccdd", null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(string_type, R"(["ffgghh", "ccddee"])")
                  .ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", string_type), arrow::field("f1", string_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", string_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"bbccdd"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({std::nullopt})).value());

    // with stats: null literal always returns false
    ASSERT_FALSE(StringStatsCheck(*predicate, 3ll, {StringFieldStats("aaa", "zzz", 0ll)}));
    ASSERT_FALSE(
        StringStatsCheck(*predicate, 1ll, {StringFieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestContains) {
    auto string_type = arrow::utf8();
    ASSERT_OK_AND_ASSIGN(
        const auto predicate_base,
        PredicateBuilder::Contains(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                   Literal(FieldType::STRING, "cde", 3)));
    const auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(string_type,
                                                        R"(["ghijkl", "defghi", "abcdef", null])")
                  .ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(
                  string_type, R"(["stuvwx", "pqrstu", "mnopqr", "jklmno"])")
                  .ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", string_type), arrow::field("f1", string_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0, 1, 0}));

    ASSERT_EQ(predicate->Negate(), nullptr);
    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", string_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"ghijkl"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"defghi"})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"abcdef"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({std::nullopt})).value());

    // with stats: Contains base class always returns true for non-null stats
    ASSERT_TRUE(StringStatsCheck(*predicate, 3ll, {StringFieldStats("aaa", "zzz", 0ll)}));
    ASSERT_TRUE(StringStatsCheck(*predicate, 3ll, {StringFieldStats("xxx", "yyy", 0ll)}));
    // all nulls
    ASSERT_FALSE(
        StringStatsCheck(*predicate, 1ll, {StringFieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestContainsNull) {
    const auto string_type = arrow::utf8();
    ASSERT_OK_AND_ASSIGN(
        const auto predicate_base,
        PredicateBuilder::Contains(
            /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING, Literal(FieldType::STRING)));
    const auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(string_type, R"(["defghi", null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(string_type, R"(["pqrstu", "jklmno"])")
                  .ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", string_type), arrow::field("f1", string_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, predicate->Test(*struct_array));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", string_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"defghi"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({std::nullopt})).value());

    // with stats: null literal always returns false
    ASSERT_FALSE(StringStatsCheck(*predicate, 3ll, {StringFieldStats("aaa", "zzz", 0ll)}));
    ASSERT_FALSE(
        StringStatsCheck(*predicate, 1ll, {StringFieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestLike) {
    ASSERT_OK_AND_ASSIGN(auto predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "a.c", 3)));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    ASSERT_EQ(predicate->Negate(), nullptr);
    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", arrow::utf8())}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"abc"})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"a.c"})).value());

    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "a.*d", 4)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"abcd"})).value());

    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "%c.e", 4)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"abcde"})).value());

    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "a\\_c", 4)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"a-c"})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"a_c"})).value());

    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "start%", 6)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"startX"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"not_startX"})).value());

    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "%middle%", 8)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"xxmiddleyy"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"xxmidxdleyy"})).value());

    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "%end", 4)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"xxend"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"xxendyy"})).value());

    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "equal", 5)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"equal"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"equalxx"})).value());

    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "st_rt%", 6)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"startxx"})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"stbrtxx"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"xxstbrtxx"})).value());

    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "abc%def%", 8)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"abchahadefxx"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"abchahadafxx"})).value());

    // with stats: Like base class always returns true for non-null stats
    ASSERT_TRUE(StringStatsCheck(*predicate, 3ll, {StringFieldStats("aaa", "zzz", 0ll)}));
    // all nulls
    ASSERT_FALSE(
        StringStatsCheck(*predicate, 1ll, {StringFieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestCompound) {
    ASSERT_OK_AND_ASSIGN(
        const auto startswith_predicate,
        PredicateBuilder::StartsWith(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                     Literal(FieldType::STRING, "aab", 3)));
    ASSERT_OK_AND_ASSIGN(
        const auto endswith_predicate,
        PredicateBuilder::EndsWith(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                   Literal(FieldType::STRING, "bcc", 3)));
    ASSERT_OK_AND_ASSIGN(const auto compound_predicate,
                         PredicateBuilder::And({startswith_predicate, endswith_predicate}));
    ASSERT_NOK_WITH_MSG(
        PredicateBuilder::Not(compound_predicate),
        "Could not construct A NOT predicate from And([StartsWith(f0, aab), EndsWith(f0, bcc)])");

    // with stats: And of StartsWith + EndsWith
    auto compound_filter = std::dynamic_pointer_cast<PredicateFilter>(compound_predicate);
    ASSERT_TRUE(compound_filter);
    // min="aab", max="aaz" covers StartsWith("aab"), EndsWith("bcc") returns true by default
    ASSERT_TRUE(StringStatsCheck(*compound_filter, 3ll, {StringFieldStats("aab", "aaz", 0ll)}));
    // min="bbb", max="ccc" does not cover StartsWith("aab")
    ASSERT_FALSE(StringStatsCheck(*compound_filter, 3ll, {StringFieldStats("bbb", "ccc", 0ll)}));
    // all nulls
    ASSERT_FALSE(StringStatsCheck(*compound_filter, 1ll,
                                  {StringFieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestPredicateToString) {
    {
        auto predicate = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                 FieldType::BIGINT, Literal(5l));
        ASSERT_EQ(predicate->ToString(), "Equal(f0, 5)");
    }
    {
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"f0",
                                                       FieldType::BIGINT, Literal(5l));
        ASSERT_EQ(predicate->ToString(), "GreaterThan(f0, 5)");
    }
    {
        auto predicate =
            PredicateBuilder::IsNotNull(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT);
        ASSERT_EQ(predicate->ToString(), "IsNotNull(f0)");
    }
    {
        std::vector<Literal> literals;
        literals.reserve(30);
        for (int64_t i = 1; i <= 21; i++) {
            literals.emplace_back(i);
        }
        auto predicate = PredicateBuilder::In(/*field_index=*/0, /*field_name=*/"f0",
                                              FieldType::BIGINT, literals);
        ASSERT_TRUE(predicate);
        ASSERT_EQ(
            predicate->ToString(),
            "In(f0, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21])");
    }
    {
        std::vector<Literal> literals;
        literals.reserve(30);
        for (int64_t i = 1; i <= 21; i++) {
            literals.emplace_back(i);
        }
        auto predicate = PredicateBuilder::NotIn(/*field_index=*/0, /*field_name=*/"f0",
                                                 FieldType::BIGINT, literals);
        ASSERT_TRUE(predicate);
        ASSERT_EQ(predicate->ToString(),
                  "NotIn(f0, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, "
                  "20, 21])");
    }

    {
        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::And({PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                           FieldType::BIGINT, Literal(3l)),
                                   PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                           FieldType::BIGINT, Literal(5l))}));
        ASSERT_EQ(predicate->ToString(), "And([Equal(f0, 3), Equal(f1, 5)])");
    }
    {
        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::Or({PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                          FieldType::BIGINT, Literal(3l)),
                                  PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                          FieldType::BIGINT, Literal(5l))}));
        ASSERT_EQ(predicate->ToString(), "Or([Equal(f0, 3), Equal(f1, 5)])");
    }
}

TEST_F(PredicateTest, TestBuildAndOr) {
    {
        // literals cannot be empty
        ASSERT_NOK(PredicateBuilder::Or({}));
    }
    {
        // literals cannot be empty
        ASSERT_NOK(PredicateBuilder::And({}));
    }
}
}  // namespace paimon::test
