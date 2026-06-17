/*
 * Copyright 2025-present Alibaba Inc.
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

// TagFilter - Filter expression for tag-based vector search.
// Supports nested AND/OR conditions with comparison operators.

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>
#include <vector>

namespace lumina::extensions { inline namespace experimental {

// TagValue represents a tag value, supporting int64_t, double, or string.
using TagValue = std::variant<int64_t, double, std::string>;
// TagValues represents a homogeneous collection of tag values for a single condition.
// Unlike std::vector<TagValue>, this enforces that all values share the same type
// (all int64_t, all double, or all string) at the type system level.
using TagValues = std::variant<std::vector<int64_t>, std::vector<double>, std::vector<std::string>>;

// Comparison operators for tag conditions.
enum class CompareOp : uint8_t {
    Eq,  // ==
    Ne,  // !=
    Gt,  // >
    Gte, // >=
    Lt,  // <
    Lte, // <=
    In   // in set
};

// Logical operators for combining conditions.
enum class LogicOp : uint8_t { And, Or };

// A single tag condition (leaf node in the filter tree).
struct TagCondition {
    std::string tagKey;           // Tag name, e.g., "color", "specs.cpu"
    CompareOp op = CompareOp::Eq; // Comparison operator
    TagValues values;             // Single value for comparisons, multiple for In

    TagCondition() = default;
    TagCondition(std::string key, CompareOp compareOp, TagValues&& vals)
        : tagKey(std::move(key))
        , op(compareOp)
        , values(std::move(vals))
    {
    }
};

// Convert CompareOp to string representation.
std::string_view CompareOpToString(CompareOp op) noexcept;

// Convert LogicOp to string representation.
std::string_view LogicOpToString(LogicOp op) noexcept;

/**
 * TagFilter represents a filter expression tree for tag-based filtering.
 * It can be a leaf node (single TagCondition) or a branch node (And/Or of children).
 *
 * This is a move-only type. Instances are typically constructed per search
 * request and passed by const reference through the search pipeline.
 *
 * Thread safety: TagFilter instances are not thread-safe. Each instance must
 * be used from a single thread, or external synchronization is required.
 *
 * Limitations:
 *   - AND conditions on the same tag dimension are not supported. For example,
 *     And(Eq("color", "red"), Eq("color", "blue")) is rejected at compile time.
 *     Use In("color", {"red", "blue"}) for "any of" semantics, or Or() to
 *     combine same-dimension conditions into separate sub-queries.
 *   - $in with an empty value set is rejected at compile time.
 *
 * Example usage:
 * @code
 * auto filter = TagFilter::And(
 *     TagFilter::Eq("color", std::string("blue")),
 *     TagFilter::Gt("price", 100),
 *     TagFilter::In("category", std::vector<std::string>{"shoes", "bags"})
 * );
 * @endcode
 */
class TagFilter
{
public:
    // Factory methods - Leaf nodes (single condition)

    // Create a condition node from a TagCondition.
    static TagFilter Condition(TagCondition cond);
    // Create an equality condition: tagKey == value
    static TagFilter Eq(std::string_view key, TagValue value);
    // Create a not-equal condition: tagKey != value
    static TagFilter Ne(std::string_view key, TagValue value);
    // Create a greater-than condition: tagKey > value
    static TagFilter Gt(std::string_view key, TagValue value);
    // Create a greater-than-or-equal condition: tagKey >= value
    static TagFilter Gte(std::string_view key, TagValue value);
    // Create a less-than condition: tagKey < value
    static TagFilter Lt(std::string_view key, TagValue value);
    // Create a less-than-or-equal condition: tagKey <= value
    static TagFilter Lte(std::string_view key, TagValue value);
    // Create an in-set condition: tagKey in {values}
    static TagFilter In(std::string_view key, TagValues values);

    // Factory methods - Branch nodes (logical combinations)

    // Create an AND combination of filters (variadic, move-only).
    template <typename... Args>
    static TagFilter And(Args&&... children)
    {
        static_assert((std::is_same_v<std::decay_t<Args>, TagFilter> && ...),
                      "All arguments to And() must be TagFilter");
        std::vector<TagFilter> vec;
        vec.reserve(sizeof...(children));
        (vec.push_back(std::forward<Args>(children)), ...);
        return And(std::move(vec));
    }

    // Create an OR combination of filters (variadic, move-only).
    template <typename... Args>
    static TagFilter Or(Args&&... children)
    {
        static_assert((std::is_same_v<std::decay_t<Args>, TagFilter> && ...),
                      "All arguments to Or() must be TagFilter");
        std::vector<TagFilter> vec;
        vec.reserve(sizeof...(children));
        (vec.push_back(std::forward<Args>(children)), ...);
        return Or(std::move(vec));
    }

    // Create an AND combination from a pre-built vector (move-only).
    static TagFilter And(std::vector<TagFilter> children);
    // Create an OR combination from a pre-built vector (move-only).
    static TagFilter Or(std::vector<TagFilter> children);

    // Accessors

    // Returns true if this is a leaf node (single condition).
    bool IsLeaf() const noexcept;
    // Returns true if this filter has not been moved from.
    bool IsValid() const noexcept;
    // Returns the logic operator (And/Or). Only valid for non-leaf nodes.
    LogicOp GetLogicOp() const noexcept;
    // Returns the tag condition. Only valid for leaf nodes.
    const TagCondition& GetCondition() const noexcept;
    // Returns the child filters. Only valid for non-leaf nodes.
    const std::vector<TagFilter>& GetChildren() const noexcept;

    // Serialize to JSON string (for debugging).
    std::string ToJson() const;

    // Move-only: destructor must be defined in .cpp where Node is complete.
    ~TagFilter();
    TagFilter(TagFilter&& other) noexcept;
    TagFilter& operator=(TagFilter&& other) noexcept;
    TagFilter(const TagFilter&) = delete;
    TagFilter& operator=(const TagFilter&) = delete;

private:
    struct Node;

    std::unique_ptr<Node> _node;

    explicit TagFilter(std::unique_ptr<Node> node);

    static TagFilter MakeCondition(std::string_view key, CompareOp op, TagValue value);
};

}} // namespace lumina::extensions::experimental
