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

#include <cstddef>
#include <cstdint>
#include <lumina/distance/Metric.h>
#include <lumina/distance/MetricDistance.h>
#include <lumina/distance/encode_space/EncodedRowSource.h>
#include <lumina/distance/encode_space/EncodingTypes.h>
#include <span>
#include <type_traits>
#include <utility>

namespace lumina::dist {

// Encoded Distance Interface (CPO)
//
// This file defines the core abstraction for computing distances between a floating-point query
// and encoded (compressed/quantized) vectors.
//
// Workflow Overview:
// 1. Prepare: One-time processing of the query for a specific encoding (e.g., building
//    lookup tables for PQ or pre-calculating distance components). This amortizes the cost
//    of complex setup across many distance evaluations.
// 2. Evaluate: Use the prepared state to compute distances for many rows.
//    - BatchEval: Best for contiguous scans (dense layout).
//    - GatherEval: Best for graph/IVF traversal where candidate rows are scattered.
//
// Design Goals:
// - Performance: Enables SIMD/ISA-specific optimizations (like AVX-512 gather) via TagInvoke.
// - Decoupling: Uses `EncodedRowSource` to abstract memory layout (flat, strided, or aux-data)
//   away from the distance logic.
// - Stability: The API remains constant even if the underlying encoding format changes.

struct PrepareTag {
    /**
     * @brief Transforms a raw query into a "Prepared State" optimized for a specific encoding.
     *
     * Why use this?
     * For many encodings (like Product Quantization), calculating a distance requires expensive
     * per-query setup (e.g., pre-computing 256 distances to codebook centroids). 'Prepare'
     * ensures this work is done only once per search.
     *
     * @param m The Metric (e.g., MetricL2, MetricIP).
     * @param e The Encoding (e.g., SQ8, PQ).
     * @param q The raw floating-point query vector.
     * @param ctx Additional encoding-specific resources (e.g., codebooks, headers).
     * @return An opaque state object (S) to be passed to Eval/Gather functions.
     */
    template <class M, class E, class... Ctx>
        requires TagInvocable<PrepareTag, M, E, std::span<const float>, Ctx&&...>
    constexpr auto operator()(const M& m, const E& e, std::span<const float> q, Ctx&&... ctx) const
        noexcept(noexcept(TagInvoke(std::declval<PrepareTag>(), m, e, q, std::forward<Ctx>(ctx)...)))
            -> TagInvokeResult<PrepareTag, M, E, std::span<const float>, Ctx&&...>
    {
        return TagInvoke(*this, m, e, q, std::forward<Ctx>(ctx)...);
    }
};
inline constexpr PrepareTag Prepare {};

struct EvalEncodedTag {
    /**
     * @brief Computes distance for a single encoded row.
     *
     * Typically used within a loop when manual flow control is needed. However, for
     * performance-critical code, BatchEval or GatherEval are usually preferred as they
     * allow the implementation to use SIMD more effectively.
     *
     * @param s The prepared state from Prepare().
     * @param r A view to a single encoded row (e.g., encode_space::EncodedRow).
     */
    template <class M, class E, class S, class R>
        requires TagInvocable<EvalEncodedTag, M, E, const S&, const R&>
    constexpr auto operator()(const M& m, const E& e, const S& s, const R& r) const
        noexcept(noexcept(TagInvoke(std::declval<EvalEncodedTag>(), m, e, s, r)))
            -> TagInvokeResult<EvalEncodedTag, M, E, const S&, const R&>
    {
        return TagInvoke(*this, m, e, s, r);
    }
};
inline constexpr EvalEncodedTag EvalEncoded {};

struct BatchEvalEncodedTag {
    /**
     * @brief Computes distances for a contiguous block of encoded rows.
     *
     * Optimized for exhaustive scans. The implementation can assume rows are adjacent in memory,
     * allowing for efficient linear prefetching and unrolled SIMD processing.
     *
     * @param b Description of the contiguous batch (base pointer + count).
     * @param out Pointer to output float array (must hold at least b.n elements).
     */
    template <class M, class E, class S>
        requires TagInvocable<BatchEvalEncodedTag, M, E, const S&, const encode_space::EncodedBatch&, float*>
    constexpr void operator()(const M& m, const E& e, const S& s, const encode_space::EncodedBatch& b, float* out) const
        noexcept(noexcept(TagInvoke(std::declval<BatchEvalEncodedTag>(), m, e, s, b, out)))
    {
        TagInvoke(*this, m, e, s, b, out);
    }
};
inline constexpr BatchEvalEncodedTag BatchEvalEncoded {};

struct GatherEvalEncodedTag {
    /**
     * @brief Computes distances for a non-contiguous set of row IDs.
     *
     * The primary workhorse for graph-based or IVF search. Given a list of candidate IDs,
     * this function "gathers" the encoded data and computes distances.
     *
     * Performance Note: Specializations of this tag often use ISA-specific instructions
     * (e.g., VPGATHERDD) or software pipelining to hide memory latency during random access.
     *
     * @param data The data source (models EncodedRowSource) providing mapping from ID to memory.
     * @param rowIds The list of logical row IDs to evaluate.
     * @param results Output span for distances (must match rowIds size).
     */
    template <class M, class E, class S>
        requires TagInvocable<GatherEvalEncodedTag, M, E, const S&, const std::byte*, std::span<const uint64_t>,
                              std::span<float>>
    constexpr void operator()(const M& m, const E& e, const S& s, const std::byte* recordsBase,
                              std::span<const uint64_t> rowIds, std::span<float> results) const
        noexcept(noexcept(TagInvoke(std::declval<GatherEvalEncodedTag>(), m, e, s, recordsBase, rowIds, results)))
    {
        TagInvoke(*this, m, e, s, recordsBase, rowIds, results);
    }

    template <class M, class E, class S, class DataSource>
        requires(encode_space::EncodedRowSource<std::remove_cvref_t<DataSource>> &&
                 TagInvocable<GatherEvalEncodedTag, M, E, const S&, const DataSource&, std::span<const uint64_t>,
                              std::span<float>>)
    constexpr void operator()(const M& m, const E& e, const S& s, const DataSource& data,
                              std::span<const uint64_t> rowIds, std::span<float> results) const
        noexcept(noexcept(TagInvoke(std::declval<GatherEvalEncodedTag>(), m, e, s, data, rowIds, results)))
    {
        TagInvoke(*this, m, e, s, data, rowIds, results);
    }

    template <class M, class E, class S, class DataSource>
        requires(encode_space::EncodedRowSource<std::remove_cvref_t<DataSource>> &&
                 !TagInvocable<GatherEvalEncodedTag, M, E, const S&, const DataSource&, std::span<const uint64_t>,
                               std::span<float>>)
    constexpr void operator()(const M& m, const E& e, const S& s, const DataSource& data,
                              std::span<const uint64_t> rowIds, std::span<float> results) const
        noexcept(noexcept(encode_space::GetEncodedRow(data, uint64_t {})) && noexcept(
            EvalEncoded(m, e, s, encode_space::GetEncodedRow(data, uint64_t {}))))
    {
        for (std::size_t i = 0; i < rowIds.size(); ++i) {
            const auto row = encode_space::GetEncodedRow(data, rowIds[i]);
            results[i] = EvalEncoded(m, e, s, row);
        }
    }
};
inline constexpr GatherEvalEncodedTag GatherEvalEncoded {};

struct GatherEvalEncodedWithLowerBoundsTag {
    /**
     * @brief Evaluates distances AND returns a lower-bound estimate for each row.
     *
     * This is an optimization for multi-stage search (e.g., DiskANN).
     *
     * Lower bounds (D_lb) guarantee that D_true >= D_lb. In search algorithms, if D_lb is
     * already greater than the current search radius, we can skip further processing of this
     * candidate.
     *
     * Note: Not all encodings support lower bounds. If unsupported, use the standard GatherEvalEncoded.
     *
     * @param results Output span for the (potentially approximate) distances.
     * @param lowerBounds Output span for the lower-bound estimates.
     */
    template <class M, class E, class S>
        requires TagInvocable<GatherEvalEncodedWithLowerBoundsTag, M, E, const S&, const std::byte*,
                              std::span<const uint64_t>, std::span<float>, std::span<float>>
    constexpr void operator()(const M& m, const E& e, const S& s, const std::byte* recordsBase,
                              std::span<const uint64_t> rowIds, std::span<float> results,
                              std::span<float> lowerBounds) const
        noexcept(noexcept(TagInvoke(std::declval<GatherEvalEncodedWithLowerBoundsTag>(), m, e, s, recordsBase, rowIds,
                                    results, lowerBounds)))
    {
        TagInvoke(*this, m, e, s, recordsBase, rowIds, results, lowerBounds);
    }

    template <class M, class E, class S, class DataSource>
        requires(encode_space::EncodedRowSource<std::remove_cvref_t<DataSource>> &&
                 TagInvocable<GatherEvalEncodedWithLowerBoundsTag, M, E, const S&, const DataSource&,
                              std::span<const uint64_t>, std::span<float>, std::span<float>>)
    constexpr void operator()(const M& m, const E& e, const S& s, const DataSource& data,
                              std::span<const uint64_t> rowIds, std::span<float> results,
                              std::span<float> lowerBounds) const
        noexcept(noexcept(TagInvoke(std::declval<GatherEvalEncodedWithLowerBoundsTag>(), m, e, s, data, rowIds, results,
                                    lowerBounds)))
    {
        TagInvoke(*this, m, e, s, data, rowIds, results, lowerBounds);
    }
};
inline constexpr GatherEvalEncodedWithLowerBoundsTag GatherEvalEncodedWithLowerBounds {};

} // namespace lumina::dist
