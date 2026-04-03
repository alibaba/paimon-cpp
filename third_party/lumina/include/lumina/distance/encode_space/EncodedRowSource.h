// Encoded row source CPO: map row ids to encoded row views.
#pragma once

#include <cstddef>
#include <cstdint>
#include <lumina/distance/encode_space/Encode.h>
#include <lumina/distance/encode_space/EncodingTypes.h>

namespace lumina::dist::encode_space {

struct GetEncodedRowTag {
    template <class DataSource>
        requires TagInvocable<GetEncodedRowTag, const DataSource&, uint64_t>
    constexpr auto operator()(const DataSource& data, uint64_t rowId) const
        noexcept(noexcept(TagInvoke(std::declval<GetEncodedRowTag>(), data, rowId)))
            -> TagInvokeResult<GetEncodedRowTag, const DataSource&, uint64_t>
    {
        return TagInvoke(*this, data, rowId);
    }
};

inline constexpr GetEncodedRowTag GetEncodedRow {};

template <class DataSource>
concept EncodedRowSource = requires(const DataSource& data, uint64_t rowId) {
                               {
                                   GetEncodedRow(data, rowId)
                                   } -> std::convertible_to<EncodedRow>;
                           };

// A simple encoded row source for strided row storage.
//
// Typical usage:
// - `base` points to row 0 payload. Row i payload is `base + i * strideBytes`.
// - `rowBytes` is the actual bytes for one row; `strideBytes` may be larger when rows are padded/aligned.
// - `auxBase` is optional. If `auxStrideBytes == 0`, aux is constant (shared) for all rows.
//   Consumers must not assume unique memory addresses for aux data in this case.
struct StridedEncodedRows {
    const std::byte* base {nullptr};
    uint64_t strideBytes {0};
    uint64_t rowBytes {0};

    const std::byte* auxBase {nullptr};
    uint64_t auxStrideBytes {0};
    uint64_t auxBytes {0};
};

inline EncodedRow TagInvoke(GetEncodedRowTag, const StridedEncodedRows& data, uint64_t rowId) noexcept
{
    EncodedRow row {};
    row.data = data.base + rowId * data.strideBytes;
    row.bytes = data.rowBytes;
    if (data.auxBase != nullptr && data.auxBytes != 0) {
        row.aux = (data.auxStrideBytes == 0) ? data.auxBase : (data.auxBase + rowId * data.auxStrideBytes);
        row.auxBytes = data.auxBytes;
    }
    return row;
}

} // namespace lumina::dist::encode_space
