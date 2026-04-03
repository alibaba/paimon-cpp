
#pragma once

#include <cstdint>
#include <lumina/core/NoCopyable.h>
#include <lumina/core/Result.h>
#include <lumina/core/Status.h>
#include <lumina/core/Types.h>
#include <vector>

namespace lumina::api {

class Dataset : public core::NoCopyable
{
public:
    virtual ~Dataset() noexcept = default;

    /** Vector dimension. Caller must keep it consistent with the Builder dimension. */
    virtual uint32_t Dim() const noexcept = 0;
    /** Total data size (optional, for pre-allocation). Return 0 if unknown. */
    virtual uint64_t TotalSize() const noexcept = 0;
    /**
     * Fetch the next batch. Implementations should clear and fill the buffers (not append).
     * vectorBuffer size must be rows * Dim(), and idBuffer size must be rows.
     * Return value: number of vectors in this batch; return 0 at end; return Status on error.
     */
    virtual core::Result<uint64_t> GetNextBatch(std::vector<float>& vectorBuffer,
                                                std::vector<core::vector_id_t>& idBuffer) noexcept = 0;
};

} // namespace lumina::api
