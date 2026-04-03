#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <iterator>
#include <lumina/api/Extension.h>
#include <lumina/api/LuminaSearcher.h>
#include <lumina/core/Constants.h>
#include <lumina/core/Status.h>
#include <span>

namespace lumina::extensions { inline namespace experimental {
// Extension for searcher to provide random access to vectors
class GetVectorExtension final : public api::ISearchExtension
{
public:
    GetVectorExtension() = default;
    ~GetVectorExtension() override = default;

    GetVectorExtension(GetVectorExtension&&) noexcept = delete;
    GetVectorExtension& operator=(GetVectorExtension&&) noexcept = delete;

    constexpr static std::string_view ExtensionName() { return core::kExtensionGetVector; }

    std::string_view Name() const noexcept override { return ExtensionName(); }

    using GetVectorFn = const float*(core::vector_id_t);
    // Returns pointer to vector data, or nullptr if id is invalid.
    // The returned pointer is valid as long as the searcher remains alive.
    // TODO: use span
    const float* GetVector(core::vector_id_t id) noexcept
    {
        if (!_getVector) {
            return nullptr;
        }
        return _getVector(id);
    }

    // TODO: use span
    using GetVectorIdsFn = std::vector<core::vector_id_t>();
    std::vector<core::vector_id_t> GetVectorIds() noexcept
    {
        assert(_getVectorIds);
        return _getVectorIds();
    }

    void SetFunc(std::function<GetVectorFn> func) noexcept { _getVector = std::move(func); }
    void SetFunc(std::function<GetVectorIdsFn> func) noexcept { _getVectorIds = std::move(func); }

private:
    std::function<GetVectorFn> _getVector;
    std::function<GetVectorIdsFn> _getVectorIds;
};
}} // namespace lumina::extensions::experimental
