#pragma once
#include <cstddef>
#include <cstdint>
#include <functional>
#include <lumina/api/Options.h>
#include <lumina/api/Query.h>
#include <lumina/core/NoCopyable.h>
#include <lumina/core/Status.h>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace lumina::api {

// Note: Attach only registers capabilities; the framework does not own extension lifetimes.
// Callers must ensure thread safety and lifetime covers the search/build process.
class ISearchExtension : public core::NoCopyable
{
public:
    virtual ~ISearchExtension() = default;
    virtual std::string_view Name() const noexcept = 0;
};

class IBuildExtension : public core::NoCopyable
{
public:
    virtual ~IBuildExtension() = default;
    virtual std::string_view Name() const noexcept = 0;
};

} // namespace lumina::api
