#pragma once
#include <cstdint>
#include <lumina/api/Options.h>
#include <lumina/core/NoCopyable.h>
#include <lumina/core/Result.h>
#include <lumina/core/Status.h>
#include <memory>

namespace lumina::io {

class FileWriter;
// TODO don't wrape with Result?
core::Result<std::unique_ptr<FileWriter>> CreateFileWriter(const lumina::api::IOOptions& ioOptions) noexcept;

class FileWriter : public core::NoCopyable
{
public:
    virtual ~FileWriter() noexcept = default;
    virtual core::Status Write(const char* data, uint64_t size) noexcept = 0;
    virtual core::Status Close() noexcept = 0;
    // Ownership is transferred when a writer is passed to LuminaBuilder::Dump.
    // Implementations must support Close being called before destruction.

    template <typename T>
    core::Status WriteObj(const T& val) noexcept
    {
        return this->Write(reinterpret_cast<const char*>(&val), sizeof(val));
    }

    virtual core::Result<uint64_t> GetLength() const noexcept = 0;
};
} // namespace lumina::io
