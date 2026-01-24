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

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <new>
#include <utility>

#include "paimon/macros.h"
#include "paimon/visibility.h"

namespace paimon {

class MemoryPool;

/// Create a default implementation of memory pool.
/// @return Unique pointer to a newly created `MemoryPool` instance.
PAIMON_EXPORT std::unique_ptr<MemoryPool> GetMemoryPool();

/// Get a system-wide singleton memory pool.
/// @return Shared pointer to the singleton `MemoryPool` instance.
PAIMON_EXPORT std::shared_ptr<MemoryPool> GetDefaultPool();

/// Abstract base class for memory pool implementations that provides controlled memory management.
class PAIMON_EXPORT MemoryPool {
 public:
    virtual ~MemoryPool() = default;
    MemoryPool& operator=(const MemoryPool& other) = delete;
    MemoryPool& operator=(MemoryPool& other) = delete;
    MemoryPool& operator=(MemoryPool&& other) = delete;

    /// Allocate memory from the pool.
    ///
    /// Allocates a block of memory with the specified size and alignment.
    /// The returned memory is uninitialized and must be properly constructed
    /// before use.
    ///
    /// @param size Number of bytes to allocate.
    /// @param alignment Memory alignment requirement (0 for default alignment).
    /// @return Pointer to allocated memory, or nullptr on failure.
    virtual void* Malloc(uint64_t size, uint64_t alignment = 0) = 0;

    /// Reallocate memory to a new size.
    ///
    /// Changes the size of a previously allocated memory block. The contents
    /// of the memory block are preserved up to the minimum of the old and new sizes.
    ///
    /// @param p Pointer to the memory block to reallocate.
    /// @param old_size Current size of the memory block.
    /// @param new_size New desired size of the memory block.
    /// @param alignment Memory alignment requirement (0 for default alignment).
    /// @return Pointer to the reallocated memory block.
    virtual void* Realloc(void* p, size_t old_size, size_t new_size, uint64_t alignment = 0) = 0;

    /// Deallocate memory back to the pool.
    ///
    /// Releases a previously allocated memory block back to the pool.
    /// The size must match the size used during allocation.
    /// The alignment used during allocation is not provided, subclass should store it
    /// by themselves if needed.
    ///
    /// @param p Pointer to the memory block to deallocate.
    /// @param size Size of the memory block (must match allocation size).
    virtual void Free(void* p, uint64_t size) = 0;

    /// Deallocate memory back to the pool with specified alignment.
    ///
    /// Releases a previously allocated memory block back to the pool.
    /// The size and alignment must match the values used during allocation.
    /// Subclass can override this method to optimize deallocation based on alignment.
    ///
    /// @param p Pointer to the memory block to deallocate.
    /// @param size Size of the memory block (must match allocation size).
    /// @param alignment Alignment of the memory block (must match allocation alignment).
    virtual void Free(void* p, uint64_t size, uint64_t alignment) {
        Free(p, size);
    }

    /// Get current memory usage.
    ///
    /// Returns the amount of memory currently allocated from this pool.
    /// This includes all outstanding allocations that have not been freed.
    ///
    /// @return Current memory usage in bytes.
    virtual uint64_t CurrentUsage() const = 0;

    /// Get peak memory usage.
    ///
    /// Returns the maximum amount of memory that has been allocated from
    /// this pool at any point in time since its creation.
    ///
    /// @return Peak memory usage in bytes.
    virtual uint64_t MaxMemoryUsage() const = 0;

    /// Custom deleter for use with std::unique_ptr that integrates with memory pools.
    ///
    /// AllocatorDelete provides automatic memory deallocation through the memory pool
    /// when used with std::unique_ptr. It stores both the pool reference and the
    /// allocation size to ensure proper cleanup.
    ///
    /// @tparam T Type of object being managed.
    template <typename T>
    class AllocatorDelete {
     private:
        std::reference_wrapper<MemoryPool> pool;
        size_t size{0};

     public:
        AllocatorDelete() : pool(*GetMemoryPool()) {}
        AllocatorDelete(MemoryPool& _pool, size_t _size) : pool(_pool), size(_size) {}
        AllocatorDelete(AllocatorDelete const&) = default;

        template <typename U>
        AllocatorDelete& operator=(const AllocatorDelete<U>& other) {
            pool = other.GetPool();
            size = other.GetSize();
            return *this;
        }

        AllocatorDelete& operator=(AllocatorDelete const& other) {
            pool = other.GetPool();
            size = other.GetSize();
            return *this;
        }

        AllocatorDelete& operator=(AllocatorDelete&& other) {
            pool = other.GetPool();
            size = other.GetSize();
            return *this;
        }

        template <typename U>
        AllocatorDelete(const AllocatorDelete<U>& other)  // NOLINT(google-explicit-constructor)
            : pool(other.GetPool()), size(other.GetSize()) {}

        MemoryPool& GetPool() const {
            return pool;
        }

        size_t GetSize() const {
            return size;
        }

        void operator()(T* p) const {
            if (p) {
                if (PAIMON_UNLIKELY(size == 0)) {
                    // This indicates a programming error where the deleter was not properly
                    // initialized with the correct allocation size. In debug builds, this
                    // will trigger an assertion. In release builds, we cannot safely
                    // deallocate without knowing the size, which may lead to undefined behavior.
                    assert(false &&
                           "Mismatched pointer allocation detected - deleter size is zero");
                    return;  // Cannot safely deallocate without size information
                }
                p->~T();
                pool.get().Free(reinterpret_cast<void*>(p), size);
            }
        }
    };

    /// Allocate an object on the pool and return a unique_ptr with custom deleter.
    ///
    /// This method provides a convenient way to allocate objects that are automatically
    /// managed by the memory pool. The returned unique_ptr will automatically deallocate
    /// the memory through the pool when it goes out of scope.
    ///
    /// @tparam T Type of object to allocate.
    /// @tparam Args Types of constructor arguments.
    /// @param args Constructor arguments to forward to T's constructor.
    /// @return unique_ptr managing the allocated object with pool-aware deleter.
    template <typename T, typename... Args>
    std::unique_ptr<T, AllocatorDelete<T>> AllocateUnique(Args&&... args) {
        struct DeferCondDeallocate {
            bool& cond;
            MemoryPool& pool;
            void* p;
            ~DeferCondDeallocate() {
                if (PAIMON_UNLIKELY(!cond)) {
                    pool.Free(p, sizeof(T));
                }
            }
        };
        auto p = Malloc(sizeof(T));
        {
            bool constructed = false;
            DeferCondDeallocate handler{constructed, *this, p};
            new (p) T(std::forward<Args>(args)...);
            constructed = true;
        }
        return std::unique_ptr<T, AllocatorDelete<T>>(reinterpret_cast<T*>(p),
                                                      AllocatorDelete<T>(*this, sizeof(T)));
    }
};

template <class T>
using pooled_unique_ptr = std::unique_ptr<T, MemoryPool::AllocatorDelete<T>>;

#define PAIMON_UNIQUE_PTR pooled_unique_ptr

}  // namespace paimon
