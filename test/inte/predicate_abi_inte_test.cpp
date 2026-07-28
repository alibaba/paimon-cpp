/*
 * Copyright 2026-present Alibaba Inc.
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

// Regression test for the Predicate cross-DSO RTTI/ABI fix (the out-of-line
// `Predicate::~Predicate` key function in common/predicate/predicate.cpp).
//
// `Predicate` is an abstract base with only pure virtuals. Without an
// out-of-line virtual ("key function"), the compiler has no single home for its
// vtable and typeinfo, so it emits them *weakly* in every translation unit that
// uses them. Combined with `-fvisibility=hidden`, separately linked modules
// (e.g. libpaimon.so vs libpaimon_parquet_file_format.so, or plugins loaded via
// dlopen) can each end up with their own `Predicate` typeinfo. A cross-module
// `dynamic_cast`/`dynamic_pointer_cast` then compares mismatched typeinfo and
// fails. Giving `Predicate` an out-of-line destructor anchors a single,
// exported definition of its RTTI that all other modules import.
//
// This file guards the fix on two levels:
//   1. Behavior: `Predicate` objects are created inside libpaimon.so and cast to
//      derived types here in the test module. This catches the failure on
//      toolchains that compare std::type_info by pointer (e.g. libc++) and in
//      downstream builds that link plugins aggressively.
//   2. Symbol layout (Linux/ELF only): assert that `Predicate`'s typeinfo has a
//      single home in libpaimon.so which the format plugins import, rather than
//      each plugin emitting its own duplicate copy. This is the deterministic
//      signature of the key function on glibc/libstdc++ toolchains, which merge
//      weak typeinfo by name and therefore would not fail the behavioral cast
//      even without the fix.

#include <memory>

#include "gtest/gtest.h"
#include "paimon/defs.h"
#include "paimon/predicate/compound_predicate.h"
#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

// A leaf predicate built in libpaimon.so must survive dynamic_pointer_cast to
// LeafPredicate across the module boundary.
TEST(PredicateAbiInteTest, LeafPredicateCastsAcrossModule) {
    std::shared_ptr<Predicate> predicate =
        PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                Literal(static_cast<int64_t>(5)));
    ASSERT_NE(predicate, nullptr);

    std::shared_ptr<LeafPredicate> leaf = std::dynamic_pointer_cast<LeafPredicate>(predicate);
    ASSERT_NE(leaf, nullptr) << "dynamic_pointer_cast<LeafPredicate> failed across the module "
                                "boundary; Predicate RTTI is likely duplicated (missing key "
                                "function)";
    EXPECT_EQ(leaf->FieldName(), "f0");
    EXPECT_EQ(leaf->FieldIndex(), 0);

    // A leaf predicate must not be mistaken for a compound predicate.
    EXPECT_EQ(std::dynamic_pointer_cast<CompoundPredicate>(predicate), nullptr);
}

// A compound predicate built in libpaimon.so must survive dynamic_pointer_cast
// to CompoundPredicate, and its children must remain castable to LeafPredicate.
TEST(PredicateAbiInteTest, CompoundPredicateCastsAcrossModule) {
    std::shared_ptr<Predicate> left =
        PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                Literal(static_cast<int64_t>(5)));
    std::shared_ptr<Predicate> right = PredicateBuilder::Equal(
        /*field_index=*/1, /*field_name=*/"f1", FieldType::INT, Literal(10));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate,
                         PredicateBuilder::And({left, right}));

    std::shared_ptr<CompoundPredicate> compound =
        std::dynamic_pointer_cast<CompoundPredicate>(predicate);
    ASSERT_NE(compound, nullptr)
        << "dynamic_pointer_cast<CompoundPredicate> failed across the module boundary";
    ASSERT_EQ(compound->Children().size(), 2u);

    EXPECT_EQ(std::dynamic_pointer_cast<LeafPredicate>(predicate), nullptr);
    for (const auto& child : compound->Children()) {
        EXPECT_NE(std::dynamic_pointer_cast<LeafPredicate>(child), nullptr);
    }
}

}  // namespace paimon::test

#if defined(__linux__) && defined(__ELF__)
#include <elf.h>
#include <fcntl.h>
#include <link.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdint>
#include <cstring>
#include <string>

namespace paimon::test {
namespace {

// Mangled name of `typeinfo for paimon::Predicate`.
constexpr const char* kPredicateTypeInfoSymbol = "_ZTIN6paimon9PredicateE";

// Result of looking up a symbol in an ELF dynamic symbol table.
enum class DynSym {
    kError,     // could not read the file
    kAbsent,    // symbol not present in .dynsym
    kDefined,   // symbol has a definition in this module (st_shndx != SHN_UNDEF)
    kImported,  // symbol is undefined here and resolved from another module
};

// On-disk path of the loaded shared object whose path contains `needle`.
std::string FindLoadedModule(const char* needle) {
    struct Ctx {
        const char* needle;
        std::string path;
    } ctx{needle, {}};
    dl_iterate_phdr(
        [](struct dl_phdr_info* info, size_t, void* data) -> int {
            auto* c = static_cast<Ctx*>(data);
            if (info->dlpi_name != nullptr && std::strstr(info->dlpi_name, c->needle) != nullptr) {
                c->path = info->dlpi_name;
                return 1;
            }
            return 0;
        },
        &ctx);
    return ctx.path;
}

// Whether `sym_name` is defined in, imported by, or absent from the dynamic
// symbol table of the ELF file at `path`.
//
// We look at whether the symbol is *defined* (has a section index) rather than
// at its binding (GLOBAL vs WEAK): a class with a key function has WEAK typeinfo
// on GCC but GLOBAL typeinfo on Clang, so the binding is not portable. What both
// compilers guarantee is that the key function makes the typeinfo a single
// definition that other modules import.
DynSym LookupDynSym(const std::string& path, const char* sym_name) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        return DynSym::kError;
    }
    struct stat st{};
    if (::fstat(fd, &st) != 0) {
        ::close(fd);
        return DynSym::kError;
    }
    void* map = ::mmap(nullptr, static_cast<size_t>(st.st_size), PROT_READ, MAP_PRIVATE, fd, 0);
    ::close(fd);
    if (map == MAP_FAILED) {
        return DynSym::kError;
    }

    const auto* base = static_cast<const uint8_t*>(map);
    const auto* ehdr = reinterpret_cast<const Elf64_Ehdr*>(base);
    DynSym result = DynSym::kAbsent;
    if (std::memcmp(ehdr->e_ident, ELFMAG, SELFMAG) == 0 && ehdr->e_ident[EI_CLASS] == ELFCLASS64) {
        const auto* shdrs = reinterpret_cast<const Elf64_Shdr*>(base + ehdr->e_shoff);
        for (int i = 0; i < ehdr->e_shnum; ++i) {
            if (shdrs[i].sh_type != SHT_DYNSYM) {
                continue;
            }
            const auto* syms = reinterpret_cast<const Elf64_Sym*>(base + shdrs[i].sh_offset);
            const char* strtab =
                reinterpret_cast<const char*>(base + shdrs[shdrs[i].sh_link].sh_offset);
            size_t count = shdrs[i].sh_size / sizeof(Elf64_Sym);
            for (size_t s = 0; s < count; ++s) {
                if (std::strcmp(strtab + syms[s].st_name, sym_name) == 0) {
                    result = syms[s].st_shndx == SHN_UNDEF ? DynSym::kImported : DynSym::kDefined;
                    break;
                }
            }
            break;
        }
    }
    ::munmap(map, static_cast<size_t>(st.st_size));
    return result;
}

}  // namespace

// Deterministic guard (compiler-agnostic across GCC and Clang): the out-of-line
// `~Predicate()` key function must give `Predicate`'s typeinfo a single home in
// libpaimon.so, which the format plugins then import instead of each emitting
// their own copy. Without the key function the typeinfo is weak and every
// module that casts a Predicate defines its own duplicate -- exactly the
// condition that breaks cross-DSO dynamic_cast. Reverting the fix flips this
// test from PASS to FAIL.
TEST(PredicateAbiInteTest, PredicateTypeInfoHasSingleHome) {
    std::string core = FindLoadedModule("libpaimon.so");
    ASSERT_FALSE(core.empty()) << "could not locate loaded libpaimon.so";

    // The core library owns the one definition of the typeinfo.
    EXPECT_EQ(LookupDynSym(core, kPredicateTypeInfoSymbol), DynSym::kDefined)
        << "typeinfo for paimon::Predicate must be defined in " << core;

    // A format plugin that casts predicates must import that definition rather
    // than defining its own duplicate. The parquet plugin performs
    // dynamic_pointer_cast<LeafPredicate>/<CompoundPredicate> on Predicate, so it
    // references the typeinfo; with the key function that reference resolves to
    // libpaimon.so (imported), without it the plugin carries its own weak copy.
    std::string plugin = FindLoadedModule("libpaimon_parquet_file_format.so");
    ASSERT_FALSE(plugin.empty()) << "could not locate loaded libpaimon_parquet_file_format.so";

    DynSym in_plugin = LookupDynSym(plugin, kPredicateTypeInfoSymbol);
    ASSERT_NE(in_plugin, DynSym::kError) << "could not read " << plugin;
    ASSERT_NE(in_plugin, DynSym::kAbsent)
        << "typeinfo for paimon::Predicate is not referenced by " << plugin
        << "; the parquet plugin is expected to cast Predicate across the DSO boundary";
    EXPECT_EQ(in_plugin, DynSym::kImported)
        << "typeinfo for paimon::Predicate is defined inside " << plugin
        << " instead of being imported from libpaimon.so; the Predicate RTTI is duplicated across "
           "DSOs (the ~Predicate() key function is missing), which breaks cross-DSO dynamic_cast";
}

}  // namespace paimon::test
#endif  // defined(__linux__) && defined(__ELF__)
