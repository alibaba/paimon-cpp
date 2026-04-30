# paimon-cpp CMake 工程评审与依赖管理建议

> 评审范围：根 `CMakeLists.txt`、`cmake_modules/`（21 个文件）、`src/paimon/CMakeLists.txt` 及各子目录、`PaimonConfig.cmake.in`、`examples/`。
>
> 评审标准：CMake 现代化（target-based）最佳工程实践、跨平台可移植性、依赖管理可维护性。
>
> 关联 issue：[#103](https://github.com/alibaba/paimon-cpp/issues/103) Flexible Third-Party Dependency Management System。

---

## 总体判断

当前 CMake 工程**能 work，但积累了较多现代化债**，整体风格沿用了 Apache Arrow 2018 年前后的写法（directory-scope `add_definitions` / `include_directories`、`macro()` 而非 `function()`、手写 `*Config.cmake.in`）。

Issue #103 提出的"per-dependency `_SOURCE` + `resolve_dependency()` 框架"方向上没问题，但完全自己复刻 Arrow 的 `ThirdpartyToolchain.cmake`（Arrow 那份已 4000+ 行）是**长期高维护成本路径**。本文档同时给出工程层修复建议与依赖管理的替代方案。

当前 issue #103 的首个 PR 建议收敛为：

- 支持 `AUTO` / `BUNDLED` / `SYSTEM` 三种依赖来源
- 支持主要第三方依赖的 per-dependency `_SOURCE`
- 用 `FindXxxAlt.cmake` 兼容不同系统包暴露出来的 target 名称
- 内部继续消费稳定的兼容 imported targets
- 输出依赖解析 summary，帮助 reviewer 和用户判断实际走了 system 还是 bundled
- 不在首个 PR 中承诺具体包管理器模式

后续 CMake 现代化可以继续拆成独立 PR：package export、target namespace、directory-scope 命令清理、平台 linker flag 修复、版本约束和包管理器集成等。

---

## 一、CMake 工程层面的问题

### 🔴 严重：会产生功能性 bug

#### 1. `PaimonConfig.cmake.in` 是手写伪导入目标，破坏可移植性

[PaimonConfig.cmake.in:15-145](../PaimonConfig.cmake.in#L15-L145) 对 ~20 个目标都用 `add_library(... IMPORTED)` + `IMPORTED_LOCATION "@CMAKE_INSTALL_PREFIX@/.../libpaimon.so"` 的硬编码方式。问题列表：

- **写死 `.so`** —— 在 macOS 上根本找不到（应是 `.dylib`），整个 `find_package(Paimon)` 在 mac 上会"假性成功"但链接失败
- **写死绝对路径** `@CMAKE_INSTALL_PREFIX@` —— 包不可重定位（用户把整个 install dir 移走就坏）
- **没有 `find_dependency(Arrow)` / `find_dependency(Protobuf)` / 等** —— 下游必须像 [examples/CMakeLists.txt:21-22](../examples/CMakeLists.txt#L21-L22) 那样自己手动 `find_package(Arrow)`，违反了 CMake package 自描述的契约
- **没有命名空间** —— `paimon_shared` 这种裸名极易和下游冲突
- [BuildUtils.cmake:135](../cmake_modules/BuildUtils.cmake#L135) 已经写了 `EXPORT ${LIB_NAME}_targets`，但**全局没有任何 `install(EXPORT ...)`** —— 这些 EXPORT 集合根本没被生成

**正确做法（标准 modern CMake 模式）：**

```cmake
# 在每个子目录里（或聚合到根）
install(EXPORT paimon_targets
        FILE PaimonTargets.cmake
        NAMESPACE Paimon::
        DESTINATION ${PAIMON_CMAKE_INSTALL_DIR})

# PaimonConfig.cmake.in 缩成 ~20 行
include(CMakeFindDependencyMacro)
find_dependency(Arrow CONFIG)
find_dependency(Threads)
if(@PAIMON_ENABLE_ORC@)
    find_dependency(Protobuf CONFIG)
endif()
include("${CMAKE_CURRENT_LIST_DIR}/PaimonTargets.cmake")
```

下游就只需 `find_package(Paimon CONFIG REQUIRED)` + `target_link_libraries(app PRIVATE Paimon::paimon)`。

#### 2. `option()` 与 `define_option()` 重复声明

[CMakeLists.txt:45-56](../CMakeLists.txt#L45-L56) 用 `option()` 定义了 `PAIMON_BUILD_STATIC` / `PAIMON_BUILD_SHARED` / `PAIMON_BUILD_TESTS` / 等；[DefineOptions.cmake:95-104](../cmake_modules/DefineOptions.cmake#L95-L104) 又用 `define_option()` 重复声明。这俩谁后赢取决于求值顺序（实际是 `include(DefineOptions)` 后赢，所以前面的 `option()` 块基本是无效代码），但容易让人改错地方。

#### 3. `string(TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE)` 自我覆盖

[SetupCxxFlags.cmake:44](../cmake_modules/SetupCxxFlags.cmake#L44) 直接把 `CMAKE_BUILD_TYPE` 自身改成大写。CMake 文档要求它保持原值（通常 mixed-case `Release`/`Debug`），多处 CMake 内置逻辑（如 `CMAKE_CXX_FLAGS_<CONFIG>`）依赖标准大小写。这条改完之后还可能影响 multi-config 生成器。建议改名：

```cmake
string(TOUPPER ${CMAKE_BUILD_TYPE} UPPERCASE_BUILD_TYPE)
```

#### 4. Linker flag 写死 GNU ld，破坏 macOS 构建

- [BuildUtils.cmake:127-132](../cmake_modules/BuildUtils.cmake#L127-L132) 用了 `-Wl,--exclude-libs,ALL`、`-Wl,-Bsymbolic`、`-Wl,-z,defs`、`-Wl,--gc-sections`
- [CMakeLists.txt:352-353](../CMakeLists.txt#L352-L353) 用了 `-Wl,--version-script=...`

这些都是 GNU ld / LLD 专属。Apple ld 全部不识别，会直接 fail。需要 `if(NOT APPLE)` 或者用 generator expression 包起来：

```cmake
target_link_options(${LIB_NAME}_shared PRIVATE
    $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--exclude-libs,ALL>
    $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,-Bsymbolic>
    $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,-z,defs>
    $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--gc-sections>)
```

#### 5. C/C++ flag 互相污染

[CMakeLists.txt:250-253](../CMakeLists.txt#L250-L253) 把 C++ flags 灌给 C 编译器再字符串删除 `-std=c++17`：

```cmake
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${CXX_COMMON_FLAGS} ${PAIMON_CXXFLAGS}")
string(REPLACE "-std=c++17" "" CMAKE_C_FLAGS ${CMAKE_C_FLAGS})
```

CMake 自 3.3 起的官方做法是 `$<COMPILE_LANGUAGE:CXX>` generator expression，按语言精准下发。一次 grep / replace 错了，C 文件就会用 C++ 标志编译，难定位。

---

### 🟡 中等：违反现代 CMake 最佳实践，技术债积累中

#### 6. 大量 directory-scope 命令（命令式 / 老式 CMake）

通篇有：
- [CMakeLists.txt:59-91](../CMakeLists.txt#L59-L91) 的 `add_definitions(-DPAIMON_ENABLE_*)`
- [CMakeLists.txt:270-348](../CMakeLists.txt#L270-L348) 的 `include_directories(...)`
- [CMakeLists.txt:294,303](../CMakeLists.txt#L294) 的 `link_directories(...)`

这些都是 directory-scope 副作用，导致：
- 第三方 ExternalProject 编译时也会"沾染"这些 flag
- include path 顺序难调试
- 让 `target_*` 的 PRIVATE/PUBLIC/INTERFACE 语义失效

**Modern CMake 的统一替代：**

```cmake
# 在 SetupCxxFlags 里
add_library(paimon_compile_options INTERFACE)
target_compile_features(paimon_compile_options INTERFACE cxx_std_17)
target_compile_options(paimon_compile_options INTERFACE
    $<$<COMPILE_LANGUAGE:CXX>:${CXX_ONLY_FLAGS}>
    ${CXX_COMMON_FLAGS})
target_compile_definitions(paimon_compile_options INTERFACE
    $<$<BOOL:${PAIMON_ENABLE_ORC}>:PAIMON_ENABLE_ORC>
    $<$<BOOL:${PAIMON_ENABLE_AVRO}>:PAIMON_ENABLE_AVRO>
    _GLIBCXX_USE_CXX11_ABI=$<IF:$<BOOL:${PAIMON_USE_CXX11_ABI}>,1,0>)
```

然后所有内部目标 `target_link_libraries(... PRIVATE paimon_compile_options)`。一处修改，全局生效，且不会泄漏到 ExternalProject。Apache Arrow 现在就这么用的（`arrow::flags::*` interface libs）。

#### 7. `EXTRA_INCLUDES ${ORC_INCLUDE_DIR}` / `${AVRO_INCLUDE_DIR}` 是冗余的

[orc/CMakeLists.txt:32-33](../src/paimon/format/orc/CMakeLists.txt#L32-L33)、[avro/CMakeLists.txt:32-33](../src/paimon/format/avro/CMakeLists.txt#L32-L33) 同时在 `EXTRA_INCLUDES` 里塞 `${ORC_INCLUDE_DIR}`，又把 `orc::orc` / `avro` 放进 `DEPENDENCIES`。后者已经通过 `INTERFACE_INCLUDE_DIRECTORIES` 自动传播 include 路径。前者是老 CMake 留下的"以防万一"——但当 Arrow/ORC 走 SYSTEM 路径时，`ARROW_INCLUDE_DIR` / `ORC_INCLUDE_DIR` 全局变量可能根本就不存在（因为是不同的 Find 模块设置的），会出现 silent miss。

#### 8. `build_*` 全是 `macro()` 而不是 `function()`

[ThirdpartyToolchain.cmake](../cmake_modules/ThirdpartyToolchain.cmake) 里所有 `build_xxx` 都是 `macro`。macro 没有自己的作用域，所有 local 变量（`ARROW_PREFIX`、`ARROW_INCLUDE_DIR` 等）会泄漏到调用方，下游又靠这些变量名工作 —— 现在 [root CMakeLists.txt:344](../CMakeLists.txt#L344) 的 `include_directories(SYSTEM ${ARROW_INCLUDE_DIR})` 就依赖这个泄漏。一旦改 SYSTEM 模式，变量名约定就崩。

**修法**：改成 `function()`，所有产出走 `add_library(... IMPORTED)` + `set_target_properties(...)`，外部只通过 imported target 拿信息。

#### 9. 命名空间不一致

| 已命名空间化 | 未命名空间化 |
| --- | --- |
| `re2::re2` | `arrow` |
| `orc::orc` | `parquet` |
| `GTest::gtest` | `zstd` |
| `jindosdk::c_sdk` | `lz4` |
| `Threads::Threads` | `zlib` |
| | `snappy`, `fmt`, `glog`, `tbb`, `avro`, `libprotobuf`, `RapidJSON` |

统一加上前缀（或保留每个上游官方命名空间，如 `Arrow::arrow_static`），可以彻底防止和下游 / 子项目重名。

#### 10. visibility 设置时机错误

[CMakeLists.txt:329](../CMakeLists.txt#L329) 的 `set(CMAKE_CXX_VISIBILITY_PRESET hidden)` 出现在 `add_subdirectory(third_party/...)` 之后。visibility 设置必须在创建 target 之前才会作用到那些 target。前面已经把 `roaring_bitmap`、`xxhash` 加进来了，对它们这条无效。

**修法**：挪到文件靠前位置（紧跟 `set(CMAKE_CXX_STANDARD 17)`），并补上：

```cmake
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_C_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)
```

#### 11. `cmake_minimum_required(VERSION 3.16)` 偏老

3.16 是 2019 年的版本。

| 版本 | 引入特性 | 对本项目意义 |
| --- | --- | --- |
| 3.19 | CMakePresets.json | 让 CI/dev 共享 build profile，替代 [build_and_package.sh](../build_and_package.sh) 的环境变量耦合 |
| 3.20 | `find_package(... CONFIG)` 行为更稳定 | 提升 SYSTEM 模式可靠性 |
| 3.24 | `FETCH_CONTENT_TRY_FIND_PACKAGE` | BUNDLED→SYSTEM 切换更优雅 |

建议升到 **3.22**（Ubuntu 22.04 LTS 自带），兼顾新特性与发行版兼容性。

#### 12. 非可移植 shell 命令

- [build_jieba](../cmake_modules/ThirdpartyToolchain.cmake#L711) `INSTALL_COMMAND bash -c "cp -r ..."`
- [build_jindosdk_c](../cmake_modules/ThirdpartyToolchain.cmake#L1051) `bash -c "cp -r ..."`
- [build_boost](../cmake_modules/ThirdpartyToolchain.cmake#L831) `bash -c "mkdir -p ... && cp -r ..."`

应该用 `${CMAKE_COMMAND} -E copy_directory ...` / `make_directory`，跨平台并能正确触发 build dependency。

---

### 🟢 轻微：值得整理但非紧急

13. [BuildUtils.cmake:257](../cmake_modules/BuildUtils.cmake#L257) 留了一行 `message(STATUS ${TEST_NAME})`，看起来是调试残留。
14. `UPPERCASE_BUILD_TYPE` 在三个文件里被独立计算（root、SetupCxxFlags、ThirdpartyToolchain），DRY 一下。
15. [CMakeLists.txt:289-290](../CMakeLists.txt#L289-L290) 有 `add_subdirectory(third_party/roaring_bitmap EXCLUDE_FROM_ALL)`，但 [CMakeLists.txt:333](../CMakeLists.txt#L333) 又 `include_directories("${CMAKE_SOURCE_DIR}/third_party/roaring_bitmap")` —— 后者冗余，因为 `roaring_bitmap` target 自身的 `INTERFACE_INCLUDE_DIRECTORIES` 已经传播了。
16. [SetupCxxFlags.cmake:42](../cmake_modules/SetupCxxFlags.cmake#L42) 设了 `CMAKE_POSITION_INDEPENDENT_CODE ON`，但 [BuildUtils.cmake:56](../cmake_modules/BuildUtils.cmake#L56) 又对每个 objlib 单独设 `POSITION_INDEPENDENT_CODE 1`，冗余。
17. [CMakeLists.txt:131-150](../CMakeLists.txt#L131-L150) 的 `COMMAND sed -i` 在 macOS 的 BSD sed 上语义不同（`-i` 强制要求一个空字符串参数）。

---

## 二、依赖管理建议

当前 issue #103 的实现思路是对的（仿 Arrow），但**仿 Arrow 是个坑**：Arrow 的 `ThirdpartyToolchain.cmake` 是十多年累积下来的工程债，行数已经 4000+，paimon-cpp 现在 1700 行只是开始。建议**重新评估**。

### 推荐方案 A：评估 vcpkg / Conan 作主路径，BUNDLED 退化为兜底

**理由：**
- vcpkg/Conan 已经把 Arrow、ORC、protobuf、zstd、snappy、lz4、glog、fmt、TBB、GTest 全部打包好了，它们解决了你现在自己在 ExternalProject 里手写的所有 patch、flags、依赖关系
- 用户接入只需 `cmake -DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake`
- BUNDLED 模式可以保留作为 fallback（CI / 离线环境）

这条路线需要单独设计讨论，当前 issue #103 的首个 PR 不应直接承诺具体包管理器集成。

**改动量小**：

```json
// vcpkg.json
{
  "name": "paimon-cpp",
  "version": "0.1.0",
  "dependencies": [
    "arrow", "protobuf", "zstd", "snappy", "lz4", "zlib",
    "re2", "fmt", "glog", "tbb", "rapidjson",
    {"name": "gtest", "host": true}
  ]
}
```

`PAIMON_DEPENDENCY_SOURCE=AUTO` 时直接走 `find_package(...)`，找不到再 fallback 到 BUNDLED。这条路 Apache Arrow 自己也支持。

### 推荐方案 B：`CPM.cmake` 替代 ExternalProject

Arrow 的 ExternalProject 最大的问题是它在 build 阶段才执行，**编译产物不在主项目的 build graph 里**。

`CPM.cmake`（一个单文件 wrapper）让 `FetchContent` 拥有缓存和 version pin 的能力，所有依赖都直接成为主 build graph 的一部分 —— `compile_commands.json` 包含第三方源码，IDE 跳转/补全、增量编译都正常工作。

**代价**：第一次配置慢（要拉所有源码），但有 `CPM_SOURCE_CACHE` 之后就和 ExternalProject 一样了。

### 不论选哪条路，下面这些改动是确定该做的

#### C. 用 `arrow_bundled_dependencies` 而不是单独构建一遍

现在 [build_arrow](../cmake_modules/ThirdpartyToolchain.cmake#L1339) 里你既单独 `build_zstd` / `build_snappy` / 等，又把 ROOT 透传给 Arrow。如果 Arrow 没真正 reuse（取决于它内部 `*_SOURCE` 的解析），最后会有两份 zstd 静态符号。建议加一个 build-time check：解开 `libarrow.a`，确认 `ZSTD_compress` 来自哪儿（应该只来自一处）。

> ⚠ 这是 ABI 风险点之一，比 issue #103 评论里讨论的"SYSTEM Avro vs BUNDLED Arrow 的 zstd"更紧迫，因为它在**当前 BUNDLED-only 路径下就已经存在**。

#### D. 包管理器模式需要后续单独设计

包管理器各自有不同的 toolchain / prefix 约定。它们不应混进第一版 `AUTO` / `SYSTEM` / `BUNDLED` 解析框架里仓促定型，后续可以围绕 `CMAKE_PREFIX_PATH`、`CMAKE_TOOLCHAIN_FILE`、manifest 文件和版本锁定策略单独设计。

#### E. SYSTEM 模式需要版本下限检查

现在 SYSTEM Arrow 不管什么版本都会被接受。`versions.txt` 里已经有 `PAIMON_ARROW_BUILD_VERSION=...`，把它作为 minimum：

```cmake
find_package(Arrow CONFIG ${PAIMON_ARROW_BUILD_VERSION})
# 找不到这个版本以上的就 fail / fallback
```

#### F. macOS 上 visibility 与 `--exclude-libs,ALL` 等价物

`--exclude-libs` 在 Apple ld 上没有等价物，唯一的 portable 方法是 **build BUNDLED deps 时编译期就 `-fvisibility=hidden -fvisibility-inlines-hidden`**。Apache Arrow 在 `EP_COMMON_TOOLCHAIN` 里就是这么干的。

---

## 三、优先级建议

按 ROI 排序：

| 优先级 | 项 | 理由 |
| --- | --- | --- |
| **P0** | 修 `PaimonConfig.cmake.in`（用 `install(EXPORT)`） | 现在的设计在 macOS 上**根本不工作**；下游用户体验差 |
| **P0** | 把 `--exclude-libs,ALL` / `--version-script` 等 GNU 专属 flag 用 `if(NOT APPLE)` 包起来 | 同上 |
| **P0** | `string(TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE)` 改名 | 潜在 multi-config 兼容隐患 |
| **P1** | 把 `add_definitions` / `include_directories` / `link_directories` 收编到 `paimon_compile_options` interface lib | 一次性还掉大笔技术债，让以后 SYSTEM/BUNDLED 切换不再靠"全局变量泄漏" |
| **P1** | `build_*` macro → function | 迁移到 SYSTEM 时不会再悄无声息地 break |
| **P1** | 评估 vcpkg/CPM 方案，看是否能替代部分手写依赖代码 | 需要作为包管理器策略单独讨论 |
| **P2** | 升级 cmake_minimum_required 到 3.22；引入 CMakePresets.json | 让 dev / CI 配置可复用 |
| **P2** | 命名空间统一 | 长期可维护性 |
| **P3** | 各种小清理（重复 set、debug print、shell 命令） | 代码卫生 |

---

## 四、结论

当前 CMake 工程是"能 work，但欠了不少现代化债" —— 沿用了 Arrow 2018 年左右的写法。

Issue #103 的方向（SOURCE / ROOT / 软默认）本身没问题，但完全自己实现并长期扩展 `resolve_dependency` 框架可能不是最佳投入。建议：

1. **先解决 P0/P1**，特别是 `PaimonConfig.cmake.in` 与 macOS 兼容性 —— 这两条直接决定了项目能不能被外部用户消费。
2. **单独评估包管理器入口**；vcpkg、Conan、CPM 等方案需要结合 CI、离线构建、版本锁定和下游用户体验再决定，不应混进 issue #103 的首个 PR 仓促定型。
3. **将 `paimon_compile_options` interface lib 落地**作为后续所有 target 的统一编译选项入口，是后续重构的"杠杆点"。

若需要可以分别针对：
- `PaimonConfig.cmake.in` 重写
- `paimon_compile_options` 的具体实现
- 包管理器集成方案对比

输出独立的改造草稿。
