# Java → C++ tantivy 跨端读 fixture

> 生成于 **2026-04-23**,用于 J6 `paimon-tantivy-java-compat-test`。

## 内容

| 文件 | 作用 |
|---|---|
| `english_simple.archive` | 由 paimon-java 的 `TantivyIndexWriter + packIndex` 路径生成的 BE archive;10 条纯英文文档,row_ids 0..9 |
| `english_simple.golden.json` | 人类可读 golden,每个 query type 的 expected row_ids |

## 版本锁定

| 组件 | 版本 |
|---|---|
| tantivy crate | **0.22.1** |
| paimon-tantivy-jni | git sha 生成时最新(commit 在 paimon 仓) |
| schema | B1:`row_id` u64 stored+indexed+fast + `text` TEXT |
| archive 字节格式 | Java-compat 大端 + 无 version |

任何组件升级(特别是 **tantivy 版本**)都可能导致段文件二进制不兼容 — 需**重新 regen**:

```bash
# 1. 构建 Java native lib(若 Rust 变了)
cd /path/to/paimon/paimon-tantivy/paimon-tantivy-jni/rust && cargo build --release
cp target/release/libtantivy_jni.dylib \
   ../src/main/resources/native/darwin-aarch64/

# 2. mvn install + 跑 fixture gen
cd /path/to/paimon
mvn install -pl paimon-tantivy/paimon-tantivy-index -am -DskipTests -Denforcer.skip=true
mvn -pl paimon-tantivy/paimon-tantivy-index test \
    -Dtest=TantivyIndexFixtureGen -DfailIfNoTests=false \
    -Denforcer.skip=true \
    -DfixtureOutDir=/path/to/paimon-cpp/test/test_data/java_tantivy_fixtures
```

## 检验

```
xxd english_simple.archive | head -1
# 00000000: 00 00 00 16 ...   ← BE int32 file_count = 22(Java 不 force-merge,多段)
```

## 相关文档

- `docs/dev/tantivy_java_cross_read_plan.md` — J6 整体 plan
- `docs/dev/test_execute.md` — J6 本次执行日志
- `docs/dev/tantivy_java_compat_plan.md` — paimon-cpp 与 paimon-java 对齐总方案
