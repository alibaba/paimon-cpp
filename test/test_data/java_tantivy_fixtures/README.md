# Java -> C++ tantivy cross-read fixtures

> Generated on **2026-04-23** for `paimon-tantivy-java-compat-test`.

## Contents

| File | Purpose |
|---|---|
| `english_simple.archive` | A BE archive produced by paimon-java's `TantivyIndexWriter + packIndex` path; 10 plain-English documents, row_ids 0..9 |
| `english_simple.golden.json` | Human-readable golden file: expected row_ids for each query type |

## Pinned versions

| Component | Version |
|---|---|
| tantivy crate | **0.22.1** |
| paimon-tantivy-jni | latest git sha at generation time (commit lives in the paimon repo) |
| schema | `row_id` u64 stored+indexed+fast + `text` TEXT |
| archive byte format | Java-compatible, big-endian, no version header |

Upgrading any component (especially the **tantivy version**) can make the segment
files binary-incompatible — regenerate the fixtures:

```bash
# 1. Build the Java native lib (if the Rust side changed)
cd /path/to/paimon/paimon-tantivy/paimon-tantivy-jni/rust && cargo build --release
cp target/release/libtantivy_jni.dylib \
   ../src/main/resources/native/darwin-aarch64/

# 2. mvn install + run the fixture generator
cd /path/to/paimon
mvn install -pl paimon-tantivy/paimon-tantivy-index -am -DskipTests -Denforcer.skip=true
mvn -pl paimon-tantivy/paimon-tantivy-index test \
    -Dtest=TantivyIndexFixtureGen -DfailIfNoTests=false \
    -Denforcer.skip=true \
    -DfixtureOutDir=/path/to/paimon-cpp/test/test_data/java_tantivy_fixtures
```

## Verification

```
xxd english_simple.archive | head -1
# 00000000: 00 00 00 16 ...   <- BE int32 file_count = 22 (Java does not
#                                force-merge, so multiple segments)
```

## Related code

- [`tantivy_java_compat_test.cpp`](../../../src/paimon/global_index/tantivy/tantivy_java_compat_test.cpp)
- [`tantivy CMakeLists.txt`](../../../src/paimon/global_index/tantivy/CMakeLists.txt)
- [`tantivy_ffi writer.rs`](../../../crates/tantivy_ffi/src/writer.rs)
