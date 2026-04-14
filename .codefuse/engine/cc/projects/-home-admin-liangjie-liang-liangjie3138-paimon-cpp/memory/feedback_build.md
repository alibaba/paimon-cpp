---
name: build-flags
description: User prefers fixed -j8 for compilation, not -j$(nproc)
type: feedback
---

Use `-j8` for make commands, not `-j$(nproc)`.

**Why:** User explicitly requested fixed parallelism.

**How to apply:** Any time generating make/build commands, use `-j8`.
