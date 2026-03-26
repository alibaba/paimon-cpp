# paimon-cpp 分支管理规范

## 分支模型

本项目采用 **GitHub 社区仓库 + antcode 内部仓库** 双仓库协作模式。

```
GitHub (github)                antcode (origin)
─────────────                  ────────────────
main ─────────────────────────> main              (镜像同步，内容保持一致)
                                 \
                                  └──> internal    (内部分支，承载内部独有功能)
```

### 分支职责

| 分支 | 仓库 | 用途 |
|------|------|------|
| `main` | github + antcode | 社区主线，两边内容保持一致。antcode 的 main 通过 merge 跟踪 github 的 main |
| `internal` | antcode | 内部开发主线，包含社区代码 + 内部独有功能（如 zdfs 文件系统等） |
| `release-*` | github + antcode | 发布分支，按需同步 |
| `feat/*` | antcode | 内部 feature 开发分支，从 internal 拉出，合入 internal |

### 远程仓库配置

```bash
git remote -v
# github   https://github.com/alibaba/paimon-cpp.git     (社区仓库)
# origin   https://code.alipay.com/antflink/paimon-cpp.git (内部仓库)
```

---

## 日常操作

### 1. 从社区同步代码到 antcode

```bash
# 拉取社区最新代码
git fetch github

# 同步 main 分支（保持与社区一致）
git checkout main
git merge github/main
git push origin main

# 同步到 internal 分支（合入内部开发线）
git checkout internal
git merge main
# 如有冲突，解决后 git add && git merge --continue
git push origin internal
```

> **为什么用 merge 而不是 cherry-pick？**
> - merge 会记录共同祖先，Git 能自动识别已合并的 commit，**不会重复冲突**
> - cherry-pick 每次都产生新 commit hash，导致 Git 无法识别已同步的内容，**每次都可能冲突**

### 2. 内部 feature 开发

```bash
# 从 internal 分支拉出 feature 分支
git checkout internal
git checkout -b feat/my-feature

# 开发完成后合入 internal
git checkout internal
git merge --no-ff feat/my-feature
git push origin internal

# 清理 feature 分支
git branch -d feat/my-feature
```

### 3. 内部 feature 贡献回社区

当内部 feature 成熟、通用性足够时，可以贡献回 GitHub 社区：

```bash
# 从 main 分支拉出贡献分支
git checkout main
git checkout -b contrib/my-feature

# 将内部 feature 的 commit cherry-pick 过来
git cherry-pick <commit-hash>

# 推送到 GitHub 并提 PR
git push github contrib/my-feature
# 在 GitHub 上创建 Pull Request
```

### 4. 同步 release 分支

```bash
git fetch github
git checkout release-0.1
git merge github/release-0.1
git push origin refs/heads/release-0.1:refs/heads/release-0.1
```

---

## 内部功能开发规范

为了减少 merge 冲突，内部独有功能应遵循以下规范：

### 目录隔离

内部功能代码放在独立目录下，不要修改社区已有文件的核心逻辑：

```
src/paimon/fs/zdfs/          # zdfs 文件系统（独立目录）
src/paimon/internal/         # 其他内部功能（建议的目录）
```

### CMake 开关控制

每个内部功能通过 CMake option 控制，默认关闭：

```cmake
option(PAIMON_ENABLE_ZDFS "Whether to enable zdfs file system" OFF)

if(PAIMON_ENABLE_ZDFS)
    add_definitions(-DPAIMON_ENABLE_ZDFS)
endif()
```

### 构建配置集中管理

内部依赖的版本信息统一添加在 `third_party/versions.txt` 文件末尾，
构建逻辑添加在 `cmake_modules/ThirdpartyToolchain.cmake` 中，
用 `if(PAIMON_ENABLE_XXX)` 包裹，避免影响社区构建。

---

## 当前内部独有功能清单

| 功能 | 目录 | CMake 开关 | 说明 |
|------|------|-----------|------|
| ZDFS 文件系统 | `src/paimon/fs/zdfs/` | `PAIMON_ENABLE_ZDFS` | 内部分布式文件系统支持 |

---

## 构建说明

```bash
# 社区标准构建（不含内部功能）
cmake -B build .
cmake --build build

# 内部构建（启用 zdfs）
cmake -B build -DPAIMON_ENABLE_ZDFS=ON -DPAIMON_USE_CXX11_ABI=OFF .
cmake --build build
```
