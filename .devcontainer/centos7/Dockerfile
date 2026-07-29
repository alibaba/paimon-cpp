# Copyright 2026-present Alibaba Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# CentOS 7 cross-build verification image for paimon-cpp + tantivy-fts.
#
# Purpose:
#   Prove the tantivy-fts stack builds on the OLDEST reasonable Linux target
#   (glibc 2.17, EOL 2024-06-30). The default Ubuntu 24.04 dev container
#   proves nothing about glibc compatibility; this image does.
#
# Build:
#   docker build -t paimon-cpp-centos7:latest -f .devcontainer/centos7/Dockerfile .
#
# Run:
#   docker run -d --name paimon-centos7 \
#     --privileged \
#     -v "$(pwd):/workspaces/paimon-cpp" \
#     paimon-cpp-centos7:latest sleep infinity
#   docker exec -it paimon-centos7 bash -l
#
# Inside the container:
#   scl enable devtoolset-11 rh-python38 -- bash        # activate modern gcc + python
#   source /opt/paimon-env.sh                           # PATH for rust, cmake
#   cd /workspaces/paimon-cpp
#
# Run ./.devcontainer/centos7/run.sh smoke from the host for the full check.

# ---------- Base ----------
# CentOS 7 reached EOL 2024-06-30; its default mirrorlist.centos.org is down.
# Pin to vault.centos.org (Red Hat's archived location) via the `linuxserver/centos`
# vault image to avoid retired-mirror failures on `yum install`.
#
# Base image: we pull from quay.io (CentOS community's canonical registry post
# Docker Hub deprecation). Override with CENTOS7_IMAGE build arg when behind a
# firewall that can't reach quay.io (e.g. registry.aliyuncs.com/library/centos:7).
ARG CENTOS7_IMAGE=quay.io/centos/centos:centos7
FROM ${CENTOS7_IMAGE}

# Repoint yum at aliyun's CentOS 7 vault mirror — vault.centos.org itself
# works but is slow/blocked from many CN networks; the aliyun mirror is a
# complete rsync and reliably fast. We overwrite CentOS-Base.repo rather
# than sed-patch it so the result is deterministic regardless of what the
# upstream image ships. fastestmirror plugin is disabled because its ping
# probes against the retired mirror list add ~60s to every `yum install`.
RUN echo -e '[base]\n\
name=CentOS-7 - Base - aliyun vault\n\
baseurl=https://mirrors.aliyun.com/centos-vault/7.9.2009/os/$basearch/\n\
gpgcheck=0\n\
enabled=1\n\
\n\
[updates]\n\
name=CentOS-7 - Updates - aliyun vault\n\
baseurl=https://mirrors.aliyun.com/centos-vault/7.9.2009/updates/$basearch/\n\
gpgcheck=0\n\
enabled=1\n\
\n\
[extras]\n\
name=CentOS-7 - Extras - aliyun vault\n\
baseurl=https://mirrors.aliyun.com/centos-vault/7.9.2009/extras/$basearch/\n\
gpgcheck=0\n\
enabled=1\n\
\n\
[centosplus]\n\
name=CentOS-7 - Plus - aliyun vault\n\
baseurl=https://mirrors.aliyun.com/centos-vault/7.9.2009/centosplus/$basearch/\n\
gpgcheck=0\n\
enabled=0\n' > /etc/yum.repos.d/CentOS-Base.repo \
    && rm -f /etc/yum.repos.d/CentOS-CR.repo \
             /etc/yum.repos.d/CentOS-Debuginfo.repo \
             /etc/yum.repos.d/CentOS-Media.repo \
             /etc/yum.repos.d/CentOS-Sources.repo \
             /etc/yum.repos.d/CentOS-Vault.repo \
             /etc/yum.repos.d/CentOS-fasttrack.repo \
             /etc/yum.repos.d/CentOS-x86_64-kernel.repo \
    && if [ -f /etc/yum/pluginconf.d/fastestmirror.conf ]; then \
         sed -i 's/^enabled=1/enabled=0/' /etc/yum/pluginconf.d/fastestmirror.conf; \
       fi \
    && yum clean all \
    && yum makecache

# ---------- Base toolchain ----------
# EPEL provides ninja-build and a newer python3 than the base 3.6.
# SCL (Software Collections) provides devtoolset-11 (gcc 11) and rh-python38
# without overriding the system gcc/python. CentOS 7's default gcc 4.8 is
# too old for C++17/20 used by lucene++ and our tantivy wrapper.
#
# Same story as CentOS-Base.repo: both epel + SCL default to mirrorlist
# endpoints that are effectively dead; overwrite with aliyun URLs that we
# know respond.
RUN yum install -y epel-release centos-release-scl \
    && echo -e '[epel]\n\
name=Extra Packages for Enterprise Linux 7 - aliyun\n\
baseurl=https://mirrors.aliyun.com/epel/7/$basearch\n\
gpgcheck=0\n\
enabled=1\n' > /etc/yum.repos.d/epel.repo \
    && rm -f /etc/yum.repos.d/epel-testing.repo /etc/yum.repos.d/epel.repo.rpmnew \
    && rm -f /etc/yum.repos.d/CentOS-SCLo-*.repo \
             /etc/yum.repos.d/CentOS-SCLo-*.repo.rpmnew \
    && echo -e '[centos-sclo-rh]\n\
name=CentOS-7 - SCLo rh - aliyun vault\n\
baseurl=https://mirrors.aliyun.com/centos-vault/7.9.2009/sclo/$basearch/rh/\n\
gpgcheck=0\n\
enabled=1\n\
\n\
[centos-sclo-sclo]\n\
name=CentOS-7 - SCLo sclo - aliyun vault\n\
baseurl=https://mirrors.aliyun.com/centos-vault/7.9.2009/sclo/$basearch/sclo/\n\
gpgcheck=0\n\
enabled=1\n' > /etc/yum.repos.d/CentOS-SCLo-scl.repo \
    && yum clean all && yum makecache \
    && yum install -y \
        devtoolset-11-gcc \
        devtoolset-11-gcc-c++ \
        devtoolset-11-binutils \
        devtoolset-11-libasan-devel \
        devtoolset-11-libubsan-devel \
        rh-python38 \
        rh-python38-python-pip \
        git \
        ninja-build \
        make \
        patch \
        curl \
        wget \
        unzip \
        which \
        file \
        sudo \
        openssl-devel \
        zlib-devel \
        libffi-devel \
        bzip2-devel \
        xz-devel \
        perl-IPC-Cmd \
    && yum clean all

# Enable the SCL collections for all subsequent shells (including RUN).
ENV BASH_ENV=/etc/profile.d/scl-enable.sh
SHELL ["/bin/bash", "-c"]
RUN printf '%s\n' \
        'source scl_source enable devtoolset-11' \
        'source scl_source enable rh-python38' \
    > /etc/profile.d/scl-enable.sh \
    && chmod +x /etc/profile.d/scl-enable.sh

# ---------- CMake (must be >= 3.22 for Corrosion) ----------
# CentOS 7's cmake package is 2.8.12; EPEL cmake3 is 3.17 — still too old.
# Install via pip in the rh-python38 SCL so we get a modern CMake without
# touching the system /usr/bin. Point pip at aliyun's pypi mirror: default
# pypi.org is 10-30s per request from CN, aliyun responds in <1s.
ENV PIP_INDEX_URL=https://mirrors.aliyun.com/pypi/simple/ \
    PIP_TRUSTED_HOST=mirrors.aliyun.com
RUN source /etc/profile.d/scl-enable.sh \
    && python3 -m pip install --upgrade pip \
    && python3 -m pip install 'cmake==3.28.*' ninja

# ---------- Rust toolchain ----------
# Install rustup as root into /opt/rust so all users share the same toolchain.
# Use the USTC mirror to keep downloads fast in CN; the CI runner version of
# this is mirrored in ci/scripts/setup_rust.sh.
ENV RUSTUP_HOME=/opt/rust/rustup \
    CARGO_HOME=/opt/rust/cargo \
    RUSTUP_DIST_SERVER=https://mirrors.ustc.edu.cn/rust-static \
    RUSTUP_UPDATE_ROOT=https://mirrors.ustc.edu.cn/rust-static/rustup
# In-container network for Docker Desktop builds is unreliable through many
# CN mirrors (observed: curl 7.29 on CentOS 7 + rsproxy.cn HTTP/2 path ⇒
# partial-read truncations; USTC ⇒ 5xx; rustup sh installer ⇒ 403 from
# legacy cipher). The most reliable fix is to sidestep the issue entirely:
# pre-download rustup-init on the host (where network is solid) and COPY it
# into the image. See .devcontainer/centos7/run.sh for the prefetch step.
COPY .devcontainer/centos7/rustup-init.bin /tmp/rustup-init
RUN chmod +x /tmp/rustup-init \
    && /tmp/rustup-init -y --default-toolchain stable --profile minimal --no-modify-path \
    && rm -f /tmp/rustup-init \
    && mkdir -p $CARGO_HOME \
    && echo -e '[source.crates-io]\n\
replace-with = "rsproxy-sparse"\n\
\n\
[source.rsproxy]\n\
registry = "https://rsproxy.cn/crates.io-index"\n\
\n\
[source.rsproxy-sparse]\n\
registry = "sparse+https://rsproxy.cn/index/"\n\
\n\
[registries.rsproxy]\n\
index = "https://rsproxy.cn/crates.io-index"\n\
\n\
[net]\n\
git-fetch-with-cli = true\n' > $CARGO_HOME/config.toml \
    && $CARGO_HOME/bin/cargo install cbindgen --version 0.29.2 --locked \
    && chmod -R a+rwx /opt/rust \
    && $CARGO_HOME/bin/rustc --version \
    && $CARGO_HOME/bin/cargo --version \
    && $CARGO_HOME/bin/cbindgen --version

# ---------- Environment file consumed by every shell ----------
# Sets PATH for rust / cmake / cargo so `docker exec paimon-centos7 bash -l`
# and interactive sessions have the toolchain on $PATH.
RUN printf '%s\n' \
        'export PATH=/opt/rust/cargo/bin:$PATH' \
        '# cmake + ninja live under the rh-python38 SCL; path prefix differs by arch.' \
        '# `command -v cmake` confirms which one is in use.' \
    > /opt/paimon-env.sh \
    && chmod +x /opt/paimon-env.sh \
    && printf '%s\n' 'source /opt/paimon-env.sh' >> /etc/profile.d/scl-enable.sh

# ---------- Non-root user ----------
# Build as `paimon` (uid 1000) so LFS objects under the mount stay owned by
# your host user, matching the main Ubuntu dev container.
RUN useradd -m -u 1000 -s /bin/bash paimon \
    && echo 'paimon ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/paimon

USER paimon
WORKDIR /workspaces/paimon-cpp

# Sanity check surfaces the tool versions in `docker run ... paimon-cpp-centos7 --version`.
CMD ["bash", "-lc", "\
    echo '--- CentOS 7 cross-build image sanity check ---'; \
    cat /etc/centos-release; \
    echo '--- glibc ---'; ldd --version | head -1; \
    echo '--- gcc   ---'; gcc --version | head -1; \
    echo '--- cmake ---'; cmake --version | head -1; \
    echo '--- ninja ---'; ninja --version; \
    echo '--- rust  ---'; rustc --version; \
    echo '--- cargo ---'; cargo --version; \
    echo '--- cbindgen ---'; cbindgen --version; \
    echo 'Ready. From the host repository, run ./.devcontainer/centos7/run.sh smoke'"]
