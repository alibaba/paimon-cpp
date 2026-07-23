#!/usr/bin/env bash
#
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
# One-shot helper to build + launch + smoke-test the CentOS 7 verification
# container. Run from the paimon-cpp repo root.
#
# Usage:
#   ./.devcontainer/centos7/run.sh build         # build image only
#   ./.devcontainer/centos7/run.sh up            # start container (detached)
#   ./.devcontainer/centos7/run.sh shell         # exec into it
#   ./.devcontainer/centos7/run.sh smoke         # run scripts/tantivy_smoke.sh inside
#   ./.devcontainer/centos7/run.sh down          # stop + remove

set -euo pipefail

IMAGE=paimon-cpp-centos7:latest
CONTAINER=paimon-centos7

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(cd "${here}/../.." && pwd)

cmd=${1:-help}

case "${cmd}" in
    build)
        # Prefetch rustup-init on the host. In-container network from Docker
        # Desktop builds is unreliable for CN mirrors (TLS/HTTP2 issues with
        # old curl/wget on CentOS 7), but host curl works. The image copies
        # this blob in. Override mirror with RUSTUP_INIT_URL=... if needed.
        rustup_init="${here}/rustup-init.bin"
        rustup_url="${RUSTUP_INIT_URL:-https://mirrors.ustc.edu.cn/rust-static/rustup/dist/x86_64-unknown-linux-gnu/rustup-init}"
        if [ ! -s "${rustup_init}" ]; then
            echo "==> Prefetching rustup-init from ${rustup_url}"
            curl --proto '=https' --tlsv1.2 -sSfL --retry 5 --retry-delay 5 \
                -o "${rustup_init}" "${rustup_url}"
        fi
        # Override base image with CENTOS7_IMAGE=... if quay.io is unreachable.
        # Common fallbacks you may need to docker-pull into local cache first:
        #   CENTOS7_IMAGE=quay.io/centos/centos:centos7   (default)
        #   CENTOS7_IMAGE=registry.aliyuncs.com/library/centos:7
        if [ -n "${CENTOS7_IMAGE:-}" ]; then
            docker build -t "${IMAGE}" -f "${here}/Dockerfile" \
                --build-arg "CENTOS7_IMAGE=${CENTOS7_IMAGE}" "${repo}"
        else
            docker build -t "${IMAGE}" -f "${here}/Dockerfile" "${repo}"
        fi
        ;;
    up)
        docker rm -f "${CONTAINER}" 2>/dev/null || true
        # Mount host SSH keys read-only (mirrors paimon-dev) so git clones of
        # internal repos (e.g. aliorc_ep on gitlab.alibaba-inc.com) that go
        # over SSH can authenticate with the host's key. Skip the mount if
        # ~/.ssh doesn't exist so the script still works for external users.
        ssh_mount=()
        if [ -d "${HOME}/.ssh" ]; then
            ssh_mount=(-v "${HOME}/.ssh:/home/paimon/.ssh:ro")
        fi
        docker run -d \
            --name "${CONTAINER}" \
            --privileged \
            -v "${repo}:/workspaces/paimon-cpp" \
            -v "paimon-centos7-cargo-registry:/opt/rust/cargo/registry" \
            -v "paimon-centos7-build:/workspaces/paimon-cpp/build-centos7" \
            "${ssh_mount[@]}" \
            "${IMAGE}" sleep infinity
        # Named volumes mount as root-owned; `paimon` user (uid 1000) needs
        # write access to build-centos7 and the cargo registry cache.
        # Also set up the gitlab.alibaba-inc.com url rewrite so aliorc_ep
        # (and any other ExternalProject pointing at internal gitlab via
        # http://) picks up the mounted SSH key.
        docker exec --user root "${CONTAINER}" bash -c '
            chown -R paimon:paimon /workspaces/paimon-cpp/build-centos7 \
                                   /opt/rust/cargo/registry
        '
        docker exec "${CONTAINER}" bash -c '
            git config --global url."git@gitlab.alibaba-inc.com:".insteadOf \
                "http://gitlab.alibaba-inc.com/"
        '
        echo "Container started. \`${0} shell\` to enter."
        ;;
    shell)
        docker exec -it "${CONTAINER}" bash -l
        ;;
    smoke)
        # Ensure container is up first; no-op if already running.
        if ! docker ps --format '{{.Names}}' | grep -qx "${CONTAINER}"; then
            echo "Container ${CONTAINER} not running; starting it."
            "$0" up
        fi
        # Two env vars pass through for Rosetta 2 (Apple Silicon) compat:
        # MALLOC_CHECK_=0 disables glibc 2.17 extra malloc integrity checks
        #   that fire false positives under Rosetta's x86_64 emulation.
        # ARROW_USER_SIMD_LEVEL=SSE4_2 keeps arrow runtime-dispatched kernels
        #   on SSE4.2 only (Rosetta does not support AVX2/BMI2/AVX-512).
        # Both are no-ops on real x86_64 CentOS 7 hardware.
        # Use a distinct build dir inside the container so it does not clash
        # with the Ubuntu dev container's build/ dir on the same volume.
        # Propagate PAIMON_ENABLE_ALIORC so `PAIMON_ENABLE_ALIORC=OFF` env
        # on the host reaches the cmake inside the container.
        docker exec \
            -e "PAIMON_ENABLE_ALIORC=${PAIMON_ENABLE_ALIORC:-ON}" \
            -e "MALLOC_CHECK_=0" \
            -e "ARROW_USER_SIMD_LEVEL=SSE4_2" \
            "${CONTAINER}" bash -lc '
            set -eux
            cd /workspaces/paimon-cpp
            git lfs install --local
            git lfs pull
            cmake -S . -B build-centos7 \
                -G Ninja \
                -DCMAKE_BUILD_TYPE=Release \
                -DPAIMON_BUILD_TESTS=ON \
                -DPAIMON_ENABLE_FSLIB=OFF \
                -DPAIMON_ENABLE_LUMINA=OFF \
                -DPAIMON_ENABLE_JINDO=OFF \
                -DPAIMON_ENABLE_LUCENE=ON \
                -DPAIMON_ENABLE_ORC=ON \
                -DPAIMON_ENABLE_ALIORC="${PAIMON_ENABLE_ALIORC:-ON}" \
                -DPAIMON_ENABLE_AVRO=ON
            # ALIORC clones from internal gitlab. `up` mounts $HOME/.ssh and
            # configures the url.insteadOf rewrite, so by default ALIORC works
            # for alibaba-inc users. External users without gitlab access can
            # opt out with `PAIMON_ENABLE_ALIORC=OFF ./run.sh smoke`.
            cmake --build build-centos7 -j "$(nproc)"
            ctest --test-dir build-centos7 \
                -R "paimon-lucene-index-test|paimon-global-index-test|paimon-tantivy-.*-test" \
                --output-on-failure
        '
        ;;
    down)
        docker rm -f "${CONTAINER}" 2>/dev/null || true
        ;;
    help|*)
        sed -n "2,20p" "$0"
        ;;
esac
