#!/usr/bin/env bash
# Build the Arkivo image and push it to registry.example.com.
#
# Safety first: the script never commits or pushes git for you — it
# REFUSES to build if there are uncommitted changes or unpushed
# commits, so every image is traceable to code that is safely in the
# remote repository.
#
# Usage:
#   scripts/build-and-push.sh                 # verify git state, build, push image
#   SKIP_PUSH=1 scripts/build-and-push.sh     # everything except the registry push (dry run)

set -euo pipefail

REGISTRY="registry.example.com"
IMAGE="${REGISTRY}/arkivo"

cd "$(dirname "$0")/.."

# --- 1. Refuse to build uncommitted work -------------------------------------
if [[ -n "$(git status --porcelain)" ]]; then
    echo "ERROR: uncommitted changes; commit (or stash) them first:" >&2
    git status --short >&2
    exit 1
fi
echo "==> Working tree clean."

# --- 2. Refuse to build unpushed work (when a remote exists) -----------------
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if git remote get-url origin >/dev/null 2>&1; then
    if ! UPSTREAM="$(git rev-parse --abbrev-ref --symbolic-full-name '@{upstream}' 2>/dev/null)"; then
        echo "ERROR: ${BRANCH} has no upstream; push it first:" >&2
        echo "  git push -u origin ${BRANCH}" >&2
        exit 1
    fi
    git fetch --quiet origin
    AHEAD="$(git rev-list --count "${UPSTREAM}..HEAD")"
    if [[ "${AHEAD}" -gt 0 ]]; then
        echo "ERROR: ${AHEAD} commit(s) not pushed to ${UPSTREAM}; run git push first." >&2
        exit 1
    fi
    echo "==> ${BRANCH} is in sync with ${UPSTREAM}."
else
    echo "==> No git remote configured; skipping push check." \
         "(Add one with: git remote add origin <url>)"
fi

GIT_SHA="$(git rev-parse --short HEAD)"

# --- 3. Build -----------------------------------------------------------------
echo "==> Building ${IMAGE}:${GIT_SHA}..."
docker build -f docker/Dockerfile \
    -t "${IMAGE}:${GIT_SHA}" \
    -t "${IMAGE}:latest" \
    .

# --- 4. Push to the registry ---------------------------------------------------
if [[ "${SKIP_PUSH:-0}" == "1" ]]; then
    echo "==> SKIP_PUSH=1: not pushing. Built ${IMAGE}:${GIT_SHA} and ${IMAGE}:latest."
    exit 0
fi

echo "==> Pushing ${IMAGE}:${GIT_SHA} and ${IMAGE}:latest..."
docker push "${IMAGE}:${GIT_SHA}"
docker push "${IMAGE}:latest"

echo "==> Done. Deploy with ARKIVO_IMAGE=${IMAGE}:${GIT_SHA} (pinned)" \
     "or ${IMAGE}:latest in the Portainer stack."
