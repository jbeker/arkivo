#!/usr/bin/env bash
# Build the Arkivo image and push it to registry.example.com.
#
# Safety first: any uncommitted work is committed, and the branch is
# pushed to its remote (when one exists), BEFORE the image builds — so
# the code that produced an image is never lost.
#
# Usage:
#   scripts/build-and-push.sh                 # commit-if-dirty, push git, build, push image
#   SKIP_PUSH=1 scripts/build-and-push.sh     # everything except the registry push (dry run)
#
# Requires: docker login registry.example.com (once, beforehand).

set -euo pipefail

REGISTRY="registry.example.com"
IMAGE="${REGISTRY}/arkivo"

cd "$(dirname "$0")/.."

# --- 1. Make sure the code is committed -------------------------------------
if [[ -n "$(git status --porcelain)" ]]; then
    echo "==> Uncommitted changes found; committing a snapshot:"
    git status --short
    git add -A
    git commit -m "Pre-build snapshot $(date -u +%Y-%m-%dT%H:%M:%SZ)"
else
    echo "==> Working tree clean."
fi

GIT_SHA="$(git rev-parse --short HEAD)"

# --- 2. Push to the git remote, if one exists --------------------------------
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if git remote get-url origin >/dev/null 2>&1; then
    echo "==> Pushing ${BRANCH} to origin..."
    git push origin "${BRANCH}"
else
    echo "==> No git remote configured; skipping git push." \
         "(Add one with: git remote add origin <url>)"
fi

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
