#!/bin/bash
set -euo pipefail

# 读取 token（.env 不进 Git）
# shellcheck disable=SC1091
source .env

: "${GITHUB_PAT:?missing}"
REPO=${REPO:-"GeorgeYYL/trading-partner"}
BRANCH=${BRANCH:-"analysis-bot"}

# 确保在 main 上生成快照（或你指定的源分支）
git switch main

# 重建孤儿分支快照（不继承历史）
git branch -D "${BRANCH}" 2>/dev/null || true
git switch --orphan "${BRANCH}"
git reset --hard

# 只拣核心代码目录
git checkout main -- apps libs infra Makefile .gitignore .dockerignore

git add apps libs infra Makefile .gitignore .dockerignore
git commit -m "analysis snapshot $(date -u +%Y-%m-%dT%H:%M:%SZ)"

# 推送（禁用钥匙串）
git -c credential.helper= \
  push "https://x-access-token:${GITHUB_PAT}@github.com/${REPO}.git" \
  "${BRANCH}" -f

echo "✅ Synced analysis snapshot to ${REPO}:${BRANCH}"
