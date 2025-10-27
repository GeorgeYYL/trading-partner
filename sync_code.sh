#!/bin/bash
set -e

# 加载配置
source .env

# 临时 commit
git add .
git commit -m "sync: update code snapshot" || true

# 强制推送到分析分支
git push https://$GITHUB_PAT@github.com/$REPO.git HEAD:$BRANCH -f

echo "✅ Synced to branch '$BRANCH'"