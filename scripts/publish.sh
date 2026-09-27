#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
if [[ $# -gt 1 || ( $# -eq 1 && "$1" != "--dry-run" ) ]]; then
    echo 'Usage: scripts/publish.sh [--dry-run]' >&2
    exit 2
fi
if [[ -n "$(git status --porcelain --untracked-files=all)" ]]; then
    echo 'Commit the complete candidate before publication.' >&2
    exit 1
fi
candidate="$(git rev-parse HEAD)"
bash scripts/check.sh
cargo package --locked --offline
if [[ "$(git rev-parse HEAD)" != "$candidate" || -n "$(git status --porcelain --untracked-files=all)" ]]; then
    echo 'The release candidate changed during validation.' >&2
    exit 1
fi
cargo publish --locked --registry univers "$@"
