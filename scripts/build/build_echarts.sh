#!/usr/bin/env bash
# Rebuild the trimmed ECharts vendor bundle from scripts/build/echarts-entry.js.
#
# The site has no build step; this is a vendoring step, run by hand only when
# the entry's series/component list changes. It installs the pinned echarts
# and esbuild into a throwaway directory, bundles the entry as an IIFE that
# assigns the same writable global `echarts` the full build did, and overwrites
# site/assets/vendor/echarts.min.js. The pre-commit hook re-hashes ?v=.
set -euo pipefail

ECHARTS_VERSION=5.4.3
ESBUILD_VERSION=0.24.2

repo="$(cd "$(dirname "$0")/../.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

cp "$repo/scripts/build/echarts-entry.js" "$work/entry.js"
cd "$work"
npm init -y >/dev/null
npm install --silent --no-audit --no-fund \
  "echarts@$ECHARTS_VERSION" "esbuild@$ESBUILD_VERSION"
npx esbuild entry.js --bundle --minify --format=iife \
  --target=es2019 --legal-comments=eof \
  --define:__DEV__=false --define:process.env.NODE_ENV='"production"' \
  --outfile="$repo/site/assets/vendor/echarts.min.js"

ls -l "$repo/site/assets/vendor/echarts.min.js"
