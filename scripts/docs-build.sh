#!/usr/bin/env bash
# Full documentation build, as run by CI.
# Generates schemas, llms.txt files, and builds the MkDocs site.
# Usage: ./scripts/docs-build.sh

set -euo pipefail

SCRIPT_DIR="$(dirname "$0")"

echo "==> Generating data model docs from annotations..."
sbt "docGenerator/run"

echo "==> Generating llms.txt and llms-full.txt..."
"$SCRIPT_DIR/generate-llms-txt.sh"

echo "==> Building MkDocs site..."
mkdocs build

echo ""
echo "==> Done. Output in site/"
