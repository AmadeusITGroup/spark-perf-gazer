#!/usr/bin/env bash
# Full local documentation build and preview.
# Generates schemas, builds the site, produces llms.txt files, then serves locally.
# Usage: ./scripts/docs-serve-local.sh

set -euo pipefail

SCRIPT_DIR="$(dirname "$0")"

echo "==> Running full docs build..."
"$SCRIPT_DIR/docs-build.sh"

echo ""
echo "==> Starting MkDocs dev server..."
echo "    Press Ctrl+C to stop."
echo ""
mkdocs serve
