#!/usr/bin/env bash
# Generate llms.txt and llms-full.txt for agent consumption.
# Files are placed in docs/ so MkDocs includes them in the built site.
#
# Page order is derived from mkdocs.yml nav — the single source of truth.
# Usage: ./scripts/generate-llms-txt.sh [docs_dir] [version]
#   docs_dir  — path to the docs directory (default: docs)
#   version   — version slug used in URLs (default: latest)

set -euo pipefail

DOCS_DIR="${1:-docs}"
DOCS_VERSION="${2:-latest}"
MKDOCS_YML="mkdocs.yml"
SCHEMA_FILE="$DOCS_DIR/schema/perfgazer-schema.json"
SITE_URL="https://amadeusitgroup.github.io/spark-perf-gazer/$DOCS_VERSION"

# ── Extract ordered markdown files from mkdocs.yml nav ───────────────────

# Pull every .md path referenced in the nav section.
nav_files=()
while IFS= read -r md; do
  nav_files+=("$DOCS_DIR/$md")
done < <(grep -oE '[^ ]+\.md' "$MKDOCS_YML")

# ── llms.txt (index) ────────────────────────────────────────────────────

{
  echo "# PerfGazer"
  echo ""
  echo "> Performance Gazer for Apache Spark — a configurable Spark Listener for post-mortem analysis."
  echo ""
  echo "- [GitHub](https://github.com/AmadeusITGroup/spark-perf-gazer)"
  echo "- [Full markdown documentation]($SITE_URL/llms-full.txt)"
  echo ""
  echo "## Docs"
  echo ""
  for f in "${nav_files[@]}"; do
    # Derive URL path: strip docs dir, strip filename for index.md, strip .md
    rel="${f#"$DOCS_DIR/"}"
    url_path="${rel%.md}"
    # index pages → parent directory
    url_path="${url_path%/index}"
    # top-level index → root
    if [ "$url_path" = "index" ]; then
      url_path=""
    fi
    # Build a readable title from the nav label (grep the YAML line)
    title=$(grep -F "$rel" "$MKDOCS_YML" | head -1 | sed 's/.*- *//;s/: .*//')
    # Fallback: derive title from filename
    if [ -z "$title" ] || echo "$title" | grep -qE '\.md$'; then
      title=$(basename "$rel" .md | sed 's/_/ /g;s/-/ /g;s/\b\(.\)/\u\1/g')
    fi
    echo "- [$title]($SITE_URL/$url_path/)"
  done
  echo ""
  echo "## Data Model"
  echo ""
  echo "- [Schema (JSON)]($SITE_URL/schema/perfgazer-schema.json)"
} > "$DOCS_DIR/llms.txt"

# ── llms-full.txt (all docs concatenated) ────────────────────────────────

FULL="$DOCS_DIR/llms-full.txt"
{
  echo "# PerfGazer — Full Documentation"
  echo ""
  for f in "${nav_files[@]}"; do
    if [ -f "$f" ]; then
      cat "$f"
      printf '\n\n---\n\n'
    fi
  done
  # Append the JSON schema
  if [ -f "$SCHEMA_FILE" ]; then
    echo "# Data Model Schema (JSON)"
    echo ""
    echo '```json'
    cat "$SCHEMA_FILE"
    echo '```'
  fi
} > "$FULL"

echo "Generated $DOCS_DIR/llms.txt and $FULL"
