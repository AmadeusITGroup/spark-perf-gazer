#!/usr/bin/env bash
# Generate llms.txt and llms-full.txt for agent consumption.
# Files are placed in docs/ so MkDocs includes them in the built site.
# Usage: ./scripts/generate-llms-txt.sh

set -euo pipefail

DOCS_DIR="${1:-docs}"
SCHEMA_FILE="$DOCS_DIR/schema/perfgazer-schema.json"
SITE_URL="https://amadeusitgroup.github.io/spark-perf-gazer"

# ── llms.txt (index) ────────────────────────────────────────────────────

cat > "$DOCS_DIR/llms.txt" <<EOF
# PerfGazer

> Performance Gazer for Apache Spark — a configurable Spark Listener for post-mortem analysis.

## Docs

- [Getting Started]($SITE_URL/getting-started/)
- [Setup via Spark Properties]($SITE_URL/user_guide/setup_spark_properties/)
- [Setup via Code]($SITE_URL/user_guide/setup_code/)
- [Databricks]($SITE_URL/user_guide/databricks/)
- [Analyze with SQL]($SITE_URL/user_guide/analyze_sql/)
- [Analyze with Scala]($SITE_URL/user_guide/analyze_scala/)
- [Data Model Reference]($SITE_URL/user_guide/data_model/)
- [Contributor Guide]($SITE_URL/contributor_guide/)

## Data Model

- [Schema (JSON)]($SITE_URL/schema/perfgazer-schema.json)
EOF

# ── llms-full.txt (all docs concatenated) ────────────────────────────────

FULL="$DOCS_DIR/llms-full.txt"
echo "# PerfGazer — Full Documentation" > "$FULL"
echo "" >> "$FULL"

# Concatenate all markdown docs in nav order
for f in \
  "$DOCS_DIR/index.md" \
  "$DOCS_DIR/getting-started.md" \
  "$DOCS_DIR/user_guide/index.md" \
  "$DOCS_DIR/user_guide/setup_spark_properties.md" \
  "$DOCS_DIR/user_guide/setup_code.md" \
  "$DOCS_DIR/user_guide/databricks.md" \
  "$DOCS_DIR/user_guide/analyze_sql.md" \
  "$DOCS_DIR/user_guide/analyze_scala.md" \
  "$DOCS_DIR/user_guide/data_model.md" \
  "$DOCS_DIR/contributor_guide.md"; do
  if [ -f "$f" ]; then
    cat "$f" >> "$FULL"
    printf '\n\n---\n\n' >> "$FULL"
  fi
done

# Append the JSON schema
if [ -f "$SCHEMA_FILE" ]; then
  echo "# Data Model Schema (JSON)" >> "$FULL"
  echo "" >> "$FULL"
  echo '```json' >> "$FULL"
  cat "$SCHEMA_FILE" >> "$FULL"
  echo '```' >> "$FULL"
fi

echo "Generated $DOCS_DIR/llms.txt and $FULL"
