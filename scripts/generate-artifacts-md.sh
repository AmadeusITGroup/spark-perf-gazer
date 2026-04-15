#!/usr/bin/env bash
# Generate docs/artifacts.md from .github/artifact-matrix.json.
# The output is a compatibility table plus sbt / Maven / Gradle snippets for each artifact.
# Usage: ./scripts/generate-artifacts-md.sh [docs-dir] [matrix-json]

set -euo pipefail

DOCS_DIR="${1:-docs}"
MATRIX_JSON="${2:-.github/artifact-matrix.json}"
OUT="$DOCS_DIR/artifacts.md"
GROUP_ID="io.github.amadeusitgroup"

if [ ! -f "$MATRIX_JSON" ]; then
  echo "ERROR: matrix file not found: $MATRIX_JSON" >&2
  exit 1
fi

# ── Helper: Scala binary version (2.12.20 → 2.12) ─────────────────────
scala_bin() { echo "$1" | sed 's/\([0-9]*\.[0-9]*\)\..*/\1/'; }

# ── Helper: Spark id suffix (3.5.2 → 3-5-2) ───────────────────────────
spark_id() { echo "$1" | tr '.' '-'; }

# ── Read matrix rows ───────────────────────────────────────────────────
# Each line: spark|scala|java|dbr
ROWS=$(jq -r '.[] | "\(.spark)|\(.scala)|\(.java)|\(.dbr)"' "$MATRIX_JSON")

if [ -z "$ROWS" ]; then
  echo "ERROR: no entries found in $MATRIX_JSON" >&2
  exit 1
fi

# ── Write the page ──────────────────────────────────────────────────────

cat > "$OUT" <<'HEADER'
# Available Artifacts

<!-- Auto-generated from .github/artifact-matrix.json — do not edit by hand. -->

PerfGazer publishes a separate artifact for every supported Spark / Scala
combination. Pick the row that matches your cluster and use the coordinates
below.

## Compatibility Matrix

| Spark | Scala | Java | Databricks Runtime |
|-------|-------|------|--------------------|
HEADER

# Table rows
while IFS='|' read -r spark scala java dbr; do
  scbin=$(scala_bin "$scala")
  printf '| %s | %s | %s | %s |\n' "$spark" "$scbin" "$java" "${dbr:+$dbr}"
done <<< "$ROWS" >> "$OUT"

# Dependency snippets per artifact
cat >> "$OUT" <<'SECTION'

## Dependency Coordinates

Replace **`VERSION`** with the latest release:
![GitHub Release](https://img.shields.io/github/v/release/AmadeusITGroup/spark-perf-gazer)

SECTION

while IFS='|' read -r spark scala java dbr; do
  scbin=$(scala_bin "$scala")
  sid=$(spark_id "$spark")
  artifact="perfgazer_spark_${sid}_${scbin}"

  cat >> "$OUT" <<EOF
### Spark ${spark} — Scala ${scbin}

EOF

  if [ -n "$dbr" ]; then
    cat >> "$OUT" <<EOF
Databricks Runtime **${dbr}** · Java ${java}

EOF
  else
    cat >> "$OUT" <<EOF
Java ${java}

EOF
  fi

  cat >> "$OUT" <<EOF
=== "spark-shell / spark-submit"

    \`\`\`bash
    --packages ${GROUP_ID}:${artifact}:VERSION
    \`\`\`

=== "sbt"

    \`\`\`scala
    libraryDependencies += "${GROUP_ID}" %% "${artifact}" % "VERSION"
    \`\`\`

=== "Maven"

    \`\`\`xml
    <dependency>
        <groupId>${GROUP_ID}</groupId>
        <artifactId>${artifact}</artifactId>
        <version>VERSION</version>
    </dependency>
    \`\`\`

=== "Gradle"

    \`\`\`groovy
    implementation '${GROUP_ID}:${artifact}:VERSION'
    \`\`\`

EOF
done <<< "$ROWS"

echo "Generated $OUT"
