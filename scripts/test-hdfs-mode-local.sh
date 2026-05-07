#!/usr/bin/env bash
# ============================================================================
# Local end-to-end test for PerfGazer HDFS mode.
#
# Uses spark.extraListeners with dbfs:/ scheme backed by a test FileSystem
# that delegates to the local disk. Verifies:
#   1. PerfGazer initializes without crashing (lazy init via extraListeners)
#   2. Reports are promoted to the remote destination after a workload
#   3. normalizePath preserves URI scheme double-slash
#
# Prerequisites:
#   - spark-shell on PATH (Spark 3.5.x)
#   - PerfGazer JAR built: sbt "core/clean" "core/package"
#   - Test JAR built: sbt "core/test:package"
#
# Usage:
#   ./scripts/test-hdfs-mode-local.sh
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Find the main JAR
JAR=$(find "$PROJECT_DIR/core/target" -name "perfgazer_spark_*.jar" -not -path "*/test-classes/*" -not -name "*tests*" | head -1)
if [ -z "$JAR" ]; then
  echo "ERROR: PerfGazer JAR not found. Run: sbt 'core/clean' 'core/package'"
  exit 1
fi

# Find the test JAR (contains TestDbfsFileSystem)
TEST_JAR=$(find "$PROJECT_DIR/core/target" -name "*tests.jar" | head -1)
if [ -z "$TEST_JAR" ]; then
  # Try building it
  echo "Test JAR not found, building..."
  (cd "$PROJECT_DIR" && sbt "core/Test/package" 2>/dev/null)
  TEST_JAR=$(find "$PROJECT_DIR/core/target" -name "*tests.jar" | head -1)
  if [ -z "$TEST_JAR" ]; then
    echo "ERROR: Test JAR not found even after build."
    exit 1
  fi
fi

echo "Using JAR: $JAR"
echo "Using test JAR: $TEST_JAR"

# Output directory
OUTPUT_DIR="/tmp/perfgazer-hdfs-mode-test"
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

# Scala script to run inside spark-shell
SCALA_SCRIPT=$(mktemp /tmp/perfgazer-test.XXXXXX)
cat > "$SCALA_SCRIPT" << 'SCALA'
import com.amadeus.perfgazer.PathBuilder._
import com.amadeus.perfgazer.PerfGazer

// 1. Verify normalizePath fix
val testUri = "abfss://container@account.dfs.core.windows.net/path/"
val normalized = testUri.normalizePath
assert(normalized == testUri, s"normalizePath corrupted URI: $normalized")
println(s"[OK] normalizePath preserves URI scheme: $normalized")

// 2. Run a workload (PerfGazer is already attached via extraListeners)
println("[INFO] Running sample workload...")
spark.range(10000).selectExpr("id", "id % 5 as group", "rand() as value")
  .groupBy("group").sum("value").collect()

// 3. Wait for async flush and close
Thread.sleep(3000)
PerfGazer.instance match {
  case Some(pg) =>
    spark.sparkContext.removeSparkListener(pg)
    pg.close()
    println("[OK] PerfGazer closed successfully")
  case None =>
    println("[FAIL] PerfGazer instance not found")
    System.exit(1)
}

// 4. Check output files at the remote destination
val appId = spark.sparkContext.applicationId
val outputDir = new java.io.File(s"/tmp/perfgazer-hdfs-mode-test/applicationId=$appId")
val files = Option(outputDir.listFiles()).getOrElse(Array.empty).filter(_.isFile)

if (files.isEmpty) {
  println(s"[FAIL] No report files found in ${outputDir.getAbsolutePath}")
  // Check staging dir for debugging
  val stagingDir = new java.io.File(s"/tmp/perfgazer/$appId")
  val stagingFiles = Option(stagingDir.listFiles()).getOrElse(Array.empty)
  if (stagingFiles.nonEmpty) {
    println(s"[INFO] Files stuck in staging dir ${stagingDir.getAbsolutePath}:")
    stagingFiles.foreach(f => println(s"       ${f.getName} (${f.length()} bytes)"))
    println("[INFO] Promotion failed silently. Check logs above for errors.")
  }
  System.exit(1)
}

println(s"[OK] Found ${files.length} report file(s) in ${outputDir.getAbsolutePath}:")
files.foreach(f => println(s"     ${f.getName} (${f.length()} bytes)"))

val hasContent = files.exists(_.length() > 0)
if (!hasContent) {
  println("[FAIL] All files are empty")
  System.exit(1)
}

println("\n[SUCCESS] All checks passed. HDFS mode works end-to-end locally.")
System.exit(0)
SCALA

echo "Running spark-shell with extraListeners (dbfs:/ → local filesystem)..."
echo "Remote destination: dbfs:$OUTPUT_DIR/applicationId={{spark.app.id}}/"
echo ""

spark-shell \
  --jars "$JAR,$TEST_JAR" \
  --conf spark.driver.bindAddress=127.0.0.1 \
  --conf spark.driver.host=127.0.0.1 \
  --conf spark.ui.enabled=false \
  --conf spark.hadoop.fs.dbfs.impl=com.amadeus.perfgazer.TestDbfsFileSystem \
  --conf spark.hadoop.fs.dbfs.impl.disable.cache=true \
  --conf "spark.extraListeners=com.amadeus.perfgazer.PerfGazer" \
  --conf "spark.perfgazer.sink.class=com.amadeus.perfgazer.JsonSink" \
  --conf "spark.perfgazer.sink.json.destination=dbfs:$OUTPUT_DIR/applicationId={{spark.app.id}}/" \
  -I "$SCALA_SCRIPT" 2>&1 | grep -E "^\[OK\]|^\[FAIL\]|^\[INFO\]|^\[SUCCESS\]|^     |ERROR.*[Pp]romot|Wrong FS"

EXIT_CODE=${PIPESTATUS[0]}

# Cleanup
rm -f "$SCALA_SCRIPT"

if [ $EXIT_CODE -eq 0 ]; then
  echo ""
  echo "Test passed. Cleaning up output..."
  rm -rf "$OUTPUT_DIR"
else
  echo ""
  echo "Test FAILED (exit code $EXIT_CODE). Output retained at: $OUTPUT_DIR"
fi

exit $EXIT_CODE
