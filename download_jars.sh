#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# LDP Tax Pipeline — JAR Downloader & Installer
#
# Downloads the Hadoop connector JARs needed for:
#   • S3   → hadoop-aws + aws-java-sdk-bundle (v1) + bundle (v2)
#   • ADLS → hadoop-azure
#
# Then copies them to $SPARK_HOME/jars/ so the Spark JVM can find them.
# This is required because the Homebrew Spark launch script only loads
# JARs from $SPARK_HOME/jars/ — there is no SPARK_CLASSPATH hook.
#
# Run ONCE before using cloud mode:
#   ./download_jars.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
JAR_DIR="${SCRIPT_DIR}/jars"
SPARK_HOME="${SPARK_HOME:-/opt/homebrew/opt/apache-spark/libexec}"
SPARK_JARS_DIR="${SPARK_HOME}/jars"

mkdir -p "$JAR_DIR"

# ── Maven Central base URL ────────────────────────────────────────────────────
MVN="https://repo1.maven.org/maven2"

# ── JAR list (group/artifact/version) ────────────────────────────────────────
# Spark 4.1 bundles hadoop-client-runtime-3.4.2, so hadoop-aws must match 3.4.2.
# hadoop-aws 3.4.2 requires AWS SDK v2 >= 2.26.0 (added crossRegionAccessEnabled).
# aws-java-sdk-bundle 1.12.367 is already shipped by Spark — no need to add again.
declare -a JARS=(
    "org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar"
    "software/amazon/awssdk/bundle/2.26.0/bundle-2.26.0.jar"
    "org/apache/hadoop/hadoop-azure/3.4.2/hadoop-azure-3.4.2.jar"
)

# ── Step 1: Download ──────────────────────────────────────────────────────────
echo "Step 1/2 — Downloading JARs to ${JAR_DIR}/"
echo ""
for JAR_PATH in "${JARS[@]}"; do
    JAR_FILE=$(basename "$JAR_PATH")
    DEST="${JAR_DIR}/${JAR_FILE}"
    if [ -f "$DEST" ]; then
        echo "  [SKIP] ${JAR_FILE} (already in jars/)"
    else
        echo "  [GET]  ${JAR_FILE}..."
        curl -fsSL --retry 3 -o "$DEST" "${MVN}/${JAR_PATH}"
        echo "  [OK]   ${JAR_FILE}"
    fi
done

# ── Step 2: Install into SPARK_HOME/jars/ ─────────────────────────────────────
echo ""
echo "Step 2/2 — Installing JARs to ${SPARK_JARS_DIR}/"
if [ ! -d "$SPARK_JARS_DIR" ]; then
    echo "  [ERROR] SPARK_HOME/jars not found: ${SPARK_JARS_DIR}"
    echo "          Set SPARK_HOME and re-run."
    exit 1
fi

for JAR_PATH in "${JARS[@]}"; do
    JAR_FILE=$(basename "$JAR_PATH")
    SRC="${JAR_DIR}/${JAR_FILE}"
    DST="${SPARK_JARS_DIR}/${JAR_FILE}"
    if [ -f "$DST" ]; then
        echo "  [SKIP] ${JAR_FILE} (already in SPARK_HOME/jars/)"
    else
        cp "$SRC" "$DST"
        echo "  [OK]   ${JAR_FILE} → ${SPARK_JARS_DIR}/"
    fi
done

echo ""
echo "════════════════════════════════════════════════════════════════"
echo " JARs installed successfully!"
echo " SPARK_HOME/jars : ${SPARK_JARS_DIR}"
echo "════════════════════════════════════════════════════════════════"
