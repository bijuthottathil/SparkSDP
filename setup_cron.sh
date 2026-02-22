#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# LDP Tax Pipeline — Cron Job Installer (macOS)
#
# Installs a cron entry that runs run.sh on a schedule.
# Default: daily at 02:00.  Override with env vars:
#
#   CRON_SCHEDULE="0 6 * * *" ./setup_cron.sh   # daily at 06:00
#   CRON_SCHEDULE="0 */6 * * *" ./setup_cron.sh # every 6 hours
#   REMOVE_CRON=true ./setup_cron.sh            # remove the job
#
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RUN_SCRIPT="${SCRIPT_DIR}/run.sh"
CRON_LOG="${SCRIPT_DIR}/logs/cron.log"
CRON_SCHEDULE="${CRON_SCHEDULE:-25 17 * * *}"  # default: 17:16 daily
CRON_TAG="# LDP_TAX_PIPELINE"                  # marker to find our entry

# ── Ensure run.sh is executable ───────────────────────────────────────────────
chmod +x "$RUN_SCRIPT"

# ── Remove mode ───────────────────────────────────────────────────────────────
if [ "${REMOVE_CRON:-false}" = "true" ]; then
    echo "[INFO] Removing LDP Tax Pipeline cron job..."
    FILTERED=$(crontab -l 2>/dev/null | grep -v "$CRON_TAG" | grep -v "$RUN_SCRIPT" || true)
    echo "$FILTERED" | crontab -
    echo "[DONE] Cron job removed."
    crontab -l 2>/dev/null || echo "(crontab is now empty)"
    exit 0
fi

# ── Build cron entry ──────────────────────────────────────────────────────────
#   ┌──── minute (0-59)
#   │  ┌─── hour (0-23)
#   │  │ ┌── day-of-month (1-31)
#   │  │ │ ┌─ month (1-12)
#   │  │ │ │ ┌ day-of-week (0-7; 0 & 7 = Sunday)
#   │  │ │ │ │
#   0  2 * * *   → every day at 02:00
CRON_LINE="${CRON_SCHEDULE} ${RUN_SCRIPT} >> ${CRON_LOG} 2>&1 ${CRON_TAG}"

# ── Install (idempotent — removes old entry first) ────────────────────────────
echo "[INFO] Installing cron job..."
mkdir -p "${SCRIPT_DIR}/logs"

# Filter out any existing entry, then append the new one
# Use '|| true' to prevent BSD grep exit-1 when the pattern isn't found
EXISTING=$(crontab -l 2>/dev/null | grep -v "$CRON_TAG" | grep -v "$RUN_SCRIPT" || true)
printf "%s\n%s\n" "$EXISTING" "$CRON_LINE" | crontab -

echo ""
echo "════════════════════════════════════════════════════════════════"
echo " Cron job installed successfully!"
echo " Schedule : ${CRON_SCHEDULE}"
echo " Command  : ${RUN_SCRIPT}"
echo " Log      : ${CRON_LOG}"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "Current crontab:"
crontab -l
echo ""
echo "Tips:"
echo "  • View recent log  : tail -f ${CRON_LOG}"
echo "  • Run now          : ${RUN_SCRIPT}"
echo "  • Local mode test  : LOCAL_MODE=true ${RUN_SCRIPT}"
echo "  • Remove job       : REMOVE_CRON=true ${SCRIPT_DIR}/setup_cron.sh"
