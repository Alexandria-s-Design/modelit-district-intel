#!/bin/bash
# ModelIt Daily District Research Orchestrator
# Called by OpenClaw cron: morning (10 districts) and evening (10 districts)
# Runs Claude Code in non-interactive mode to research each district
set -euo pipefail

BATCH_SIZE=${1:-10}
REPO_DIR="/root/modelit-district-intel"
SCRIPTS_DIR="$REPO_DIR/scripts"
PROMPT_FILE="$REPO_DIR/DISTRICT-INTEL-PROMPT.md"
LOG_FILE="$REPO_DIR/data/research-log.jsonl"

# Ensure we're up to date
cd "$REPO_DIR" && git pull --rebase 2>/dev/null || true

# Get next batch of unresearched districts by priority
DISTRICTS=$(python3 "$SCRIPTS_DIR/get-next-batch.py" --count "$BATCH_SIZE" --status unresearched)

if [ -z "$DISTRICTS" ]; then
    echo "No more unresearched districts. All done!"
    exit 0
fi

TOTAL=$(echo "$DISTRICTS" | wc -l)
echo "Starting research batch: $TOTAL districts"
echo "$DISTRICTS"
echo "---"

SUCCESS=0
FAILED=0

while IFS= read -r DISTRICT; do
    [ -z "$DISTRICT" ] && continue

    echo ""
    echo "=========================================="
    echo "Researching: $DISTRICT"
    echo "=========================================="

    # Generate slug for directory name
    SLUG=$(echo "$DISTRICT" | tr '[:upper:]' '[:lower:]' | sed 's/ /-/g' | sed 's/[^a-z0-9-]//g')

    # Get CDE baseline data for this district
    BASELINE=$(python3 "$SCRIPTS_DIR/get-district-data.py" "$DISTRICT" 2>/dev/null || echo "No CDE data found.")

    # Build the prompt: substitute district name + append CDE data
    PROMPT=$(sed "s/\[DISTRICT NAME\]/$DISTRICT/g; s/\[STATE\]/California/g" "$PROMPT_FILE")
    PROMPT="$PROMPT

---

## Pre-loaded CDE Baseline Data

The following verified data from the California Department of Education has been pre-loaded.
Use this as your starting baseline — verify and expand with web research.

$BASELINE"

    # Run Claude Code in non-interactive mode
    START_TIME=$(date +%s)

    if claude -p "$PROMPT" \
        --allowedTools "Bash,Read,Write,Edit,Glob,Grep,WebSearch,WebFetch" \
        --dangerously-skip-permissions \
        --model sonnet \
        --max-budget-usd 2.00 \
        --no-session-persistence 2>&1; then

        END_TIME=$(date +%s)
        DURATION=$((END_TIME - START_TIME))

        # Update master list status
        python3 "$SCRIPTS_DIR/update-status.py" "$DISTRICT" researched

        # Stage and commit this district
        git add "districts/$SLUG/" data/ comparison.xlsx pipeline.md 2>/dev/null || true
        git add -A "districts/$SLUG/" 2>/dev/null || true
        git commit -m "Add $DISTRICT district intelligence profile" 2>/dev/null || true
        git push 2>/dev/null || true

        # Log success
        echo "{\"timestamp\":\"$(date -Iseconds)\",\"district\":\"$DISTRICT\",\"status\":\"success\",\"duration_sec\":$DURATION}" >> "$LOG_FILE"

        SUCCESS=$((SUCCESS + 1))
        echo "SUCCESS: $DISTRICT (${DURATION}s)"
    else
        END_TIME=$(date +%s)
        DURATION=$((END_TIME - START_TIME))

        # Log failure
        echo "{\"timestamp\":\"$(date -Iseconds)\",\"district\":\"$DISTRICT\",\"status\":\"failed\",\"duration_sec\":$DURATION}" >> "$LOG_FILE"

        FAILED=$((FAILED + 1))
        echo "FAILED: $DISTRICT (${DURATION}s)"
    fi

done <<< "$DISTRICTS"

# Push data updates (status changes, logs)
cd "$REPO_DIR"
git add data/ 2>/dev/null || true
git commit -m "Update research status: $SUCCESS succeeded, $FAILED failed" 2>/dev/null || true
git push 2>/dev/null || true

echo ""
echo "=========================================="
echo "Batch complete: $SUCCESS/$TOTAL succeeded, $FAILED failed"
echo "=========================================="
