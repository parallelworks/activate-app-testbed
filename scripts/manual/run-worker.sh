#!/bin/bash
# Launch a testbed run from the static inputs file using only the pw CLI,
# follow it to completion, and print the dispatch log. Set PW_CONTEXT to
# override the pw context.
#
# Usage: ./run-worker.sh [inputs.json] [workflow-name]
set -e
INPUTS=${1:-$(dirname "$0")/worker-inputs.json}
WORKFLOW=${2:-app-testbed}
PW="pw${PW_CONTEXT:+ --context $PW_CONTEXT}"

[ -f "$INPUTS" ] || { echo "ERROR: inputs file not found: $INPUTS"; exit 1; }
if grep -qE "OWNER|REPLACE" "$INPUTS"; then
  echo "ERROR: $INPUTS still contains placeholders."
  echo "Regenerate it for your clusters with:"
  echo "  $(dirname "$0")/../programmatic/launch-worker.py --server-host <cluster> --site <cluster> --print-inputs > $INPUTS"
  exit 1
fi

SLUG=$($PW workflows run "$WORKFLOW" -i "$INPUTS" -o json \
  | python3 -c 'import json,sys; r=json.load(sys.stdin); print(r.get("run",r).get("slug",""))')
[ -n "$SLUG" ] || { echo "ERROR: could not determine run slug from pw output"; exit 1; }
echo "launched: $SLUG"

LAST=""
for i in $(seq 1 90); do
  ST=$($PW workflows runs view "$SLUG" -o json 2>/dev/null \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("status",""))' || true)
  [ "$ST" != "$LAST" ] && echo "  status: ${ST:-unknown}"
  LAST=$ST
  case "$ST" in running|pending|queued|"") sleep 10 ;; *) break ;; esac
done

echo ""
echo "--- dispatch log tail ---"
$PW workflows runs logs "$SLUG" 2>/dev/null | sed 's/\x1b\[[0-9;]*m//g' | tail -30
[ "$ST" = "completed" ] && { echo "run $SLUG completed"; exit 0; }
echo "run $SLUG ended with status: ${ST:-unknown}"
$PW workflows runs errors "$SLUG" 2>/dev/null | tail -20
exit 1
