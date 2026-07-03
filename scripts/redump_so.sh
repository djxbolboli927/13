#!/usr/bin/env bash
# Re-dump every DEX program .so listed in src/program_registry.rs from the
# cluster's *current* deployed version. Run this whenever the simulator
# reports unexpected "InvalidInstruction"-class errors -- they usually mean
# the local .so is older than what mainnet is now executing.
#
# Usage:
#   ./scripts/redump_so.sh <RPC_URL> <OUTPUT_DIR>
#
# Example:
#   ./scripts/redump_so.sh https://api.mainnet-beta.solana.com /home/soluser/m/so
#
# Notes:
#   - You need the `solana` CLI on PATH (any recent version is fine; the
#     `program dump` subcommand has been stable for years).
#   - For closed/upgrade-frozen programs the dump still works -- we only
#     need read access, not authority.
#   - Failures (program closed, RPC rate-limited) are logged but do not
#     abort the run; partial mapping is still better than nothing.

set -uo pipefail

if [ "$#" -lt 2 ]; then
    echo "usage: $0 <RPC_URL> <OUTPUT_DIR>" >&2
    exit 1
fi

RPC="$1"
OUT_DIR="$2"
mkdir -p "$OUT_DIR"

REGISTRY="$(dirname "$0")/../src/program_registry.rs"
if [ ! -f "$REGISTRY" ]; then
    echo "program_registry.rs not found at $REGISTRY" >&2
    exit 1
fi

# Parse lines of the form ("PUBKEY", "FILENAME"), ignoring comments / empty.
mapfile -t PAIRS < <(
    grep -E '^\s*\("[1-9A-HJ-NP-Za-km-z]+",\s*"[^"]+"\)' "$REGISTRY" \
    | sed -E 's/^\s*\("([^"]+)",\s*"([^"]+)"\).*/\1 \2/'
)

if [ "${#PAIRS[@]}" -eq 0 ]; then
    echo "no program entries parsed from $REGISTRY" >&2
    exit 1
fi

ok=0; fail=0
for pair in "${PAIRS[@]}"; do
    pid="${pair% *}"
    fname="${pair#* }"
    out="$OUT_DIR/$fname"
    printf '[%s] dumping -> %s ... ' "$pid" "$out"
    if solana program dump -u "$RPC" "$pid" "$out" >/dev/null 2>&1; then
        size=$(stat -c%s "$out" 2>/dev/null || echo 0)
        echo "ok (${size} bytes)"
        ok=$((ok+1))
    else
        echo "FAILED"
        fail=$((fail+1))
    fi
done

echo
echo "summary: ${ok} dumped, ${fail} failed, total ${#PAIRS[@]}"
