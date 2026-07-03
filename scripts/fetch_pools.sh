#!/usr/bin/env bash
# Fetch all pool/vault accounts for a given DEX program from mainnet RPC.
#
# This is a STANDALONE tool for operators to pre-populate or inspect pool
# state before (or independently of) the bot. It uses getProgramAccounts
# which returns every account owned by the given program.
#
# Usage:
#   ./scripts/fetch_pools.sh <RPC_URL> <PROGRAM_ID> [OUTPUT_DIR]
#
# Examples:
#   # Fetch all Meteora DLMM pools to stdout (count only)
#   ./scripts/fetch_pools.sh https://api.mainnet-beta.solana.com LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo
#
#   # Save raw JSON response to a file
#   ./scripts/fetch_pools.sh https://api.mainnet-beta.solana.com LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo ./pools
#
#   # Fetch all pools for every DEX in program_registry.rs
#   ./scripts/fetch_pools.sh --all https://api.mainnet-beta.solana.com ./pools
#
# Notes:
#   - getProgramAccounts is an expensive RPC call. Many public endpoints
#     rate-limit or outright block it. Use a dedicated/paid RPC.
#   - The --all flag iterates over every program in program_registry.rs
#     and saves each result as <PROGRAM_ID>.json in OUTPUT_DIR.
#   - Output is raw JSON from RPC. Each entry contains pubkey + account
#     data (base64-encoded). Parse with jq or a custom tool.

set -uo pipefail

REGISTRY="$(dirname "$0")/../src/program_registry.rs"

fetch_program_accounts() {
    local rpc="$1"
    local pid="$2"
    local payload
    payload=$(cat <<ENDJSON
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "getProgramAccounts",
  "params": [
    "$pid",
    {
      "encoding": "base64",
      "commitment": "confirmed"
    }
  ]
}
ENDJSON
)
    curl -s -X POST "$rpc" \
        -H "Content-Type: application/json" \
        -d "$payload"
}

count_accounts() {
    local json="$1"
    if command -v jq &>/dev/null; then
        echo "$json" | jq '.result | length' 2>/dev/null || echo "?"
    else
        echo "?(install jq for count)"
    fi
}

if [ "${1:-}" = "--all" ]; then
    if [ "$#" -lt 3 ]; then
        echo "usage: $0 --all <RPC_URL> <OUTPUT_DIR>" >&2
        exit 1
    fi
    RPC="$2"
    OUT_DIR="$3"
    mkdir -p "$OUT_DIR"

    if [ ! -f "$REGISTRY" ]; then
        echo "program_registry.rs not found at $REGISTRY" >&2
        exit 1
    fi

    mapfile -t PAIRS < <(
        grep -E '^\s*\("[1-9A-HJ-NP-Za-km-z]+",\s*"[^"]+"\)' "$REGISTRY" \
        | sed -E 's/^\s*\("([^"]+)",\s*"([^"]+)"\).*/\1 \2/'
    )

    if [ "${#PAIRS[@]}" -eq 0 ]; then
        echo "no program entries parsed from $REGISTRY" >&2
        exit 1
    fi

    total=0
    for pair in "${PAIRS[@]}"; do
        pid="${pair% *}"
        fname="${pair#* }"
        out="$OUT_DIR/${pid}.json"
        printf '[%s] (%s) fetching ... ' "$pid" "$fname"
        result=$(fetch_program_accounts "$RPC" "$pid")
        echo "$result" > "$out"
        n=$(count_accounts "$result")
        printf '%s accounts -> %s\n' "$n" "$out"
        total=$((total+1))
        sleep 1
    done
    echo
    echo "done: fetched $total programs to $OUT_DIR/"
    exit 0
fi

if [ "$#" -lt 2 ]; then
    echo "usage: $0 <RPC_URL> <PROGRAM_ID> [OUTPUT_DIR]" >&2
    echo "       $0 --all <RPC_URL> <OUTPUT_DIR>" >&2
    exit 1
fi

RPC="$1"
PID="$2"
OUT_DIR="${3:-}"

printf 'fetching getProgramAccounts for %s ... ' "$PID"
result=$(fetch_program_accounts "$RPC" "$PID")

n=$(count_accounts "$result")
echo "$n accounts"

if [ -n "$OUT_DIR" ]; then
    mkdir -p "$OUT_DIR"
    out="$OUT_DIR/${PID}.json"
    echo "$result" > "$out"
    echo "saved to $out"
else
    echo "$result"
fi
