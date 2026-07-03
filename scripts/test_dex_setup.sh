#!/usr/bin/env bash
# Standalone script to dump DEX programs and fetch ALT addresses for testing.
# This script is independent from the main bot and can be run manually.
#
# Usage:
#   ./scripts/test_dex_setup.sh <RPC_URL> <OUTPUT_DIR>
#
# Example:
#   ./scripts/test_dex_setup.sh https://api.mainnet-beta.solana.com /home/soluser/m/so

set -uo pipefail

if [ "$#" -lt 2 ]; then
    echo "usage: $0 <RPC_URL> <OUTPUT_DIR>" >&2
    exit 1
fi

RPC="$1"
OUT_DIR="$2"
mkdir -p "$OUT_DIR"

echo "=== DEX Program Registry Test Script ==="
echo "RPC: $RPC"
echo "Output Dir: $OUT_DIR"
echo ""

# List of DEX program IDs from your DEX_PROGRAM_IDS
DEX_PROGRAMS=(
    "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG:Meteora_DAMM_v2.so"
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK:Raydium_Concentrated_Liquidity.so"
    "MNFSTqtC93rEfYHB6hF82sKdZpUDFWkViLByLd1k1Ms:Manifest.so"
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc:Whirlpools_Program.so"
    "BSwp6bEBihVLdqJRKGgzjcGLHkcTuzmSo1TQkHepzH8p:BonkSwap.so"
    "fUSioN9YKKSa3CUC2YUc4tPkHJ5Y6XW1yz8y6F7qWz9:Fusion_AMM.so"
    "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP:Meteora_Pools_Program.so"
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo:Meteora_DLMM_Program.so"
    "DEXYosS6oEGvk8uCDayvwEZz4qEyDJRf9nFgYCaqPMTm:1Dex_Program.so"
    "24Uqj9JCLxUeoC3hGfh5W3s9FM9uCHDS2SG3LYwBpyTi:Invariant_Swap.so"
    "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB:PancakeSwap.so"
    "HyaB3W9q6XdA5xwpU4XnSZV94htfmbmqJXZcEbRaJutt:Meteora_Vault_Program.so"
    "MERLuDFBMmsHnsBPZw2sDQZHvXFMwp8EdjudcU2HKky:Mercurial_Stable_Swap.so"
    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C:Raydium_CPMM.so"
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4:Jupiter_Aggregator_v6.so"
)

ok=0; fail=0
echo "=== Dumping DEX Programs ==="
for entry in "${DEX_PROGRAMS[@]}"; do
    pid="${entry%%:*}"
    fname="${entry##*:}"
    out="$OUT_DIR/$fname"
    printf '[%s] dumping -> %s ... ' "$pid" "$fname"
    if solana program dump -u "$RPC" "$pid" "$out" >/dev/null 2>&1; then
        size=$(stat -c%s "$out" 2>/dev/null || echo 0)
        echo "ok (${size} bytes)"
        ok=$((ok+1))
    else
        echo "FAILED"
        fail=$((fail+1))
    fi
done

echo ""
echo "=== Program Summary: ${ok} dumped, ${fail} failed, total ${#DEX_PROGRAMS[@]} ==="
echo ""

# Now fetch account info for each program to check if they exist
echo "=== Verifying Program Accounts ==="
for entry in "${DEX_PROGRAMS[@]}"; do
    pid="${entry%%:*}"
    printf '[%s] checking account... ' "$pid"
    if solana account "$pid" -u "$RPC" --output json 2>/dev/null | jq -e '.type == "program"' >/dev/null 2>&1; then
        echo "OK (is program)"
    else
        echo "WARNING (not a program or RPC error)"
    fi
done

echo ""
echo "=== Fetching Common ALT Addresses ==="
# Common ALTs used by Jupiter/Metis
# These are well-known ALTs that Jupiter uses
JUPITER_ALTS=(
    "3AL7kDjdMADqmDc1piGtDuWzQfbFqXzV8YxTJ8G6qpump"
    "GbPYrQEDYV9Bn4Kp9oT3h5sQSYFpwLyhpD7PJE8xgRHX"
    "CRaGFDCkqdPB7FnJpJfzvvRoEeT9pqxFXFoipPxZ7Jst"
    "DRpbCBLoTZy9XyZKJxMcmFA3AJR45mNqRhC5SkraShyc"
    "FTpcGfsvXwGRcqjSaBNoxNYFSYeKgQVCuKtHkRvxXtpu"
)

for alt in "${JUPITER_ALTS[@]}"; do
    printf '[%s] fetching ALT info... ' "$alt"
    if solana account "$alt" -u "$RPC" --output json 2>/dev/null | jq -e '.type == "addressLookupTable"' >/dev/null 2>&1; then
        echo "OK (is ALT)"
        # Show how many addresses are in the ALT
        count=$(solana account "$alt" -u "$RPC" --output json 2>/dev/null | jq '.data[0] | length / 32 | floor' 2>/dev/null || echo "?")
        echo "       ~${count} addresses in table"
    else
        echo "WARNING (not an ALT or RPC error)"
    fi
done

echo ""
echo "=== Test Complete ==="
echo "Check $OUT_DIR for dumped .so files"
