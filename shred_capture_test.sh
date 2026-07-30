#!/usr/bin/env bash
# ============================================================================
#  shred_capture_test.sh
#  ---------------------------------------------------------------------------
#  یک تستِ مستقل (بدونِ کدِ ربات) که مستقیماً jito-shredstream-proxy را با
#  کیف‌پولِ وایت‌لیستِ خودت اجرا می‌کند و اندازه می‌گیرد جیتو در واقع چند
#  شرد در ثانیه به سرورِ تو می‌دهد. هدف: جواب به این سوال —
#      «محدودیتِ نرسیدنِ شردز از سرورِ من است یا نه؟»
#
#  دو سنجهٔ مستقل می‌گیرد:
#    (A) شمارندهٔ داخلیِ خودِ پروکسی  (received / sent / duplicate)
#    (B) شمارشِ خامِ پکت‌های UDP روی پورتِ dest (پکت‌های شردِ بازسازی‌شده)
#
#  طرزِ اجرا (مستقیم در ترمینال):
#      bash shred_capture_test.sh              # ۶۰ ثانیه، تنظیماتِ config.toml
#      DURATION=120 bash shred_capture_test.sh # ۱۲۰ ثانیه
#
#  ⚠️ قبل از اجرا، رباتِ اصلی را STOP کن — جیتو معمولاً فقط یک اتصالِ
#     ShredStream به‌ازای هر کیفِ وایت‌لیست می‌دهد، و پورت‌ها هم مشترک‌اند.
# ============================================================================
set -u

# ── محلِ config.toml (کنارِ همین اسکریپت، وگرنه مسیرِ فعلی) ────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG="${CONFIG:-$SCRIPT_DIR/config.toml}"

# ── یک مقدار را از config.toml بخوان (اولین کلیدِ غیرکامنت) ────────────────
cfg() { # cfg <key> <default>
  local v=""
  if [[ -f "$CONFIG" ]]; then
    v="$(grep -E "^[[:space:]]*$1[[:space:]]*=" "$CONFIG" \
         | grep -v '^[[:space:]]*#' | head -n1 \
         | sed -E 's/^[^=]*=[[:space:]]*//; s/^"//; s/"[[:space:]]*$//; s/[[:space:]]*#.*$//')"
  fi
  echo "${v:-$2}"
}

PROXY_BIN="${PROXY_BIN:-$(cfg proxy_bin /root/a/jito-tools/jito-shredstream-proxy)}"
KEYPAIR="${KEYPAIR:-$(cfg shred_keypair /root/g/wallet/shred.json)}"
BLOCK_ENGINE="${BLOCK_ENGINE:-$(cfg block_engine_url https://mainnet.block-engine.jito.wtf)}"
REGIONS="${REGIONS:-$(cfg desired_regions amsterdam,frankfurt)}"
DEST="${DEST:-$(cfg proxy_dest_ip_ports 127.0.0.1:20001)}"
SRC_PORT="${SRC_PORT:-$(cfg proxy_src_bind_port 20000)}"
GRPC_PORT="${GRPC_PORT:-9999}"
DURATION="${DURATION:-60}"

DEST_HOST="${DEST%%:*}"
DEST_PORT="${DEST##*:}"

LOG="$(mktemp /tmp/shred_capture_XXXX.log)"

echo "════════════════════════════════════════════════════════════════"
echo "  ShredStream capture test"
echo "════════════════════════════════════════════════════════════════"
echo "  proxy_bin     : $PROXY_BIN"
echo "  keypair       : $KEYPAIR   (کیفِ وایت‌لیست)"
echo "  block_engine  : $BLOCK_ENGINE"
echo "  regions       : $REGIONS"
echo "  dest (UDP)    : $DEST"
echo "  grpc port     : $GRPC_PORT"
echo "  duration      : ${DURATION}s"
echo "  proxy log     : $LOG"
echo "════════════════════════════════════════════════════════════════"

# ── چک‌های اولیه ──────────────────────────────────────────────────────────
if [[ ! -x "$PROXY_BIN" ]]; then
  echo "❌ باینریِ پروکسی پیدا/اجرا نشد: $PROXY_BIN"
  echo "   PROXY_BIN=/path/to/jito-shredstream-proxy bash $0"
  exit 1
fi
if [[ ! -f "$KEYPAIR" ]]; then
  echo "❌ فایلِ کیف‌پول پیدا نشد: $KEYPAIR"
  echo "   KEYPAIR=/path/to/shred.json bash $0"
  exit 1
fi
# پورت gRPC آزاد است؟ (اگر رباتْ پروکسی را بالا نگه داشته باشد اینجا گیر می‌کند)
if command -v ss >/dev/null 2>&1 && ss -ltnp 2>/dev/null | grep -q ":$GRPC_PORT "; then
  echo "❌ پورت $GRPC_PORT اشغال است — احتمالاً رباتِ اصلی روشن است."
  echo "   اول ربات را ببند، بعد این تست را اجرا کن."
  exit 1
fi

# ── (B) شمارندهٔ خامِ پکتِ UDP روی پورتِ dest ─────────────────────────────
UDP_OUT="$(mktemp /tmp/shred_udp_XXXX.txt)"
PY_BIN="$(command -v python3 || command -v python || true)"
UDP_PID=""
if [[ -n "$PY_BIN" ]]; then
  "$PY_BIN" - "$DEST_HOST" "$DEST_PORT" "$DURATION" "$UDP_OUT" <<'PYEOF' &
import socket, sys, time
host, port, dur, out = sys.argv[1], int(sys.argv[2]), float(sys.argv[3]), sys.argv[4]
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8 * 1024 * 1024)
try:
    s.bind((host, port))
except OSError as e:
    open(out, "w").write("BIND_FAIL %s\n" % e)
    sys.exit(0)
s.settimeout(1.0)
pkts = 0; byts = 0; start = time.time(); last = start; last_pkts = 0
while time.time() - start < dur:
    try:
        d, _ = s.recvfrom(2048)
        pkts += 1; byts += len(d)
    except socket.timeout:
        pass
    now = time.time()
    if now - last >= 5:
        rate = (pkts - last_pkts) / (now - last)
        print("  [udp] +%ds  packets=%d  (%.0f pkt/s)" % (int(now-start), pkts, rate), flush=True)
        last = now; last_pkts = pkts
el = max(time.time() - start, 1e-9)
open(out, "w").write("PKTS %d BYTES %d SECS %.2f RATE %.1f\n" % (pkts, byts, el, pkts/el))
PYEOF
  UDP_PID=$!
  echo "▶ شمارندهٔ UDP روی $DEST فعال شد (pid=$UDP_PID)"
else
  echo "⚠ python پیدا نشد — فقط سنجهٔ داخلیِ پروکسی (A) گرفته می‌شود."
fi

# ── (A) اجرای پروکسی با متریکِ کامل (بدون خاموش‌کردنِ solana_metrics) ──────
echo "▶ پروکسی در حالِ اجرا برای ${DURATION}s ... (متریک‌ها روشن)"
RUST_LOG="${RUST_LOG:-info}" \
  timeout "${DURATION}s" "$PROXY_BIN" shredstream \
    --block-engine-url "$BLOCK_ENGINE" \
    --auth-keypair "$KEYPAIR" \
    --desired-regions "$REGIONS" \
    --dest-ip-ports "$DEST" \
    --src-bind-port "$SRC_PORT" \
    --grpc-service-port "$GRPC_PORT" \
    >"$LOG" 2>&1 &
PROXY_PID=$!

# پیشرفتِ زنده از رویِ لاگِ پروکسی (خطِ heartbeat هر چند ثانیه)
tail -n +1 -f "$LOG" 2>/dev/null | grep --line-buffered -iE \
  "heartbeat|received|recv_packets|packets_count|Failed|error|bind|auth" &
TAIL_PID=$!

wait "$PROXY_PID" 2>/dev/null
sleep 1
kill "$TAIL_PID"  2>/dev/null
[[ -n "$UDP_PID" ]] && wait "$UDP_PID" 2>/dev/null

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  نتیجه"
echo "════════════════════════════════════════════════════════════════"

# ── سنجهٔ (A): از لاگِ پروکسی ─────────────────────────────────────────────
echo "── (A) شمارندهٔ داخلیِ پروکسی ────────────────────────────────"
EXIT_LINE="$(grep -iE "Exiting Shredstream|received .* sent successfully" "$LOG" | tail -n1)"
if [[ -n "$EXIT_LINE" ]]; then
  echo "  $EXIT_LINE"
  RECV="$(echo "$EXIT_LINE" | grep -oE '[0-9]+ received' | grep -oE '[0-9]+' | head -n1)"
  if [[ -n "${RECV:-}" && "$DURATION" -gt 0 ]]; then
    echo "  → ≈ $((RECV / DURATION)) shreds/s (received)"
  fi
else
  echo "  (خطِ خلاصهٔ پروکسی پیدا نشد — چند خطِ آخرِ لاگ:)"
  tail -n 15 "$LOG" | sed 's/^/    /'
fi

# اگر auth/bind شکست خورد، صریح بگو
if grep -qiE "Failed to (bind|auth)|authentication|no auth|permission|whitelist" "$LOG"; then
  echo ""
  echo "  ⚠ نشانهٔ مشکلِ auth/whitelist یا bind در لاگ دیده شد:"
  grep -iE "Failed|auth|whitelist|bind|permission" "$LOG" | tail -n 5 | sed 's/^/    /'
fi

# ── سنجهٔ (B): از شمارندهٔ UDP ────────────────────────────────────────────
echo ""
echo "── (B) پکت‌های خامِ UDP (شردِ بازسازی‌شده روی $DEST) ─────────"
if [[ -s "$UDP_OUT" ]]; then
  line="$(cat "$UDP_OUT")"
  if [[ "$line" == BIND_FAIL* ]]; then
    echo "  ⚠ نتوانست پورت $DEST را bind کند: ${line#BIND_FAIL }"
    echo "    (احتمالاً چیزِ دیگری آن پورت را گرفته — dest را عوض کن)"
  else
    echo "  $line"
    R="$(echo "$line" | grep -oE 'RATE [0-9.]+' | awk '{print $2}')"
    [[ -n "${R:-}" ]] && echo "  → ≈ ${R} pkt/s"
  fi
else
  echo "  (دادهٔ UDP ثبت نشد)"
fi

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  تفسیر"
echo "════════════════════════════════════════════════════════════════"
cat <<'TXT'
  • اگر (A) received/s چند هزار در ثانیه و پایدار بود و (B) pkt/s هم بالا
    و یکنواخت بود  →  سرورِ تو همهٔ شردها را می‌گیرد؛ محدودیت از جیتو/شبکه
    نیست و مشکل پایین‌دست در کدِ ربات است.
  • اگر این نرخ‌ها پایین، بریده‌بریده یا نزدیکِ صفر بودند  →  محدودیت در
    مسیرِ جیتو↔سرورِ توست (auth/whitelist، ریجن، فایروال/MTU، پهنای‌باند).
  • اگر خطِ auth/bind/whitelist دیدی  →  کیفِ وایت‌لیست یا اتصال مشکل دارد.

  مرجعِ تقریبی: mainnet در ساعتِ فعال حدود ۲.۵ اسلات در ثانیه دارد و هر
  اسلات هزاران شرد؛ پس received/s سالم معمولاً چند هزار در ثانیه است.
TXT
echo ""
echo "  لاگِ کاملِ پروکسی برای بررسیِ دقیق‌تر: $LOG"
echo "════════════════════════════════════════════════════════════════"
