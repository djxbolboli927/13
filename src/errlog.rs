//! Errors-only file logger.
//!
//! The operator asked for a single file (default under `/root/g/`) that records
//! ONLY the things they can't afford to miss when they aren't watching the
//! terminal:
//!   * important errors,
//!   * why a profitable opportunity was NOT sent,
//!   * why a sent transaction was lost (reverted / dropped).
//!
//! Nothing else goes here — this is deliberately NOT a mirror of the terminal.
//! Lines are appended with a UTC timestamp and flushed immediately so a crash
//! never loses the last few entries.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::{Mutex, OnceLock};

static SINK: OnceLock<Option<Mutex<File>>> = OnceLock::new();

/// Initialise the error log at `<dir>/shred-arb-errors.log`. Safe to call once
/// at startup; if the directory can't be opened the logger degrades to a no-op
/// (the bot keeps running — the file is a convenience, not a dependency).
pub fn init(dir: &str) {
    let _ = SINK.set(open(dir));
    if let Some(Some(_)) = SINK.get() {
        log("errlog", "── error log started ──");
    }
}

fn open(dir: &str) -> Option<Mutex<File>> {
    let p = Path::new(dir);
    if let Err(e) = std::fs::create_dir_all(p) {
        eprintln!("[errlog] cannot create {dir}: {e} — error file disabled");
        return None;
    }
    let path = p.join("shred-arb-errors.log");
    match OpenOptions::new().create(true).append(true).open(&path) {
        Ok(f) => {
            eprintln!("[errlog] writing errors to {}", path.display());
            Some(Mutex::new(f))
        }
        Err(e) => {
            eprintln!("[errlog] cannot open {}: {e} — error file disabled", path.display());
            None
        }
    }
}

/// Append one line: `<ts> [<kind>] <msg>`. `kind` is a short tag such as
/// `not-sent`, `lost`, or `error` so the file greps cleanly.
pub fn log(kind: &str, msg: &str) {
    let Some(Some(m)) = SINK.get() else {
        return;
    };
    let ts = now_iso();
    if let Ok(mut f) = m.lock() {
        let _ = writeln!(f, "{ts} [{kind}] {msg}");
        let _ = f.flush();
    }
}

/// Minimal ISO-8601 UTC timestamp without pulling in chrono.
fn now_iso() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = d.as_secs();
    let ms = d.subsec_millis();
    // days since epoch → y/m/d (civil calendar, valid for 1970+).
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let (y, mo, day) = civil_from_days(days);
    format!("{y:04}-{mo:02}-{day:02}T{h:02}:{mi:02}:{s:02}.{ms:03}Z")
}

// Howard Hinnant's days→civil date algorithm.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}
