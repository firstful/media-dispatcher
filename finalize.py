#!/usr/bin/env python3
"""finalize: library finalization crawl (cheapest checks first, recency-biased).

Every media item under the Jellyfin library gets a sidecar
`<video>.finalized.json` holding ALL checks. An item is FINALIZED only when
every check PASSes. The nightly crawl:

  1. scans all video files
  2. ranks by recency (files touched in the last FINALIZE_RECENT_DAYS first,
     then by age ascending — old files drain gradually)
  3. for each file, runs only the checks that aren't pass yet, cheapest first
  4. writes/refreshes the sidecar; queues actions for failures that have one
  5. stops after the nightly budget (FINALIZE_HOURS)

Actions (whisper transcribe, ffsubsync re-sync, transcode, rename) are queued
to the sidecar `actions` list and executed by the overnight workers — never
inline in the crawl.

Design: homelab-docs `ops/media-finalization.md`.
"""

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

# Same env-driven paths as media-optimize.py
JELLYFIN = Path(os.environ.get("JELLYFIN_ROOT", "/mnt/raid_store/jellyfin/Media"))
MEDIA_LIBS = [s for s in os.environ.get(
    "MEDIA_LIBS", "Adult_Movies,Adult_TV,Kids_Movies,Kids_TV").split(",") if s]
STATE = Path(os.environ.get("MEDIA_STATE_DIR", "/root/media-optimize"))
LOG = Path(os.environ.get("MEDIA_LOG", "/var/log/media-optimize.log"))

VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".ts", ".m2ts", ".mov"}
SUB_EXTS = {".srt", ".vtt", ".ass", ".sub"}
MIN_FILE_BYTES = 50 * 1024 * 1024
PREFERRED_CODECS = {"hevc", "av1"}
RECENT_DAYS = float(os.environ.get("FINALIZE_RECENT_DAYS", "7"))
BUDGET_HOURS = float(os.environ.get("FINALIZE_HOURS", "4"))
SCHEMA_VERSION = 1

# ffsubsync agreement threshold: score is an absolute magnitude logged by the
# CLI (~89k for a 90-min movie with full dialog). Below this, treat as
# questionable → re-sync/transcribe.
FSUBSYNC_MIN_SCORE = float(os.environ.get("FSUBSYNC_MIN_SCORE", "20000"))

# Source-gated checks (Reed 2026-08-06). The sidecar carries TWO flags the
# downloader sets at publish:
#   trusted_source: bool  — downloader vouches for the item's identity
#   tvdb_id: int|None     — TVDB id when known/searched
# Semantics:
#   trusted_source=true + tvdb_id set   (PBS)  → cleanup + SRTs, identity known
#   trusted_source=true + tvdb_id null  (YT)   → cleanup + SRTs, no TVDB identity
#   trusted_source falsy/absent                → FULL verification (runtime
#                                                agreement via TVDB, transcode…)
# Library root is ONLY a fallback for pre-flag files (no provenance yet).
SOURCE_BY_LIB = {
    "Kids_Youtube": "yt",
    "Kids_TV": "pbs",
    "Kids_Movies": "movie",
    "Adult_TV": "tv",
    "Adult_Movies": "movie",
}
# Sources that only need cleanup + SRT (skip runtime/transcode verification)
LITE_SOURCES = {"yt", "pbs"}

SIDECAR_SUFFIX = ".finalized.json"


def log(msg):
    line = time.strftime("%Y-%m-%d %H:%M:%S") + " " + msg
    print(line)
    try:
        with LOG.open("a") as f:
            f.write(line + "\n")
    except OSError:
        pass


def sidecar_path(video: Path) -> Path:
    return video.with_suffix(video.suffix + SIDECAR_SUFFIX)


def detect_source(video: Path, m: dict) -> tuple[str, bool, int | None]:
    """Return (source, trusted, tvdb_id) for this item.

    Primary signal = the sidecar flags set by the downloader at publish:
      trusted_source (bool) + tvdb_id (int|None).
    Fallback = library root (SOURCE_BY_LIB) for pre-flag files.
    """
    prov = m.get("provenance") or {}
    trusted = bool(prov.get("trusted_source"))
    tvdb_id = prov.get("tvdb_id")
    if tvdb_id is not None:
        try:
            tvdb_id = int(tvdb_id)
        except (TypeError, ValueError):
            tvdb_id = None
    dl = str(prov.get("downloader") or "").lower()
    lib = rel_lib(video)
    # Flags win: a trusted item is yt/pbs-like regardless of library.
    if trusted:
        # PBS searches tvdb_id; YT sets none. Both are lite (cleanup+SRT).
        if tvdb_id is not None:
            return "pbs", True, tvdb_id
        return "yt", True, None
    # Untrusted / no flags: fall back to library root.
    # cs-downloader = untrusted (aggregates third-party streams): full
    # verification stays on. Kids_TV keeps the "pbs" label — that label only
    # drives the duration clip threshold (>=60s) vs the "tv" episode floor
    # (>=10min), and Peppa/Bluey eps are 5-7 min (2026-08-16 regression: a
    # 5-min ep flagged "tv" fails duration, so provenance on Kids_TV files
    # must not relabel them).
    if dl in ("cs-downloader", "cs_downloader", "csdownloader"):
        return ("pbs" if lib == "Kids_TV" else SOURCE_BY_LIB.get(lib, "movie")), False, tvdb_id
    return SOURCE_BY_LIB.get(lib, "tv"), False, tvdb_id


def rel_lib(video: Path) -> str:
    try:
        parts = video.relative_to(JELLYFIN).parts
    except ValueError:
        return ""
    return parts[0] if parts else ""


def read_sidecar(video: Path) -> dict:
    p = sidecar_path(video)
    if not p.exists():
        return {"schema_version": SCHEMA_VERSION,
                "file": str(video.relative_to(JELLYFIN)),
                "finalized": False, "finalized_at": None,
                "checks": {}, "actions": [], "history": []}
    try:
        m = json.loads(p.read_text())
    except Exception:
        m = {}
    m.setdefault("schema_version", SCHEMA_VERSION)
    m.setdefault("file", str(video.relative_to(JELLYFIN)))
    m.setdefault("finalized", False)
    m.setdefault("finalized_at", None)
    m.setdefault("checks", {})
    m.setdefault("actions", [])
    m.setdefault("history", [])
    return m


def write_sidecar(video: Path, m: dict):
    m["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    all_pass = (m["checks"] and
                all(c.get("status") == "pass" for c in m["checks"].values()))
    if all_pass and not m["finalized"]:
        m["finalized"] = True
        m["finalized_at"] = m["updated_at"]
        log("FINALIZED %s" % m["file"])
    elif not all_pass and m["finalized"]:
        m["finalized"] = False
        m["finalized_at"] = None
    sidecar_path(video).write_text(json.dumps(m, indent=2))


def set_check(m: dict, name: str, status: str, **kw):
    """Record one check result; append to history when status changes."""
    prev = m["checks"].get(name)
    entry = {"status": status,
             "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
             **kw}
    m["checks"][name] = entry
    if prev and prev.get("status") != status:
        m["history"].append({"check": name, "status": status,
                             "at": entry["checked_at"],
                             "detail": kw.get("detail", "")})


def add_action(m: dict, action: str, detail: str = ""):
    if action not in [a.get("action") for a in m["actions"]]:
        m["actions"].append({"action": action, "detail": detail,
                             "queued_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})


def ffprobe(path: Path):
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error",
             "-show_entries", "stream=codec_type,codec_name,width,height,display_aspect_ratio:format=duration,size",
             "-of", "json", str(path)],
            capture_output=True, text=True, timeout=90)
        return json.loads(r.stdout or "{}")
    except Exception:
        return {}


def expected_aspect(rel: str) -> str | None:
    """Guess expected AR from the filename/library path. Heuristic:
    '16x9'/'1080p' styles → 16:9; '4x3' → 4:3; otherwise None (unknown)."""
    low = rel.lower()
    if "4x3" in low or "4:3" in low:
        return "4:3"
    if "16x9" in low or "16:9" in low or "1080p" in low or "720p" in low:
        return "16:9"
    return None


def run_checks(video: Path, rel: str, m: dict):
    """Cheapest checks first. Each sets pass/fail in the sidecar."""
    checks = m["checks"]
    st = video.stat() if video.exists() else None

    source, trusted, tvdb_id = detect_source(video, m)
    m["source"] = source          # record for provenance/debugging
    m["trusted_source"] = trusted  # the flag the downloader set
    m["tvdb_id"] = tvdb_id

    # 1. exists
    if st is None or st.st_size < MIN_FILE_BYTES:
        set_check(m, "exists", "fail", detail="missing or < 50MB")
        return
    set_check(m, "exists", "pass", detail="%d bytes" % st.st_size)

    # 2-5. container / resolution / AR / duration — one ffprobe call
    data = ffprobe(video)
    v = next((s for s in data.get("streams", []) if s.get("codec_type") == "video"), None)
    if not v:
        set_check(m, "container", "fail", detail="no video stream")
        return
    codec = v.get("codec_name") or "?"
    height = int(v.get("height") or 0)
    dur = float((data.get("format") or {}).get("duration") or 0)
    set_check(m, "container", "pass", codec=codec, detail="video stream readable")
    set_check(m, "codec", "pass" if codec in PREFERRED_CODECS else "warn",
              codec=codec, detail="preferred hevc/av1" if codec in PREFERRED_CODECS else "transcode candidate")
    set_check(m, "resolution", "pass" if height > 0 else "fail", height=height)
    # AR: compare detected to expected (only when expected is known)
    exp_ar = expected_aspect(rel)
    det_ar = v.get("display_aspect_ratio") or ""
    if exp_ar and det_ar:
        ok = (det_ar.replace(":", "x") == exp_ar.replace(":", "x"))
        set_check(m, "aspect_ratio", "pass" if ok else "fail",
                  detected=det_ar, expected=exp_ar)
    elif exp_ar:
        set_check(m, "aspect_ratio", "warn", detected=det_ar or "?", expected=exp_ar)
    # duration sanity — source-aware: YT videos are legitimately 3-20 min;
    # movies ≥ 60min; TV episodes ≥ 10min.
    dur_ok = dur > 0
    if source in LITE_SOURCES:
        dur_ok = dur >= 60  # any real clip
    elif "/Movie" in rel or "Movies" in rel or source == "movie":
        dur_ok = dur >= 60 * 60
    elif dur_ok:
        dur_ok = dur >= 10 * 60
    set_check(m, "duration", "pass" if dur_ok else "fail",
              duration_s=round(dur, 1), min_expect=(
                  "clip" if source in LITE_SOURCES
                  else ("movie>=60m" if source == "movie" else "episode>=10m")))

    # 6-7. subtitles: presence + ffsubsync agreement
    subs = list(video.parent.glob(video.stem + "*.srt")) + \
           list(video.parent.glob(video.stem + ".*.srt"))
    subs += list(video.parent.glob(video.stem + "*.vtt")) + \
            list(video.parent.glob(video.stem + ".*.vtt"))
    if subs:
        srt = subs[0]
        set_check(m, "srt_present", "pass", srt=srt.name)
        # ffsubsync score (already-run markers only — crawl does NOT run
        # ffsubsync inline; that's an overnight action. If a score exists from
        # the transcode marker, use it; else 'unknown' + queue re-sync check.
        score = None
        marker = video.with_suffix(video.suffix + ".json")
        if marker.exists():
            try:
                score = json.loads(marker.read_text()).get("subs_sync_score")
            except Exception:
                score = None
        if score is None:
            set_check(m, "srt_synced", "unknown",
                      detail="no ffsubsync score yet — queued")
            add_action(m, "ffsubsync", "score %s" % srt.name)
        elif score >= FSUBSYNC_MIN_SCORE:
            set_check(m, "srt_synced", "pass", score=score, method="ffsubsync")
        else:
            set_check(m, "srt_synced", "fail", score=score,
                      detail="below threshold — whisper re-transcribe")
            add_action(m, "whisper", "score %s < %s" % (score, FSUBSYNC_MIN_SCORE))
    else:
        set_check(m, "srt_present", "fail", detail="no sidecar subtitle")
        set_check(m, "srt_synced", "fail", detail="no subtitle to sync")
        add_action(m, "whisper", "missing srt")

    # 8. runtime agreement (TVDB) — SKIPPED for trusted sources (yt/pbs:
    # Reed 2026-08-06, they need cleanup + SRTs only, no runtime verification).
    # Untrusted items get the full cross-check.
    marker = video.with_suffix(video.suffix + ".json")
    if trusted:
        set_check(m, "runtime_agreement", "n/a",
                  detail="trusted_source=true — no runtime verification")
    else:
        marker = video.with_suffix(video.suffix + ".json")
        tvdb = None
        if marker.exists():
            try:
                tvdb = json.loads(marker.read_text()).get("tvdb_verdict")
            except Exception:
                tvdb = None
        if tvdb == "mismatch":
            set_check(m, "runtime_agreement", "fail", detail="TVDB mismatch")
        elif tvdb == "ok":
            set_check(m, "runtime_agreement", "pass", detail="TVDB ok")
        else:
            set_check(m, "runtime_agreement", "unknown",
                      detail="no TVDB cross-check yet")
            if "/TV/" in rel or "TV" in rel or source == "tv":
                add_action(m, "tvdb_check")

    # 9. style agreement — filename matches renamer conventions
    # (Title (Year)/SxxExx or Title (Year)/Title (Year).ext)
    style_ok = bool(re_search_style(rel))
    set_check(m, "style_agreement", "pass" if style_ok else "warn",
              detail="renamer-conformant" if style_ok else "non-standard name")

    # 10. provenance — downloader records source URLs/hash + the two flags
    # (trusted_source, tvdb_id) at publish. trusted_source itself satisfies
    # provenance; an untrusted item without URLs/hash is flagged.
    prov = m.get("provenance") or {}
    prov_ok = bool(prov.get("trusted_source")) or bool(prov.get("source_urls")) or bool(prov.get("content_hash"))
    if prov_ok:
        set_check(m, "provenance", "pass",
                  trusted_source=prov.get("trusted_source") or False,
                  tvdb_id=prov.get("tvdb_id"),
                  **{k: prov[k] for k in ("source_urls", "content_hash", "downloader") if k in prov})
    else:
        set_check(m, "provenance", "fail", detail="no downloader provenance/flags")
        # NOTE: provenance comes from the downloader at publish; the crawl only
        # flags it — a downloader-side write is the fix, not an overnight action.

    # 11. transcode_ok — from the worker marker if present
    if marker.exists():
        try:
            mstatus = json.loads(marker.read_text()).get("status")
            set_check(m, "transcode_ok", "pass" if mstatus in ("ok", "skipped") else "warn",
                      detail="worker marker %s" % mstatus)
        except Exception:
            pass
    else:
        set_check(m, "transcode_ok", "n/a",
                  detail="no worker marker (not in transcode pipeline)")


def re_search_style(rel: str) -> bool:
    """Cheap filename-convention check. True for 'Title (Year)/SxxExx.ext' or
    'Title (Year)/Title (Year).ext' style paths."""
    low = rel.lower()
    if low.endswith(".sample.mkv") or "/sample" in low:
        return True  # samples ignored elsewhere; don't fail them here
    if "(" in low and ")" in low:
        # has a (Year) or (Quality) token — acceptable
        return True
    if re_match_episode(low):
        return True
    return False


def re_match_episode(low: str) -> bool:
    import re
    return bool(re.search(r"s\d{1,2}e\d{1,2}", low))


def cmd_finalize(budget_hours: float, recent_days: float):
    log("finalize: scan %s (budget %.1fh, recent %.0fd)" %
        (JELLYFIN, budget_hours, recent_days))
    t0 = time.time()
    deadline = t0 + budget_hours * 3600

    items = []
    for root_dir in MEDIA_LIBS:
        base = JELLYFIN.joinpath(root_dir)
        if not base.exists():
            continue
        for p in sorted(base.rglob("*")):
            if not p.is_file() or p.suffix.lower() not in VIDEO_EXTS:
                continue
            if "sample" in p.name.lower():
                continue
            try:
                st = p.stat()
            except OSError:
                continue
            items.append((p, st.st_mtime))

    # recency bias: recent-days files first, then by age ascending
    recent_cutoff = time.time() - recent_days * 86400
    items.sort(key=lambda x: (0 if x[1] >= recent_cutoff else 1, x[1]))

    n_done = 0
    n_finalized = 0
    for video, _mtime in items:
        if time.time() > deadline:
            log("finalize: budget reached — %d/%d scanned" % (n_done, len(items)))
            break
        rel = str(video.relative_to(JELLYFIN))
        m = read_sidecar(video)
        maybe_backfill_tvdb(video, m)  # PBS: search + set tvdb_id (dispatcher has the key)
        run_checks(video, rel, m)
        write_sidecar(video, m)
        n_done += 1
        if m["finalized"]:
            n_finalized += 1

    log("finalize: %d scanned, %d finalized (%.0f%% of scanned)" %
        (n_done, n_finalized, (100.0 * n_finalized / n_done) if n_done else 0))


def maybe_backfill_tvdb(video: Path, m: dict) -> None:
    """PBS items: search TVDB for the show id and stash it in provenance.

    PBS sets trusted_source=true but tvdb_id=null at publish (the pbskids-sync
    pod has no TVDB key). The dispatcher pod does — so the crawl fills it in.
    Uses the pbs_show + pbs_season fields from provenance. One search per show
    is cached in state, and only runs for items still missing the id."""
    prov = m.get("provenance") or {}
    if not prov.get("trusted_source") or prov.get("tvdb_id") is not None:
        return  # not PBS, or already has the id
    show = str(prov.get("pbs_show") or "").strip()
    if not show:
        return
    cache_path = STATE / "tvdb-id-search.json"
    cache = {}
    try:
        cache = json.loads(cache_path.read_text())
    except Exception:
        pass
    key = show.lower()
    if key in cache:
        tvdb_id = cache[key]
        if tvdb_id:
            prov["tvdb_id"] = tvdb_id
        return
    # lazy-import media-optimize's tvdb helpers (same pod, has the API key)
    try:
        import media_optimize as mo
    except Exception:
        return
    token = mo.tvdb_login()
    if not token:
        return
    try:
        import urllib.parse as _up
        data = mo.tvdb_get(token, "search?query=" + _up.quote(show) + "&type=series")
        hits = data.get("data") or []
        # prefer exact title match, else first hit
        tvdb_id = None
        for h in hits:
            if (h.get("name") or "").lower() == show.lower():
                tvdb_id = h.get("id"); break
        if tvdb_id is None and hits:
            tvdb_id = hits[0].get("id")
        cache[key] = tvdb_id
        try:
            cache_path.write_text(json.dumps(cache))
        except Exception:
            pass
        if tvdb_id:
            prov["tvdb_id"] = tvdb_id
            log("finalize: PBS tvdb_id %s -> %s" % (show, tvdb_id))
    except Exception as e:
        log("finalize: tvdb search failed for %s: %s" % (show, e))


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--budget-hours", type=float, default=BUDGET_HOURS)
    ap.add_argument("--recent-days", type=float, default=RECENT_DAYS)
    args = ap.parse_args()
    STATE.mkdir(parents=True, exist_ok=True)
    cmd_finalize(args.budget_hours, args.recent_days)


if __name__ == "__main__":
    main()
