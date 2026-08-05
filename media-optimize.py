#!/usr/bin/env python3
"""media-optimize: plan/feed/reconcile pipeline for the k3s transcode worker.

Optimizes the Jellyfin library on the RAID store:
  plan      - scan library, rank candidates (re-encode savings, missing subs)
  feed      - copy top candidates (<= --hours runtime budget) into the transcode
              inbox; the always-on worker DaemonSet processes them to HEVC
              (res-aware bitrate) or passes them through, and best-effort
              downloads+syncs subtitles.
  reconcile - move worker outputs from done/ back into the library, replacing
              the originals, then purge originals older than RETENTION_DAYS.

Nightly cycle (cron): reconcile (yesterday's outputs) -> feed (next batch).
The worker runs continuously, so the inbox drains between runs.

Safety:
- Library files are COPIED to the inbox; originals stay until reconcile verifies
  the new output (ffprobe) and moves it into place, then the original is removed.
- Replaced originals are kept in done/originals/ for RETENTION_DAYS as rollback.
- Re-encode only non-HEVC/AV1 sources with >= MIN_SAVINGS_RATIO projected
  savings ratio AND a source bitrate above the tier target (below-tier sources
  are already compressed — the worker passes them through untouched).
"""
import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

# This host's resolver returns NAT64 AAAA records (64:ff9b::…) first and has no
# IPv6 egress — socket/urllib connect() hangs on them. Force IPv4 resolution
# (same as curl -4). Must run before any socket use.
import socket as _socket
_orig_getaddrinfo = _socket.getaddrinfo
def _getaddrinfo_ipv4(*args, **kwargs):
    return [r for r in _orig_getaddrinfo(*args, **kwargs) if r[0] == _socket.AF_INET]
_socket.getaddrinfo = _getaddrinfo_ipv4

# All paths env-driven so the same script runs on the host (defaults) and inside
# the k3s dispatcher pod (NFS mounts at /media/...).
JELLYFIN = Path(os.environ.get("JELLYFIN_ROOT", "/mnt/raid_store/jellyfin"))
INBOX = Path(os.environ.get("TRANSCODE_INBOX", "/mnt/raid_store/transcode/inbox"))
DONE = Path(os.environ.get("TRANSCODE_DONE", "/mnt/raid_store/transcode/done"))
LOG = Path(os.environ.get("MEDIA_LOG", "/var/log/media-optimize.log"))
STATE = Path(os.environ.get("MEDIA_STATE_DIR", "/root/media-optimize"))
PLAN_CACHE = STATE.joinpath("plan.json")
PROBE_CACHE = STATE.joinpath("probe-cache.json")

VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".ts", ".m2ts", ".mov"}
SUB_EXTS = {".srt", ".vtt", ".ass", ".sub"}
MARKER_EXTS = {".json"}  # worker-written <video>.json transcode markers
MIN_FILE_BYTES = 50 * 1024 * 1024      # smaller = sample/trailer/short
# Re-encode only when the projected savings are a MEANINGFUL FRACTION of the
# source (compressibility ratio), not a hard byte count. A 200MB episode at
# 40% savings is worth the encode; a 7GB file at 5% is not. Combined with
# src_br > target (below-tier sources are already compressed and pass through
# untouched — see SKIP_IF_COMPRESSED in worker.sh), this keeps the queue to
# files that actually shrink.
MIN_SAVINGS_RATIO = float(os.environ.get("MIN_SAVINGS_RATIO", "0.15"))
# Recency bias for ranking: score = savings_ratio * (1 + recency), where
# recency = RECENCY_BOOST / (1 + age_days / RECENCY_HALF_LIFE_DAYS).
# Fresh files get a score nudge (1.0 boost at age 0, decaying to ~0 as the
# file ages) so newly-downloaded content is processed before ancient rips,
# without letting a 15%-savings fresh file starve a 50%-savings old one.
RECENCY_BOOST = float(os.environ.get("RECENCY_BOOST", "1.0"))
RECENCY_HALF_LIFE_DAYS = float(os.environ.get("RECENCY_HALF_LIFE_DAYS", "30"))
RETENTION_DAYS = 14                    # replaced originals kept this long
ALREADY_OPTIMAL_CODECS = {"hevc", "av1"}
MARKER_VERSION = 1                     # bump to force re-evaluation of marked files
TVDB_CACHE = STATE / "tvdb-cache.json"  # title|year|season|ep -> runtime_min


def tvdb_login():
    """Bearer token for api4.thetvdb.com (cached in state). None if no key."""
    key = os.environ.get("TVDB_API_KEY", "").strip()
    if not key:
        return None
    tokf = STATE / "tvdb-token.json"
    try:
        t = json.loads(tokf.read_text())
        if t.get("expires", 0) > time.time() + 3600:
            return t["token"]
    except Exception:
        pass
    try:
        req = urllib.request.Request(
            "https://api4.thetvdb.com/v4/login",
            data=json.dumps({"apikey": key}).encode(),
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as r:
            token = json.loads(r.read())["data"]["token"]
        tokf.write_text(json.dumps({"token": token,
                                    "expires": time.time() + 6 * 86400}))
        return token
    except Exception as e:
        log("tvdb login failed: %s" % e)
        return None


def tvdb_get(token, path):
    req = urllib.request.Request(
        "https://api4.thetvdb.com/v4/" + path,
        headers={"Authorization": "Bearer " + token})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())


def tvdb_episode_runtime(show, year, season, episode):
    """Expected runtime (minutes) for a TV episode via TVDB v4, cached.
    Returns None when unknown (no key, sparse entry, lookup failure)."""
    key = "%s|%s|%d|%d" % (show.lower(), year or "", season, episode)
    try:
        c = json.loads(TVDB_CACHE.read_text())
    except Exception:
        c = {}
    if key in c:
        return c[key]
    token = tvdb_login()
    val = None
    if token:
        try:
            q = urllib.parse.quote(show)
            search = tvdb_get(token, "search?query=%s&type=series" % q)
            sid = None
            for r in search.get("data", []):
                if year and str(r.get("year")) == str(year):
                    sid = str(r.get("objectID", "")).replace("series-", "")
                    break
            if sid is None and search.get("data"):
                sid = str(search["data"][0]["objectID"]).replace("series-", "")
            if sid:
                ext = tvdb_get(token, "series/%s/extended?meta=episodes" % sid)
                for e in ext.get("data", {}).get("episodes", []):
                    if (e.get("seasonNumber") == season
                            and e.get("number") == episode):
                        val = e.get("runtime")
                        break
        except Exception as ex:
            log("tvdb lookup failed for %s: %s" % (show, ex))
    c[key] = val
    try:
        TVDB_CACHE.write_text(json.dumps(c))
    except Exception:
        pass
    return val


def parse_tv_ref(rel):
    """From a library-relative tv path, return (show, year, season, episode)
    or None. Expects 'tv/Show (Year)/Season NN/Show SxxEyy.ext'."""
    m = re.search(r"S(\d{1,2})E(\d{1,2})", rel, re.IGNORECASE)
    if not m:
        return None
    parts = Path(rel).parts
    if len(parts) < 4 or parts[0] != "tv":
        return None
    show_dir = parts[1]
    ym = re.search(r"^(.*?)\s*\((\d{4})\)$", show_dir)
    show = ym.group(1).strip() if ym else show_dir
    year = ym.group(2) if ym else None
    sm = re.search(r"Season\s+(\d+)", parts[2], re.IGNORECASE)
    season = int(sm.group(1)) if sm else int(m.group(1))
    return show, year, season, int(m.group(2))


def read_marker(p):
    """Return parsed marker dict if valid (version + size matches), else None."""
    marker = p.parent.joinpath(p.stem + ".json")
    try:
        if not marker.exists():
            return None
        m = json.loads(marker.read_text())
        if m.get("marker_version") != MARKER_VERSION:
            return None
        if m.get("status") not in ("transcoded", "pass_through"):
            return None
        if m.get("output_size_bytes") != p.stat().st_size:
            return None  # stale — file changed since it was processed
        return m
    except Exception:
        return None


def log(msg):
    line = "[%s] %s" % (time.strftime("%Y-%m-%dT%H:%M:%S%z"), msg)
    print(line, flush=True)
    try:
        with open(LOG, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass


def tier_target_bps(height):
    """Must match worker.sh BITRATE_* tiers."""
    if height <= 480:
        return 1_500_000
    if height <= 720:
        return 4_000_000
    if height <= 1080:
        return 6_000_000
    return 8_000_000


def probe(path):
    """Return dict(codec,height,bit_rate,duration,size,warnings) or None."""
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries",
             "stream=codec_type,codec_name,width,height,bit_rate:format=duration,size",
             "-of", "json", str(path)],
            capture_output=True, text=True, timeout=60)
        data = json.loads(r.stdout or "{}")
    except Exception:
        return None
    v = next((s for s in data.get("streams", []) if s.get("codec_type") == "video"), None)
    if not v:
        return None
    fmt = data.get("format", {})
    dur = float(fmt.get("duration") or 0)
    size = int(fmt.get("size") or os.path.getsize(path))
    br = v.get("bit_rate")
    if not br and dur > 0:
        br = int(size * 8 / dur)
    return {"codec": v.get("codec_name"), "height": v.get("height"),
            "bit_rate": int(br) if br else None, "duration": dur, "size": size,
            "warnings": (r.stderr or "").strip()}


def probe_cached(path, cache):
    """probe() with a size/mtime-keyed cache so nightly re-scans are fast."""
    try:
        st = path.stat()
        key = "%s|%d|%d" % (path, st.st_size, st.st_mtime)
    except OSError:
        return None
    if key in cache:
        return cache[key]
    info = probe(path)
    cache[key] = info
    return info


def in_flight(p):
    """True if this library file is currently waiting/processing in the inbox."""
    rel = p.relative_to(JELLYFIN)
    return INBOX.joinpath(rel).exists() or INBOX.joinpath(str(rel) + ".processing").exists()


def scan():
    cache = {}
    try:
        cache = json.loads(PROBE_CACHE.read_text())
    except Exception:
        cache = {}
    items = []
    for root_dir in ("movies", "tv"):
        base = JELLYFIN.joinpath(root_dir)
        if not base.exists():
            continue
        for p in sorted(base.rglob("*")):
            if not p.is_file() or p.suffix.lower() not in VIDEO_EXTS:
                continue
            if "sample" in p.name.lower():
                continue
            try:
                if p.stat().st_size < MIN_FILE_BYTES:
                    continue
            except OSError:
                continue
            # movies only: multi-video dir guard (skip CD1/CD2, extras piles).
            # TV season dirs legitimately hold many episodes — never skip those.
            if root_dir == "movies":
                siblings = [f for f in p.parent.iterdir()
                            if f.is_file() and f.suffix.lower() in VIDEO_EXTS]
                if len(siblings) > 1:
                    continue
            if in_flight(p):
                continue
            # Worker-written marker: no probe needed — use its recorded metadata.
            # (Marker gate also applied in evaluate(); this skips the ffprobe.)
            m = read_marker(p)
            if m is not None:
                items.append((p, {
                    "codec": m.get("codec") or "marked",
                    "height": m.get("height") or 0,
                    "bit_rate": 0,
                    "duration": m.get("duration_s") or 0,
                    "size": p.stat().st_size,
                }))
                continue
            info = probe_cached(p, cache)
            if not info or not info["duration"]:
                continue
            items.append((p, info))
    try:
        PROBE_CACHE.write_text(json.dumps(cache))
    except Exception:
        pass
    return items


def evaluate(p, info):
    """Return (needs_reencode, needs_subs, savings_bytes, est_out_bytes,
    savings_ratio)."""
    height = info["height"] or 0
    target = tier_target_bps(height)
    src_br = info["bit_rate"] or 0
    codec = info["codec"] or ""
    size = info["size"] or 0
    est_out = int(target * info["duration"] / 8)
    savings = size - est_out
    savings_ratio = (savings / size) if size > 0 else 0.0
    has_sub = any(p.parent.joinpath(p.stem + e).exists() for e in SUB_EXTS)
    # Marker gate: a worker-written marker is authoritative — the file was
    # already transcoded or judged already-compressed; never re-encode it.
    if read_marker(p) is not None:
        return False, not has_sub, savings, est_out, savings_ratio
    needs_reencode = (src_br > target and codec not in ALREADY_OPTIMAL_CODECS
                      and savings_ratio >= MIN_SAVINGS_RATIO)
    needs_subs = not has_sub
    return needs_reencode, needs_subs, savings, est_out, savings_ratio


def build_plan(hours):
    items = scan()
    ranked = []
    now = time.time()
    for p, info in items:
        nr, ns, savings, est, ratio = evaluate(p, info)
        if not (nr or ns):
            continue
        try:
            mtime = p.stat().st_mtime
        except OSError:
            mtime = 0.0
        age_days = max(0.0, (now - mtime) / 86400)
        # Recency-weighted score: fresh files rank above ancient ones of equal
        # compressibility; a genuinely huge savings ratio still wins.
        recency = RECENCY_BOOST / (1.0 + age_days / RECENCY_HALF_LIFE_DAYS)
        score = ratio * (1.0 + recency) if nr else 0.0
        ranked.append({
            "path": str(p),
            "rel": str(p.relative_to(JELLYFIN)),
            "codec": info["codec"],
            "height": info["height"],
            "dur_min": int(info["duration"] // 60),
            "src_mb": info["size"] // 1048576,
            "est_mb": est // 1048576,
            "savings_mb": savings // 1048576,
            "ratio": ratio,
            "age_days": age_days,
            "score": score,
            "reencode": nr,
            "subs": ns,
        })
    ranked.sort(key=lambda x: (not x["reencode"], -x["score"]))
    budget_s = float(hours) * 3600
    selected, used = [], 0.0
    for it in ranked:
        d = it["dur_min"] * 60
        if used + d > budget_s:
            continue
        selected.append(it)
        used += d
    return selected, ranked, used


def cmd_plan(hours):
    t0 = time.time()
    selected, ranked, used = build_plan(hours)
    n_re = sum(1 for x in ranked if x["reencode"])
    n_sub = sum(1 for x in ranked if not x["reencode"])
    save_mb = sum(x["savings_mb"] for x in ranked if x["reencode"])
    log("plan: %d candidates (%d re-encode est. save %dMB, %d subs-only); "
        "selected %d (%.0fmin runtime, budget %.1fh) [scan %.0fs]"
        % (len(ranked), n_re, save_mb, n_sub, len(selected), used / 60, hours, time.time() - t0))
    for it in selected:
        log("  %s %s  codec=%s %sp %dmin src=%dMB est=%dMB save=%dMB (%.0f%%, %dd)"
            % ("REENC" if it["reencode"] else "SUBS ",
               it["rel"], it["codec"], it["height"], it["dur_min"],
               it["src_mb"], it["est_mb"], it["savings_mb"],
               it["ratio"] * 100, it["age_days"]))
    try:
        PLAN_CACHE.write_text(json.dumps(selected))
    except Exception:
        pass
    return selected


def cmd_feed(hours, apply_):
    # reuse a fresh plan cache if available (plan+feed in one cron run)
    sel = None
    try:
        if PLAN_CACHE.exists() and time.time() - PLAN_CACHE.stat().st_mtime < 900:
            sel = json.loads(PLAN_CACHE.read_text())
    except Exception:
        sel = None
    if sel is None:
        sel = cmd_plan(hours)
    if not apply_:
        log("feed dry-run: would copy %d files to inbox" % len(sel))
        return
    fed = 0
    for it in sel:
        src = Path(it["path"])
        if not src.exists():
            continue
        dst = INBOX.joinpath(it["rel"])
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.exists() or INBOX.joinpath(str(it["rel"]) + ".processing").exists():
            continue  # already queued
        # Stage under a non-video name, then atomically rename into place —
        # the worker polls every 30s and would otherwise claim a half-written
        # file mid-copy (observed: copystat FileNotFoundError + FAILED encodes).
        staging = dst.parent.joinpath(dst.name + ".staging")
        shutil.copy2(src, staging)
        os.replace(staging, dst)
        fed += 1
        log("  FEED %s" % it["rel"])
    log("feed: %d files copied to inbox" % fed)


def cmd_reconcile(purge=True):
    moved = 0
    for out in sorted(DONE.rglob("*")):
        if not out.is_file():
            continue
        rel = out.relative_to(DONE)
        parts = rel.parts
        # Never touch staging/backup dirs: top-level originals/ and subs/, plus
        # per-file subtitle staging (subtitles.py downloads to <dir>/subs/ first,
        # then moves the synced .srt to the sidecar next to the video).
        if parts[0] in ("originals", "subs") or "subs" in parts:
            continue
        if parts[0] not in ("movies", "tv"):
            log("  skip unrecognized done path: %s" % rel)
            continue
        if (out.suffix.lower() not in VIDEO_EXTS
                and out.suffix.lower() not in SUB_EXTS
                and out.suffix.lower() not in MARKER_EXTS):
            continue
        # DEFER: if the matching inbox file is still .processing, the worker is
        # mid-encode and this done/ output is a partial write — never move it.
        proc = INBOX.joinpath(str(rel) + ".processing")
        if not proc.exists() and out.suffix.lower() not in VIDEO_EXTS:
            # sidecar (.srt/.json): the worker still owns the job while the
            # VIDEO's .processing exists — defer so subtitles.py doesn't later
            # recreate a subs-only marker that clobbers the full one.
            for vext in VIDEO_EXTS:
                vrel = str(rel)[:-len(out.suffix)] + vext + ".processing"
                if INBOX.joinpath(vrel).exists():
                    proc = INBOX.joinpath(vrel)
                    break
        if proc.exists():
            log("  DEFER %s (still encoding)" % rel)
            continue
        target = JELLYFIN.joinpath(*parts)
        if out.suffix.lower() in VIDEO_EXTS:
            # Reject anything still being written (size changed in last 2s) or
            # with ffprobe errors — never place a partial/truncated file in the
            # library (observed: an interrupted feed left a truncated mp4 whose
            # header still reported full duration).
            s1 = out.stat().st_size
            time.sleep(2)
            s2 = out.stat().st_size
            if s1 != s2:
                log("  DEFER %s (size changing %d -> %d)" % (rel, s1, s2))
                continue
            info = probe(out)
            if not info or info.get("warnings"):
                log("  REJECT %s: ffprobe %s, leaving in done/"
                    % (rel, "failed" if not info else "warnings"))
                continue
        target.parent.mkdir(parents=True, exist_ok=True)
        if out.suffix.lower() in VIDEO_EXTS:
            orig = None
            for f in target.parent.iterdir():
                if (f.is_file() and f.suffix.lower() in VIDEO_EXTS
                        and f.stem == out.stem and f.name != out.name):
                    orig = f
                    break
            old_size = (orig.stat().st_size if orig
                        else (target.stat().st_size if target.exists() else 0))
            replaced = orig is not None or target.exists()
            shutil.move(str(out), str(target))
            if orig:
                orig.unlink()
            moved += 1
            new_size = target.stat().st_size
            if replaced:
                log("  REPLACED %s (%dMB -> %dMB, saved %dMB)"
                    % (rel, old_size // 1048576, new_size // 1048576,
                       max(0, old_size - new_size) // 1048576))
            else:
                log("  MOVED %s (pass-through)" % rel)
        else:  # sidecar (.srt/.json marker) -> move, overwrite existing
            shutil.move(str(out), str(target))
            log("  SIDE %s" % rel)
    if purge:
        now = time.time()
        purged = 0
        originals = DONE.joinpath("originals")
        if originals.exists():
            for orig in sorted(originals.rglob("*")):
                if orig.is_file():
                    # ctime, NOT mtime: archived copies inherit the source file's
                    # mtime (feed uses copy2), so mtime is 2018 while the file
                    # was archived today. ctime updates on the worker's mv.
                    st = orig.stat()
                    if now - st.st_ctime > RETENTION_DAYS * 86400:
                        orig.unlink()
                        purged += 1
        if purged:
            log("purged %d originals older than %dd" % (purged, RETENTION_DAYS))
    for d in sorted([d for d in DONE.rglob("*") if d.is_dir()],
                    key=lambda x: -len(x.parts)):
        try:
            d.rmdir()
        except OSError:
            pass
    log("reconcile: %d outputs placed in library" % moved)


def cmd_validate():
    """Report media-identity validation from markers: subtitle sync score +
    status, duration consistency (probe vs marker), and TMDB runtime match
    when TMDB_API_KEY is set. Flags files that fail verification."""
    cache = {}
    try:
        cache = json.loads(PROBE_CACHE.read_text())
    except Exception:
        cache = {}
    tmdb_key = os.environ.get("TMDB_API_KEY", "").strip()
    rows, n_marked, n_synced, n_failed = [], 0, 0, 0
    for root_dir in ("movies", "tv"):
        base = JELLYFIN.joinpath(root_dir)
        if not base.exists():
            continue
        for p in sorted(base.rglob("*")):
            if not p.is_file() or p.suffix.lower() not in VIDEO_EXTS:
                continue
            m = read_marker(p)
            if m is None:
                continue
            n_marked += 1
            info = probe_cached(p, cache)
            dur = info["duration"] if info and info.get("duration") else 0
            m_dur = m.get("duration_s") or 0
            dur_ok = (dur > 0 and m_dur > 0
                      and abs(dur - m_dur) / m_dur < 0.05)
            score = m.get("subs_sync_score")
            status = m.get("subs_status", "unknown")
            rel = str(p.relative_to(JELLYFIN))
            # TVDB runtime cross-check (TV episodes only; movie runtimes are
            # null in TVDB v4). Mismatch > 10% (+5 min grace) flags the file.
            tvdb_verdict = None
            tv_ref = parse_tv_ref(rel)
            if tv_ref:
                show, year, season, episode = tv_ref
                rt = tvdb_episode_runtime(show, year, season, episode)
                if rt:
                    tvdb_verdict = ("ok" if abs(dur - rt * 60)
                                    <= max(0.10 * rt * 60, 300) else "mismatch")
            if status == "synced":
                n_synced += 1
                ver = "OK"
            elif status == "sync_failed":
                n_failed += 1
                ver = "FLAG"  # subs did NOT match audio — possible wrong episode
            elif status in ("too_small", "not_found"):
                n_failed += 1
                ver = "UNVERIFIED"  # no usable subs to check
            else:
                ver = "UNVERIFIED"
            if tvdb_verdict == "mismatch":
                ver = "FLAG"  # duration disagrees with TVDB reference
            rows.append({
                "rel": rel, "status": status,
                "score": score, "dur_ok": dur_ok,
                "tvdb": tvdb_verdict, "verdict": ver,
            })
    try:
        PROBE_CACHE.write_text(json.dumps(cache))
    except Exception:
        pass
    log("validate: %d marked files (%d synced subs, %d subs failed); "
        "%d FLAG" % (n_marked, n_synced, n_failed,
                     sum(1 for r in rows if r["verdict"] == "FLAG")))
    for r in sorted(rows, key=lambda x: (x["verdict"] != "FLAG", -((x["score"] or 0)))):
        log("  %s %-70s subs=%-11s score=%s dur_ok=%s tvdb=%s"
            % (r["verdict"], r["rel"], r["status"],
               ("%.3f" % r["score"]) if r["score"] is not None else "n/a",
               r["dur_ok"], r["tvdb"] or "n/a"))
    if os.environ.get("TVDB_API_KEY", "").strip():
        log("validate: TVDB runtime cross-check enabled")
    else:
        log("validate: TVDB_API_KEY not set — runtime cross-check skipped")
    return rows


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p_plan = sub.add_parser("plan", help="list candidates + selected batch")
    p_plan.add_argument("--hours", type=float, default=3.0, help="runtime budget in hours")
    p_feed = sub.add_parser("feed", help="copy selected batch to the inbox")
    p_feed.add_argument("--hours", type=float, default=3.0)
    p_feed.add_argument("--apply", action="store_true", help="actually copy (default dry-run)")
    p_rec = sub.add_parser("reconcile", help="move done/ outputs back into the library")
    p_rec.add_argument("--no-purge", action="store_true", help="keep expired originals")
    sub.add_parser("validate", help="report media-identity validation from markers")
    args = ap.parse_args()
    STATE.mkdir(parents=True, exist_ok=True)
    if args.cmd == "plan":
        cmd_plan(args.hours)
    elif args.cmd == "feed":
        cmd_feed(args.hours, args.apply)
    elif args.cmd == "reconcile":
        cmd_reconcile(purge=not args.no_purge)
    elif args.cmd == "validate":
        cmd_validate()


if __name__ == "__main__":
    main()
