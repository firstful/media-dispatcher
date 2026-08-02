# media-dispatcher

Pipeline service that optimizes the Jellyfin library by dispatching jobs to the
[transcode-worker](http://192.168.1.2:3000/reeds/transcode-worker) DaemonSet.

## What it does

Runs `media-optimize.py` (plan / feed / reconcile) against the media library:

- **plan** — scans the library (probe-cache, size/mtime-keyed → seconds), ranks
  re-encode candidates by projected savings (non-HEVC/AV1, source bitrate > tier
  target, >= 300MB), then missing-subtitle files; caps at `--hours` of runtime.
- **feed** — copies selected files into the transcode inbox (`.staging` + atomic
  rename so the 30s-polling worker never claims a half-written file). The worker
  DaemonSet (2 pods on GPU-labeled nodes) encodes to HEVC at resolution-aware
  bitrates, pass-throughs already-compressed files, and best-effort downloads +
  syncs subtitles.
- **reconcile** — moves worker outputs from `done/` back into the library
  (ffprobe-verified: size-stability + stderr + mid-encode `.processing` DEFER),
  replaces originals, moves subtitle sidecars, purges `done/originals/` older
  than 14 days, cleans empty dirs.

## Why a service

The dispatcher runs **in the cluster** with NFS mounts to both the library
(`/mnt/raid_store/jellyfin`) and the transcode inbox/done
(`/mnt/raid_store/transcode`) — no host-side pipeline. It is deployed by Flux
from `reeds/sutliff-cluster` (`apps/media-dispatcher/`) and stays idle; the
Hermes nightly cron invokes it on demand:

```bash
kubectl -n transcode exec deploy/media-dispatcher -- python3 /app/media-optimize.py reconcile
kubectl -n transcode exec deploy/media-dispatcher -- python3 /app/media-optimize.py plan --hours 3
kubectl -n transcode exec deploy/media-dispatcher -- python3 /app/media-optimize.py feed --hours 3 --apply
```

State (probe cache, plan cache, log) persists at `/media/state` (inside the
transcode-work NFS share) so restarts don't lose the cache.

## Layout

- `media-optimize.py` — the pipeline (env-driven paths; host defaults for local use)
- `Dockerfile` — python:3.11-slim + ffmpeg (ffprobe for verification)
- `k8s/deployment.yaml` — Deployment in ns `transcode`, 1 replica, NFS volumes
- `k8s/pv-pvc.yaml` — `jellyfin-media` NFS PV/PVC (requires the
  `/mnt/raid_store/jellyfin` export on the NFS server)

## Deploy

Flux owns this: update `apps/media-dispatcher/` in `reeds/sutliff-cluster`,
commit + push. Image tag bumps require `docker build` + `docker push
192.168.1.2:3000/reeds/media-dispatcher:<tag>` first.
