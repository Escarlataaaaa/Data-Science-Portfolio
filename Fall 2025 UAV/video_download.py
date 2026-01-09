import os
import datetime as dt
import time
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pathlib
from pathlib import Path
from zoneinfo import ZoneInfo
import random
import matplotlib.pyplot as plt


API_BASE = "https://fleet-batch.api.maymobility.com"
VIDEO_URL = f"{API_BASE}/v1/video"
DL_URL = f"{API_BASE}/v1/download"
VEHICLES = ["metatron"] 
CAMERA = "front-center" 
DURATION_SECONDS = 30  
STEP_SECONDS = 30  
THROTTLE_SEC = 0.2
DAY_TZ = dt.UTC
SKIP_ON_404_SEC = 300
LOOKBACK_S = 5 * 24 * 3600     
MAX_FILES_PER_VEHICLE = None 
MIN_VALID_BYTES = 1_000_000
MAX_OUTPUT_BYTES = 10 * 1024**3
BUCKET_PREFIX = "download"

OUTPUT_DIR = pathlib.Path("downloads")

def _highest_bucket_idx(vehicle_dir: Path) -> int:
    if not vehicle_dir.exists():
        return 0
    mx = 0
    for child in vehicle_dir.iterdir():
        if child.is_dir() and child.name.startswith(BUCKET_PREFIX):
            tail = child.name[len(BUCKET_PREFIX):]
            try:
                mx = max(mx, int(tail))
            except ValueError:
                pass
    return mx

def _vehicle_bucket_dir(vehicle_dir: Path) -> Path:
    vehicle_dir.mkdir(parents=True, exist_ok=True)

    # ensure there is at least download1
    idx = _highest_bucket_idx(vehicle_dir)
    if idx == 0:
        idx = 1
        (vehicle_dir / f"{BUCKET_PREFIX}{idx}").mkdir(parents=True, exist_ok=True)

    bucket = vehicle_dir / f"{BUCKET_PREFIX}{idx}"
    size_bytes = _dir_size_bytes(bucket)
    if size_bytes >= MAX_OUTPUT_BYTES:
        idx += 1
        bucket = vehicle_dir / f"{BUCKET_PREFIX}{idx}"
        bucket.mkdir(parents=True, exist_ok=True)
        print(f"[INFO] download cap reached in {vehicle_dir} (size={size_bytes} bytes) → switching to {bucket}")

    return bucket


def _rel_out(p: Path) -> str:
    try:
        return str(p.relative_to(OUTPUT_DIR))
    except Exception:
        return str(p)


def _dir_size_bytes(p: Path) -> int:
    total = 0
    if not p.exists():
        return 0
    for root, _, files in os.walk(p):
        for fn in files:
            try:
                total += os.path.getsize(os.path.join(root, fn))
            except OSError:
                pass
    return total


def out_dir_for(vehicle: str, day_str: str) -> Path:
    vehicle_dir = OUTPUT_DIR / vehicle
    return _vehicle_bucket_dir(vehicle_dir)
    
def current_time_unix():
    return int(dt.datetime.now(dt.UTC).timestamp())

def wait_for_bag_ready(token, filename, max_wait_sec=100, base_sleep=3):
    headers = {"Authorization": f"Bearer {token}"}
    params  = {"filename": filename}
    deadline = time.time() + max_wait_sec
    sleep = base_sleep

    while time.time() < deadline:
        try:
            r = requests.head(DL_URL, headers=headers, params=params, timeout=15)
            code = r.status_code
            if code == 405:  
                r = requests.get(DL_URL, headers=headers, params=params, timeout=15, stream=False)
                code = r.status_code
        except requests.RequestException:
            code = None

        if code == 200:
            return True
        if code in (404, 500, 503) or code is None:
            time.sleep(sleep)
            sleep = min(sleep * 1.5, 30)
            continue

        time.sleep(sleep)
        sleep = min(sleep * 1.5, 30)

    return False

def request_video_filename(token, vehicle, camera, start_ts, duration_s=DURATION_SECONDS):
    end_ts = start_ts + duration_s
    
    headers = {"Authorization": f"Bearer {token}"}
    params = {"vehicle": vehicle, "camera": camera, "startTime": start_ts, "endTime": end_ts}
    try:
        r = requests.get(VIDEO_URL, headers=headers, params=params, timeout=(10, 120))
    except (requests.ReadTimeout, requests.ConnectionError) as e:
        when = dt.datetime.fromtimestamp(start_ts, dt.UTC).strftime("%Y-%m-%d %H:%M:%S")
        print(f"[MISS][timeout] {when}Z {vehicle} {camera}: {e}")
        return None, None
    
    code = r.status_code
    if code in (400, 404):
        body = (r.text or "").strip().replace("\n", " ")
        when = dt.datetime.fromtimestamp(start_ts, dt.UTC).strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{r.status_code}] {when}Z {vehicle} {camera}: {body[:160]}")
        
        return None, code
    
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        when = dt.datetime.fromtimestamp(start_ts, dt.UTC).strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{r.status_code}] {when}Z {vehicle} {camera}: {e}")
        return None, code

    try:
        data = r.json()
        if isinstance(data, dict) and "filename" in data:
            return data["filename"], 200
        if isinstance(data, str) and data.endswith(".bag"):
            return data, 200
    except ValueError:
        pass

    txt = (r.text or "").strip()
    if txt.endswith(".bag"):
        return txt, 200
    
    ctype = r.headers.get("Content-Type", "")
    snippet = (r.text or "")[:200].replace("\n", " ")
    print(f"[WARN] Unexpected /v1/video body (type={ctype!r}): {snippet!r}")
    return None, code

def _looks_like_url_bytes(b: bytes) -> bool:
    return b.startswith(b"http://") or b.startswith(b"https://")

def _download_stream(url, out_path, timeout=300, headers=None):
    with requests.get(url, stream=True, timeout=timeout, headers=headers, allow_redirects=True) as r:
        r.raise_for_status()
        tmp = out_path.with_suffix(out_path.suffix + ".part")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                if chunk:
                    if len(chunk) < 1024 and _looks_like_url_bytes(chunk.strip()):
                        next_url = chunk.decode("utf-8", errors="ignore").strip().split()[0]
                        f.close()
                        try: tmp.unlink(missing_ok=True)
                        except Exception: pass
                        return _download_stream(next_url, out_path, timeout=timeout, headers=None)
                    f.write(chunk)
        tmp.replace(out_path)
    return out_path

def _get_presigned_or_binary_response(token, filename, timeout=180):
    headers = {"Authorization": f"Bearer {token}"}
    params  = {"filename": filename}
    r = requests.get(DL_URL, headers=headers, params=params, stream=True, timeout=timeout, allow_redirects=False)

    if r.is_redirect or r.status_code in (302, 303, 307, 308):
        loc = r.headers.get("Location")
        if loc and loc.startswith(("http://", "https://")):
            r.close()
            return ("url", loc)

    ctype = (r.headers.get("Content-Type") or "").lower()
    if r.status_code == 200 and ("octet-stream" in ctype or "application/x-rosbag" in ctype or "binary" in ctype):
        prefix = r.raw.read(4096, decode_content=True)
        if _looks_like_url_bytes(prefix):
            r.close()
            presigned = prefix.decode("utf-8", errors="ignore").strip().split()[0]
            return ("url", presigned)
        r.close()
        r2 = requests.get(DL_URL, headers=headers, params=params, stream=True, timeout=timeout, allow_redirects=True)
        return ("resp", r2)

    if r.status_code == 200:
        body = r.content[:8192]
        txt = body.decode("utf-8", errors="ignore").strip()
        if txt.startswith(("http://", "https://")):
            r.close()
            return ("url", txt.split()[0])
        try:
            data = json.loads(txt)
            if isinstance(data, dict) and "url" in data:
                r.close()
                return ("url", data["url"])
        except Exception:
            pass
    r.close()
    return ("fail", f"status={r.status_code}, ctype={ctype}")

def _extract_filename_from_headers(resp, fallback_name):
    import re
    cd = resp.headers.get("Content-Disposition", "")
    m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', cd)
    if m:
        return m.group(1)
    return fallback_name

def download_bag(token, filename, out_dir=OUTPUT_DIR, final_filename: str | None = None):
    out_dir.mkdir(parents=True, exist_ok=True)
    if not wait_for_bag_ready(token, filename, max_wait_sec=120, base_sleep=3):
        return False

    kind, payload = _get_presigned_or_binary_response(token, filename, timeout=180)

    chosen_name = final_filename if final_filename else filename
    out_path = out_dir / chosen_name

    if kind == "url":
        return _download_stream(payload, out_path, timeout=300, headers=None)

    if kind == "resp":
        resp = payload
        if final_filename is None:
            suggested = _extract_filename_from_headers(resp, filename)
            out_path = out_dir / suggested

        tmp = out_path.with_suffix(out_path.suffix + ".part")
        with open(tmp, "wb") as f:
            for chunk in resp.itercontent(1 << 20):
                if chunk:
                    if len(chunk) < 1024 and _looks_like_url_bytes(chunk.strip()):
                        presigned = chunk.decode("utf-8", errors="ignore").strip().split()[0]
                        resp.close()
                        f.close()
                        try: tmp.unlink(missing_ok=True)
                        except Exception: pass
                        return _download_stream(presigned, out_path, timeout=300, headers=None)
                    f.write(chunk)
        resp.close()
        tmp.replace(out_path)
        return out_path

    print(f"[WARN] failed to download {filename}: {payload}")
    return False
    

def pull_videos(token, vehicles, camera, start_ts, total_seconds, window_seconds=30, bucket_by_end=False, throttle_sec=THROTTLE_SEC):
    if isinstance(vehicles, str):
        vehicles = [vehicles]

    end_ts = int(start_ts) + int(total_seconds)
    stats = {v: {"saved": 0, "miss": 0, "skip": 0} for v in vehicles}

    for vehicle in vehicles:
        ts = int(start_ts)
        start_h = dt.datetime.fromtimestamp(start_ts, dt.UTC).strftime("%Y-%m-%d %H:%M:%S")
        end_h   = dt.datetime.fromtimestamp(end_ts,   dt.UTC).strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n=== {vehicle} | {camera} | {start_h}Z → {end_h}Z ===")

        while ts < end_ts:
            remaining = end_ts - ts
            dur = int(min(window_seconds, remaining))
            dur = max(1, min(30, dur))
            fname, code = request_video_filename(token, vehicle, camera, ts, dur)
            if fname:
                bucket_ts = ts + (dur - 1 if bucket_by_end else 0)
                day_str = dt.datetime.fromtimestamp(bucket_ts, DAY_TZ).strftime("%Y-%m-%d")
                out_dir = out_dir_for(vehicle, day_str)
                path = download_bag(token, fname, out_dir)
                when = dt.datetime.fromtimestamp(ts, dt.UTC).strftime("%Y-%m-%d %H:%M:%S")
                if path:
                    stats[vehicle]["saved"] += 1
                    print(f"[OK]  {when}Z -> {_rel_out(path)}")
                else:
                    stats[vehicle]["skip"] += 1
                    print(f"[SKIP] {when}Z {vehicle}: download failed")
            else:
                stats[vehicle]["miss"] += 1
                when = dt.datetime.fromtimestamp(ts, dt.UTC).strftime("%Y-%m-%d %H:%M:%S")
                print(f"[MISS] {when}Z {vehicle} (code={code})")
            ts += dur
            time.sleep(throttle_sec)
    return stats

def midnight_utc_ts():
    today = dt.datetime.now(dt.UTC).date()
    midn  = dt.datetime.combine(today, dt.time(0, 0, 0, tzinfo=dt.UTC))
    return int(midn.timestamp())

def batch_scan_window(lookback_s, clip_s):
    end_ts  = midnight_utc_ts() - clip_s             
    start_ts = max(end_ts - lookback_s + clip_s, 0)   
    if start_ts > end_ts:
        start_ts = end_ts
    return start_ts, end_ts


def grab_first_n_per_vehicle(token, vehicles):
    if vehicles is None:
        vehicles = VEHICLES
    if isinstance(vehicles, str):
        vehicles = [vehicles]

    start_ts, end_ts = batch_scan_window(LOOKBACK_S, DURATION_SECONDS)

    for vehicle in vehicles:
        print(f"\n=== {vehicle} | {CAMERA} | scanning {dt.datetime.fromtimestamp(start_ts, dt.UTC):%Y-%m-%d %H:%M:%S}Z → {dt.datetime.fromtimestamp(end_ts, dt.UTC):%Y-%m-%d %H:%M:%S}Z ===")
        saved = 0
        ts = start_ts

        while ts <= end_ts and (MAX_FILES_PER_VEHICLE is None or saved < MAX_FILES_PER_VEHICLE):
            fname, code = request_video_filename(token, vehicle, CAMERA, ts, DURATION_SECONDS)
            if fname:
                day_str = dt.datetime.fromtimestamp(ts, DAY_TZ).strftime("%Y-%m-%d")
                out_dir = out_dir_for(vehicle, day_str)
                path = download_bag(token, fname, out_dir)
                when = dt.datetime.fromtimestamp(ts, dt.UTC).strftime("%Y-%m-%d %H:%M:%S")
                if path:
                    saved += 1
                    print(f"[OK]  {when}Z -> {_rel_out(path)} ({saved}/{MAX_FILES_PER_VEHICLE})")
                else:
                    print(f"[SKIP] {when}Z {vehicle}: download not ready/failed twice")
                ts += DURATION_SECONDS
            else:
                ts += SKIP_ON_404_SEC if code == 404 else DURATION_SECONDS
                time.sleep(THROTTLE_SEC)
        print(f"[DONE] {vehicle}: saved {saved} file(s).")

def looks_like_url_file(path: Path) -> bool:
    try:
        head = path.open("rb").read(32)
        return head.startswith(b"http://") or head.startswith(b"https://")
    except Exception:
        return False

def grab_random_clips(token, vehicles, num_trials=10, duration=DURATION_SECONDS):
    if isinstance(vehicles, str):
        vehicles = [vehicles]

    start_ts, end_ts = batch_scan_window(LOOKBACK_S, duration)

    for vehicle in vehicles:
        print(f"\n=== {vehicle} random trials ({num_trials}) ===")
        saved = 0
        for trial in range(num_trials):
            ts = random.randint(start_ts, end_ts - duration)
            fname, code = request_video_filename(token, vehicle, CAMERA, ts, duration)
            when = dt.datetime.fromtimestamp(ts, dt.UTC).strftime("%Y-%m-%d %H:%M:%S")

            if not fname:
                print(f"[MISS] {when}Z {vehicle} (code={code})")
                continue

            day_str = dt.datetime.fromtimestamp(ts, dt.UTC).strftime("%Y-%m-%d")
            out_dir = out_dir_for(vehicle, day_str)
            path = download_bag(token, fname, out_dir)

            if path:
                sz = path.stat().st_size
                if sz < 1_000_000: 
                    print(f"[WARN] {when}Z {vehicle} got tiny file ({sz} bytes) – retrying.")
                else:
                    saved += 1
                    print(f"[OK] {when}Z -> {_rel_out(path)} (size={sz/1e6:.1f} MB)")
                    break 
            else:
                print(f"[FAIL] {when}Z {vehicle} download failed")

            time.sleep(THROTTLE_SEC)

        print(f"[DONE] {vehicle}: {saved} valid bag(s) downloaded.")

def day_bounds_utc(year: int, month: int, day: int) -> tuple[int, int]:
    start_dt = dt.datetime(year, month, day, 0, 0, 0, tzinfo=dt.UTC)
    start_ts = int(start_dt.timestamp())
    end_ts   = start_ts + 24 * 3600
    return start_ts, end_ts


def grab_whole_day(token: str, vehicles, camera: str,
                   year: int, month: int, day: int,
                   max_files_per_vehicle: int | None = None):
    if isinstance(vehicles, str):
        vehicles = [vehicles]

    start_ts, end_ts = day_bounds_utc(year, month, day)
    total_seconds = end_ts - start_ts
    today_start = int(dt.datetime.now(dt.UTC).replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    if start_ts >= today_start:
        raise ValueError("That day is 'today' in UTC. The batch API requires using the realtime API for the current day.")

    cap = max_files_per_vehicle
    print(f"\n=== WHOLE DAY {year:04d}-{month:02d}-{day:02d} UTC ===")
    print(f"Window: {dt.datetime.fromtimestamp(start_ts, dt.UTC):%Y-%m-%d %H:%M:%S}Z"
          f" → {dt.datetime.fromtimestamp(end_ts, dt.UTC):%Y-%m-%d %H:%M:%S}Z")

    stats = {}
    for vehicle in vehicles:
        saved = 0
        ts = start_ts
        stats[vehicle] = {"saved": 0, "miss": 0, "skip": 0}
        while ts < end_ts and (cap is None or saved < cap):
            fname, code = request_video_filename(token, vehicle, camera, ts, DURATION_SECONDS)
            when = dt.datetime.fromtimestamp(ts, dt.UTC).strftime("%Y-%m-%d %H:%M:%S")
            if fname:
                day_str = dt.datetime.fromtimestamp(ts, DAY_TZ).strftime("%Y-%m-%d")
                out_dir = out_dir_for(vehicle, day_str)
                final_name = timeslot_filename(vehicle, camera, ts, DURATION_SECONDS)
                path = download_bag(token, fname, out_dir, final_filename=final_name)

                if not path:
                    stats[vehicle]["skip"] += 1
                    print(f"[SKIP] {when}Z {vehicle}: download failed")
                else:
                    sz = path.stat().st_size
                    if sz < MIN_VALID_BYTES:
                        try: path.unlink()
                        except Exception: pass
                        stats[vehicle]["skip"] += 1
                        print(f"[WARN] {when}Z {vehicle} tiny file ({sz} bytes) – counted as SKIP")
                    else:
                        saved += 1
                        stats[vehicle]["saved"] += 1
                        cap_str = f" ({saved}/{cap})" if cap else ""
                        print(f"[OK]  {when}Z -> {_rel_out(path)} (size={sz/1e6:.1f} MB){cap_str}")
            else:
                stats[vehicle]["miss"] += 1
                print(f"[MISS] {when}Z {vehicle} (code={code})")

            ts += DURATION_SECONDS

    return stats

def timeslot_filename(vehicle: str, camera: str, start_ts: int, dur_s: int) -> str:
    t = dt.datetime.fromtimestamp(start_ts, dt.UTC)
    stamp = t.strftime("%Y-%m-%dT%H-%M-%SZ")
    return f"{vehicle}_{camera}_{stamp}_{dur_s}s.bag"

def window_bounds_utc(
    year: int, month: int, day: int,
    start_hour: int = 0, start_minute: int = 0,
    duration_hours: int = 12, duration_minutes: int = 0
) -> tuple[int, int]:
    start_dt = dt.datetime(year, month, day, start_hour, start_minute, 0, tzinfo=dt.UTC)
    end_dt = start_dt + dt.timedelta(hours=duration_hours, minutes=duration_minutes)
    return int(start_dt.timestamp()), int(end_dt.timestamp())


def grab_window(token: str, vehicles, camera: str,
                year: int, month: int, day: int,
                start_hour: int = 0, start_minute: int = 0,
                duration_hours: int = 12, duration_minutes: int = 0,
                max_files_per_vehicle: int | None = None):
    if isinstance(vehicles, str):
        vehicles = [vehicles]

    start_ts, end_ts = window_bounds_utc(
        year, month, day,
        start_hour=start_hour, start_minute=start_minute,
        duration_hours=duration_hours, duration_minutes=duration_minutes
    )
    cap = max_files_per_vehicle
    stats = {}
    for vehicle in vehicles:
        saved = 0
        ts = start_ts
        stats[vehicle] = {"saved": 0, "miss": 0, "skip": 0}
        while ts < end_ts and (cap is None or saved < cap):
            fname, code = request_video_filename(token, vehicle, camera, ts, DURATION_SECONDS)
            when = dt.datetime.fromtimestamp(ts, dt.UTC).strftime("%Y-%m-%d %H:%M:%S")
            if fname:
                day_str = dt.datetime.fromtimestamp(ts, dt.UTC).strftime("%Y-%m-%d")
                out_dir = out_dir_for(vehicle, day_str)
                final_name = f"{vehicle}_{camera}_{dt.datetime.fromtimestamp(ts, dt.UTC):%Y-%m-%dT%H-%M-%SZ}_{DURATION_SECONDS}s.bag"
                path = download_bag(token, fname, out_dir, final_filename=final_name)
                if not path:
                    stats[vehicle]["skip"] += 1
                    print(f"[SKIP] {when}Z {vehicle}: download failed")
                else:
                    sz = path.stat().st_size
                    if sz < MIN_VALID_BYTES:
                        try: path.unlink()
                        except Exception: pass
                        stats[vehicle]["skip"] += 1
                        print(f"[WARN] {when}Z {vehicle} tiny file ({sz} bytes) – counted as SKIP")
                    else:
                        saved += 1
                        stats[vehicle]["saved"] += 1
                        cap_str = f" ({saved}/{cap})" if cap else ""
                        print(f"[OK]  {when}Z -> {_rel_out(path)} (size={sz/1e6:.1f} MB){cap_str}")
            else:
                stats[vehicle]["miss"] += 1
                print(f"[MISS] {when}Z {vehicle} (code={code})")
            ts += DURATION_SECONDS
    return stats

def scan_availability(token: str,
                      vehicle: str,
                      camera: str,
                      start_ts: int,
                      end_ts: int,
                      window_seconds: int = DURATION_SECONDS,
                      skip_on_404_sec: int = SKIP_ON_404_SEC):
    """
    Probe /v1/video to see when video is available without downloading.
    Returns:
        samples: list[(ts, has_video, code)]
        segments: list[(start_ts, end_ts)] where video is continuously available
    """
    samples = []
    ts = int(start_ts)

    while ts < end_ts:
        dur = min(window_seconds, end_ts - ts)
        dur = max(1, min(30, dur))

        fname, code = request_video_filename(token, vehicle, camera, ts, dur)
        has_video = fname is not None
        samples.append((ts, has_video, code))

        if not has_video and code == 404:
            ts += skip_on_404_sec
        else:
            ts += dur

    # compress into contiguous availability segments
    segments = []
    seg_start = None
    prev_ts = None

    for ts, has_video, _ in samples:
        if has_video:
            if seg_start is None:
                seg_start = ts
        else:
            if seg_start is not None:
                segments.append((seg_start, prev_ts + window_seconds))
                seg_start = None
        prev_ts = ts

    if seg_start is not None:
        segments.append((seg_start, end_ts))

    return samples, segments


def visualize_availability(samples, title: str = ""):
    """
    Simple timeline visualization using matplotlib.
    samples: list[(ts, has_video, code)]
    """
    times = [dt.datetime.fromtimestamp(ts, dt.UTC) for ts, _, _ in samples]
    vals  = [1 if has else 0 for _, has, _ in samples]

    plt.figure(figsize=(12, 2))
    plt.step(times, vals, where="post")
    plt.yticks([0, 1], ["no video", "video"])
    plt.ylim(-0.2, 1.2)
    plt.xlabel("time (UTC)")
    plt.title(title or "Video availability")
    plt.tight_layout()
    plt.show()


def print_segments(segments):
    """
    Print contiguous availability segments in human-readable form.
    """
    for i, (s, e) in enumerate(segments, 1):
        sh = dt.datetime.fromtimestamp(s, dt.UTC).strftime("%Y-%m-%d %H:%M:%S")
        eh = dt.datetime.fromtimestamp(e, dt.UTC).strftime("%Y-%m-%d %H:%M:%S")
        print(f"[SEG {i}] {sh}Z → {eh}Z")

def visualize_availability_multi(per_vehicle_samples, camera: str, start_ts: int):
    """
    per_vehicle_samples: dict[vehicle -> list[(ts, has_video, code)]]
    """
    n = len(per_vehicle_samples)
    fig, axes = plt.subplots(n, 1, sharex=True, figsize=(12, 2 * n))

    if n == 1:
        axes = [axes]

    for ax, (vehicle, samples) in zip(axes, per_vehicle_samples.items()):
        times = [dt.datetime.fromtimestamp(ts, dt.UTC) for ts, _, _ in samples]
        vals  = [1 if has else 0 for _, has, _ in samples]

        ax.step(times, vals, where="post")
        ax.set_yticks([0, 1])
        ax.set_yticklabels(["no", "yes"])
        ax.set_ylim(-0.2, 1.2)
        ax.set_ylabel(vehicle)

    axes[-1].set_xlabel("time (UTC)")
    fig.suptitle(f"Video availability per vehicle | camera={camera} | start={dt.datetime.fromtimestamp(start_ts, dt.UTC):%Y-%m-%d}")
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    token = "eyJraWQiOiJvcXJNWDRyVXJ2V21lbVhENDRFRVwvWFF0YWFaY3dWbFZzM3M4RElKekREcz0iLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiI2NzNzZ3V2ZW05ZnJxdnZmMWg3bGI4YjY5NCIsInRva2VuX3VzZSI6ImFjY2VzcyIsInNjb3BlIjoiYmF0Y2gtdGVsZW1ldHJ5XC9jY3RhIiwiYXV0aF90aW1lIjoxNzYzNDI5MzY5LCJpc3MiOiJodHRwczpcL1wvY29nbml0by1pZHAudXMtZWFzdC0xLmFtYXpvbmF3cy5jb21cL3VzLWVhc3QtMV9oRTI5U1hRVXYiLCJleHAiOjE3NjM1MTU3NjksImlhdCI6MTc2MzQyOTM2OSwidmVyc2lvbiI6MiwianRpIjoiYjE5YmJkMTItODM3Zi00ZGU2LTkxMmYtOTVjZTA5ODJkMzRjIiwiY2xpZW50X2lkIjoiNjczc2d1dmVtOWZycXZ2ZjFoN2xiOGI2OTQifQ.e5LdP0j3bPGAZYz0WGRFB0W_aC1rtGYd1wsHEbarB2XvT8sNqkmRTCs9gRxfyPfcmJ21ynJFEQ31W8NlG4v39O4SszlKTgkYZmGvZjkmXescsadQId7y6D4r1j54mu6QcWPof-ApKFwViHBDMcVa_aNZ6BvHEgY8WvvpZJ0a_R_8G7e3AMbMUeyw-P9PqCJTVcrOLKgisVacTJdIv70OGkumjWk6y3Puwv-Ujl-1l4CaydMEY7lkAWfbH4fgakrtVOBnMdpuIYhRfh75Q-5LY06AEiBLlS1u_6t606xAqdc1cN3osFbXhDdjsrmDUFBs7eFSyx_d0YhVP-9wuprNTQ"

    # example: scan a 1-hour window, same as your grab_window call
    start_ts, end_ts = window_bounds_utc(
        2025, 11, 13,
        start_hour=0,
        start_minute=0,
        duration_hours=24*5,
        duration_minutes=0,
    )

    vehicles = ["mallory", "megalodon", "morizo", "mav", "metatron", "mastermind", "marymae"]
    camera = "front-center"
    per_vehicle_samples = {}

    for vehicle in vehicles:
        print(f"\n========== {vehicle} | {camera} ==========")
        samples, segments = scan_availability(
            token,
            vehicle,
            camera,
            start_ts,
            end_ts,
            window_seconds=DURATION_SECONDS,
        )
        per_vehicle_samples[vehicle] = samples
        print_segments(segments)

    visualize_availability_multi(per_vehicle_samples, camera, start_ts)