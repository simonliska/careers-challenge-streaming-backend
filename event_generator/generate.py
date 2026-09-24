"""
Event generator for the Teton streaming backend challenge.

Simulates N devices in care rooms emitting heartbeat / presence / motion /
sleep_state / fall_warn / net_status events. POSTs each event as JSON to
your service's /events endpoint.

Modes:
    baseline     N devices × ~1 event/sec each, mixed types
    burst        baseline + two 10x bursts of 30s
    offline      baseline + 20% of devices go silent for 60s then replay
    adversarial  burst + offline + ±30s clock skew

Run:
    python event_generator/generate.py --mode baseline --duration 60 --devices 100
    python event_generator/generate.py --mode burst --duration 180 --target http://localhost:8080

Smaller defaults than production (100 devices, not 5000) so you can iterate
on a laptop. Crank --devices when you're ready to stress-test.
"""

import argparse
import hashlib
import json
import queue
import random
import sys
import threading
import time
from collections import deque
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


def make_devices(n: int, offset: int = 0) -> list:
    """Return a list of n device descriptors, ~2 devices per room on average."""
    devices = []
    for i in range(n):
        idx = i + offset
        room_idx = idx // 2
        devices.append({
            "device_id": f"dev_{idx:04d}",
            "room_id":   f"room_{room_idx:03d}",
            "seq":       0,
            "clock_skew": 0.0,        # set later per scenario
            "offline_until": 0.0,     # set later per scenario
            "buffer": deque(),        # events buffered while offline
        })
    return devices


def make_event(device: dict, etype: str, now: float) -> dict:
    """Build an event dict for a device. `now` is wall-clock seconds."""
    device["seq"] += 1
    ts = now + device["clock_skew"]
    e = {
        "device_id": device["device_id"],
        "room_id":   device["room_id"],
        "type":      etype,
        "ts":        time.strftime("%Y-%m-%dT%H:%M:%S",
                                   time.gmtime(ts)) + f".{int(ts * 1000) % 1000:03d}Z",
        "seq":       device["seq"],
    }
    if etype == "presence":
        e["in_room"] = random.choice([True, False])
    elif etype == "motion":
        e["magnitude"] = round(random.random(), 2)
    elif etype == "sleep_state":
        e["state"] = random.choice(["asleep", "awake", "unknown"])
    elif etype == "fall_warn":
        e["confidence"] = round(random.uniform(0.7, 0.99), 2)
    elif etype == "net_status":
        e["rssi"] = random.randint(-90, -50)
    return e


# Weights chosen to roughly match: heartbeat ~1Hz, motion ~0.3Hz, others rare.
EVENT_WEIGHTS = [
    ("heartbeat",    50),
    ("motion",       15),
    ("presence",      4),
    ("sleep_state",   3),
    ("net_status",    3),
    ("fall_warn",     1),   # rare; jitter handled below
]
_EVENT_TYPES, _WEIGHTS = zip(*EVENT_WEIGHTS)


def pick_event_type() -> str:
    return random.choices(_EVENT_TYPES, weights=_WEIGHTS, k=1)[0]


class Sender:
    """Posts events to the target URL, tracks counts and failures."""

    def __init__(self, target: str):
        self.target = target.rstrip("/") + "/events"
        self.sent = 0
        self.failed = 0
        self.lock = threading.Lock()

    def send(self, event: dict) -> None:
        data = json.dumps(event).encode()
        req = Request(self.target, data=data, method="POST",
                      headers={"Content-Type": "application/json"})
        try:
            with urlopen(req, timeout=5) as resp:
                resp.read()
            with self.lock:
                self.sent += 1
        except (URLError, HTTPError, OSError):
            with self.lock:
                self.failed += 1


# Bounded per-worker queue depth for the pool path. Blocking put() applies
# generator-side backpressure when senders saturate (mirrors server-side).
_POOL_QUEUE_SIZE = 4096


def _pool_worker(host: str, port: int, use_https: bool, path: str,
                 q: "queue.Queue", stats: dict) -> None:
    """Drain one worker queue over a persistent HTTP connection (keep-alive).

    Send-once like the legacy Sender: any transport error counts the item
    as failed, drops the connection, and moves on. Per-status and
    per-exception counters plus failure timestamps are recorded for
    diagnosis only and change nothing on the wire.
    """
    import http.client
    import time as _time
    t0 = stats.get("_t0", _time.time())
    conn = None
    while True:
        item = q.get()
        if item is None:
            return
        try:
            if conn is None:
                if use_https:
                    conn = http.client.HTTPSConnection(host, port, timeout=5)
                else:
                    conn = http.client.HTTPConnection(host, port, timeout=5)
            conn.request("POST", path, body=item,
                         headers={"Content-Type": "application/json",
                                  "Connection": "keep-alive"})
            resp = conn.getresponse()
            resp.read()
            if resp.status >= 400:
                stats["failed"] += 1
                stats[f"http_{resp.status}"] = stats.get(f"http_{resp.status}", 0) + 1
            else:
                stats["sent"] += 1
        except Exception as e:
            stats["failed"] += 1
            key = f"exc_{type(e).__name__}"
            stats[key] = stats.get(key, 0) + 1
            stats.setdefault("fail_at", []).append(round(_time.time() - t0))
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
            conn = None


class PoolSender:
    """Threaded keep-alive sender. Same send() signature as Sender.

    Events route by hash(device_id) % workers, so each device is owned by
    exactly one worker thread draining FIFO — per-device send order holds.
    """

    def __init__(self, target: str, workers: int):
        parts = urlparse(target.rstrip("/"))
        self.use_https = parts.scheme == "https"
        self.host = parts.hostname or "localhost"
        default_port = 443 if self.use_https else 80
        self.port = parts.port or default_port
        self.path = (parts.path or "") + "/events"
        self.workers = max(1, workers)
        self.queues = [queue.Queue(maxsize=_POOL_QUEUE_SIZE)
                       for _ in range(self.workers)]
        self.stats = [{"sent": 0, "failed": 0} for _ in range(self.workers)]
        self.threads = []
        for i in range(self.workers):
            t = threading.Thread(target=_pool_worker,
                                 args=(self.host, self.port, self.use_https,
                                       self.path, self.queues[i], self.stats[i]),
                                 daemon=True)
            t.start()
            self.threads.append(t)

    def _route(self, event: dict) -> int:
        h = hashlib.md5(event.get("device_id", "").encode()).digest()
        return int.from_bytes(h[:4], "little") % self.workers

    def send(self, event: dict) -> None:
        data = json.dumps(event).encode()
        self.queues[self._route(event)].put(data)

    def close(self) -> None:
        for q in self.queues:
            q.put(None)
        for t in self.threads:
            t.join()

    @property
    def sent(self) -> int:
        return sum(s["sent"] for s in self.stats)

    @property
    def failed(self) -> int:
        return sum(s["failed"] for s in self.stats)


def emit_one(device: dict, now: float, sender: Sender, gt: dict) -> None:
    """Pick an event type, emit (or buffer if device offline), track ground truth."""
    etype = pick_event_type()
    event = make_event(device, etype, now)

    # Track ground truth (sum of fall_warns is what dedup must reduce to N distinct
    # facts; total events sent overall used as smoke total).
    with gt["lock"]:
        gt["total"] += 1
        if etype == "fall_warn":
            # Sensor jitter: emit each fall as 1-3 nearly-identical events
            jitter = random.randint(1, 3)
            for _ in range(jitter - 1):
                device["seq"] += 1
                copy = dict(event)
                copy["seq"] = device["seq"]
                _send_or_buffer(device, copy, now, sender)
                gt["total"] += 1
            gt["distinct_falls"] += 1

    _send_or_buffer(device, event, now, sender)


def _send_or_buffer(device: dict, event: dict, now: float, sender: Sender) -> None:
    if now < device["offline_until"]:
        device["buffer"].append(event)
        return
    # If we just came back online, flush the buffer first
    while device["buffer"]:
        sender.send(device["buffer"].popleft())
    sender.send(event)


def schedule_offline(devices: list, fraction: float, duration: float, when: float) -> None:
    """Mark `fraction` of devices offline for `duration` seconds, starting at `when`."""
    sample = random.sample(devices, k=int(len(devices) * fraction))
    for d in sample:
        d["offline_until"] = when + duration


def add_clock_skew(devices: list, max_skew: float) -> None:
    """Give each device a fixed ± random clock skew up to max_skew seconds."""
    for d in devices:
        d["clock_skew"] = random.uniform(-max_skew, max_skew)


def run(devices: list, target: str, duration: float, rps_per_device: float,
        burst_at: list, offline_at: list, workers: int = 1) -> dict:
    """Run the simulation. Returns ground-truth dict."""
    if workers > 1:
        sender = PoolSender(target, workers)
    else:
        sender = Sender(target)
    gt = {"total": 0, "distinct_falls": 0, "lock": threading.Lock()}
    started = time.time()
    end = started + duration
    next_tick = [started + 1.0 / rps_per_device for _ in devices]

    print(f"Sending events to {target}/events for {duration:.0f}s "
          f"({len(devices)} devices × {rps_per_device}/sec each, "
          f"{workers} sender worker(s))")

    while True:
        now = time.time()
        if now >= end:
            break

        # Apply scheduled bursts (10x rate for 30s).
        rate_multiplier = 1.0
        for burst_start, burst_dur in burst_at:
            if burst_start <= (now - started) < burst_start + burst_dur:
                rate_multiplier = 10.0
                break

        # Apply scheduled offline windows.
        for off_start, off_dur, off_fraction in offline_at:
            if abs((now - started) - off_start) < 0.5:
                schedule_offline(devices, off_fraction, off_dur, now)

        # Emit events that are due
        for i, d in enumerate(devices):
            if now < next_tick[i]:
                continue
            emit_one(d, now, sender, gt)
            next_tick[i] = now + (1.0 / rps_per_device) / rate_multiplier

        # Tight loop, but yield briefly so we're not pegging a core
        time.sleep(0.005)

    if isinstance(sender, PoolSender):
        sender.close()
    print(f"\nGround truth:")
    print(f"  total events sent:    {gt['total']} (incl. fall jitter)")
    print(f"  distinct falls:       {gt['distinct_falls']} (dedup target)")
    print(f"  HTTP sent ok:         {sender.sent}")
    print(f"  HTTP failed:          {sender.failed}")
    if isinstance(sender, PoolSender):
        detail = {}
        fail_at = []
        for s in sender.stats:
            for k, v in s.items():
                if k == "fail_at":
                    fail_at.extend(v)
                elif k not in ("sent", "failed") and not k.startswith("_") and v:
                    detail[k] = detail.get(k, 0) + v
        if detail:
            print(f"  failure detail:       {detail}")
        if fail_at:
            from collections import Counter
            print(f"  failure timing (s):   {dict(sorted(Counter(fail_at).items()))}")
    return {"total": gt["total"], "distinct_falls": gt["distinct_falls"],
            "sent_ok": sender.sent, "failed": sender.failed}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["baseline", "burst", "offline", "adversarial"],
                   default="baseline")
    p.add_argument("--target", default="http://localhost:8080")
    p.add_argument("--devices", type=int, default=100,
                   help="Number of devices to simulate (default: 100)")
    p.add_argument("--device-offset", type=int, default=0,
                   help="Starting device index (default: 0). Use disjoint "
                        "offsets to run parallel generators without ID overlap.")
    p.add_argument("--workers", type=int, default=1,
                   help="Sender threads with keep-alive HTTP (default: 1, "
                        "legacy sync behavior). Crank for peak load.")
    p.add_argument("--duration", type=float, default=60.0,
                   help="Seconds to run (default: 60)")
    p.add_argument("--rps-per-device", type=float, default=1.0,
                   help="Base events/sec per device (default: 1.0)")
    args = p.parse_args()

    devices = make_devices(args.devices, args.device_offset)

    burst_at = []
    offline_at = []
    if args.mode in ("burst", "adversarial"):
        burst_at = [(15.0, 30.0), (90.0, 30.0)]
    if args.mode in ("offline", "adversarial"):
        offline_at = [(30.0, 60.0, 0.20)]
    if args.mode == "adversarial":
        add_clock_skew(devices, max_skew=30.0)

    try:
        gt = run(devices, args.target, args.duration, args.rps_per_device,
                 burst_at, offline_at, args.workers)
        # Print the ground-truth as JSON so eval/check.py can pick it up
        sys.stderr.write(json.dumps({"ground_truth": gt}) + "\n")
    except KeyboardInterrupt:
        print("\nInterrupted")


if __name__ == "__main__":
    main()
