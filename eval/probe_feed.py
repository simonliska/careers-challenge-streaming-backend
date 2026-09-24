"""SSE probe for alarm feed latency (stdlib only).

Subscribes to GET /alarms/feed?since=<now> before a load run, timestamps
each `data:` arrival, and reports e2e delay (arrival_wall - event ts).

Run:
    python3 eval/probe_feed.py --target http://localhost:9090 --duration 60
"""
import argparse
import datetime
import json
import sys
import time
import urllib.request


def parse_ts(s: str) -> float:
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.datetime.fromisoformat(s).timestamp()
    except ValueError:
        return 0.0


def percentile(xs: list, q: float) -> float:
    if not xs:
        return 0.0
    xs = sorted(xs)
    return xs[int(q * (len(xs) - 1))]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--target", default="http://localhost:9090")
    p.add_argument("--duration", type=float, default=60.0)
    p.add_argument("--since", default="")
    args = p.parse_args()

    since = args.since
    if not since:
        since = datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    url = f"{args.target.rstrip('/')}/alarms/feed?since={since}"
    req = urllib.request.Request(url, headers={"Accept": "text/event-stream"})
    print(f"Probing {url} for {args.duration:.0f}s", file=sys.stderr)

    delays = []
    count = 0
    end = time.time() + args.duration
    try:
        with urllib.request.urlopen(req, timeout=args.duration + 10) as resp:
            while time.time() < end:
                line = resp.readline().decode("utf-8", "replace").strip()
                if not line:
                    continue
                if line.startswith(":"):
                    continue
                if not line.startswith("data:"):
                    continue
                arrival = time.time()
                try:
                    payload = json.loads(line[5:].strip())
                except ValueError:
                    continue
                ts = parse_ts(payload.get("ts", ""))
                if ts > 0:
                    delays.append((arrival - ts) * 1000.0)
                count += 1
                print(line, flush=True)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"probe ended: {e}", file=sys.stderr)

    p50 = percentile(delays, 0.50)
    p95 = percentile(delays, 0.95)
    print(f"\nprobe alarms={count} e2e_ms p50={p50:.1f} p95={p95:.1f}",
          file=sys.stderr)
    sys.stderr.write(json.dumps({"probe": {"alarms": count, "e2e_p50_ms": p50,
                                            "e2e_p95_ms": p95}}) + "\n")


if __name__ == "__main__":
    main()
