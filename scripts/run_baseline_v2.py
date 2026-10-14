import csv, getpass, os, re, statistics, subprocess, sys, threading, time
import oracledb

CONC, TXNS, WARMUP_TXNS, RUNS, SEED = 10, 500, 100, 5, 42
CONTAINER = "oracle-db"
UNITS = {"B": 1, "KB": 1e3, "MB": 1e6, "GB": 1e9,
         "KIB": 1024, "MIB": 1024**2, "GIB": 1024**3}
NAN = float("nan")

password = getpass.getpass("ai_lab password: ")
os.environ["AI_LAB_PASSWORD"] = password

SNAP_SQL = """ALTER SESSION SET CONTAINER = FREEPDB1;
SET PAGESIZE 0 FEEDBACK OFF HEADING OFF LINESIZE 300 TRIMSPOOL ON
SELECT 'EV|'||event||'|'||total_waits||'|'||time_waited_micro FROM v$system_event WHERE wait_class <> 'Idle';
SELECT 'ST|'||name||'|'||value FROM v$sysstat WHERE name IN ('physical read total IO requests','physical write total IO requests','physical read total bytes','physical write total bytes','redo size','user commits');
EXIT
"""


def reset():
    with oracledb.connect(user="ai_lab", password=password,
                          dsn="localhost:1521/FREEPDB1") as c:
        cur = c.cursor()
        cur.execute("DELETE FROM order_items WHERE order_id IN "
                    "(SELECT order_id FROM orders WHERE status = 'NEW')")
        cur.execute("DELETE FROM orders WHERE status = 'NEW'")
        c.commit()


def to_mb(s):
    m = re.match(r"([\d.]+)\s*([A-Za-z]+)", s.strip())
    return float(m.group(1)) * UNITS[m.group(2).upper()] / 1024**2


def cpu_usec():
    out = subprocess.run(["podman", "exec", CONTAINER, "cat",
                          "/sys/fs/cgroup/cpu.stat"],
                         capture_output=True, text=True).stdout
    m = re.search(r"usage_usec\s+(\d+)", out)
    return int(m.group(1)) if m else None


def snapshot():
    out = subprocess.run(["podman", "exec", "-i", CONTAINER, "sqlplus", "-S",
                          "/", "as", "sysdba"],
                         input=SNAP_SQL, capture_output=True, text=True).stdout
    events, stats = {}, {}
    for line in out.splitlines():
        if line.startswith("EV|"):
            ev, w, t = line[3:].rsplit("|", 2)
            events[ev] = (int(w), int(t))
        elif line.startswith("ST|"):
            name, v = line[3:].rsplit("|", 1)
            stats[name] = float(v)
    return events, stats


def mem_sampler(stop, samples):
    while not stop.is_set():
        out = subprocess.run(
            ["podman", "stats", "--no-stream", "--format", "{{.MemUsage}}",
             CONTAINER], capture_output=True, text=True).stdout.strip()
        try:
            samples.append(to_mb(out.split("/")[0]))
        except Exception:
            pass


def run_once(txns):
    reset()
    ev0, st0 = snapshot()
    stop, mem = threading.Event(), []
    t = threading.Thread(target=mem_sampler, args=(stop, mem))
    t.start()
    c0, w0 = cpu_usec(), time.perf_counter()
    out = subprocess.run(
        [sys.executable, "benchmark_oltp.py", "--concurrency", str(CONC),
         "--transactions", str(txns), "--seed", str(SEED)],
        capture_output=True, text=True).stdout
    c1, w1 = cpu_usec(), time.perf_counter()
    stop.set()
    t.join()
    ev1, st1 = snapshot()

    def g(pattern):
        m = re.search(pattern, out)
        return float(m.group(1)) if m else NAN

    def d(name):
        return st1.get(name, NAN) - st0.get(name, NAN)

    waits = {e: ((ev1[e][1] - ev0.get(e, (0, 0))[1]) / 1000.0,
                 ev1[e][0] - ev0.get(e, (0, 0))[0]) for e in ev1}
    return {
        "success": g(r"Successful transactions:\s+(\d+)"),
        "failed": g(r"Failed transactions:\s+(\d+)"),
        "duration_s": g(r"Duration:\s+([\d.]+) sec"),
        "tps": g(r"Throughput:\s+([\d.]+) TPS"),
        "avg_ms": g(r"Average:\s+([\d.]+) ms"),
        "p95_ms": g(r"p95:\s+([\d.]+) ms"),
        "p99_ms": g(r"p99:\s+([\d.]+) ms"),
        "db_cpu_pct": ((c1 - c0) / ((w1 - w0) * 1e6) * 100
                       if c0 is not None and c1 is not None else NAN),
        "db_mem_mb": max(mem) if mem else NAN,
        "io_requests": d("physical read total IO requests")
                       + d("physical write total IO requests"),
        "io_mb": (d("physical read total bytes")
                  + d("physical write total bytes")) / 1024**2,
        "redo_mb": d("redo size") / 1024**2,
        "commits": d("user commits"),
        "_waits": waits,
    }


print("Warm-up run (discarded)...")
run_once(WARMUP_TXNS)

rows = []
for i in range(1, RUNS + 1):
    print(f"Measured run {i}/{RUNS}...")
    r = run_once(TXNS)
    r["run"] = i
    rows.append(r)

reset()
cols = ["run", "success", "failed", "duration_s", "tps", "avg_ms", "p95_ms",
        "p99_ms", "db_cpu_pct", "db_mem_mb", "io_requests", "io_mb",
        "redo_mb", "commits"]

print()
print("PER-RUN RESULTS")
print("  ".join(f"{c:>11}" for c in cols))
for r in rows:
    print("  ".join(f"{r[c]:>11.2f}" for c in cols))

print()
print("SUMMARY (median, min, max, variability = stdev / mean)")
for c in cols[4:]:
    v = [r[c] for r in rows if r[c] == r[c]]
    if not v:
        print(f"{c:>12}: no data")
        continue
    cv = statistics.pstdev(v) / statistics.mean(v) * 100 if statistics.mean(v) else 0
    print(f"{c:>12}: median {statistics.median(v):10.2f}  min {min(v):10.2f}  "
          f"max {max(v):10.2f}  variability {cv:5.1f}%")
print(f"total failed transactions across runs: {sum(r['failed'] for r in rows):.0f}")

events = set()
for r in rows:
    events |= set(r["_waits"])
summary = []
for e in events:
    ms = [r["_waits"].get(e, (0, 0))[0] for r in rows]
    wt = [r["_waits"].get(e, (0, 0))[1] for r in rows]
    summary.append((e, statistics.median(ms), statistics.median(wt)))
summary.sort(key=lambda x: -x[1])

print()
print("TOP WAIT EVENTS PER RUN (median across runs, non-idle, all sessions)")
print(f"{'event':<45}{'time_ms':>12}{'waits':>12}")
for e, ms, wt in summary[:8]:
    print(f"{e:<45}{ms:>12.1f}{wt:>12.0f}")

with open("baseline_v2_results.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)
with open("baseline_v2_waits.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["event", "median_time_ms", "median_waits"])
    w.writerows(summary)
print("Saved baseline_v2_results.csv and baseline_v2_waits.csv")
