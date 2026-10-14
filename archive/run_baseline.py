import csv, getpass, re, statistics, subprocess, sys, threading
import oracledb

CONC, TXNS, WARMUP_TXNS, RUNS, SEED = 10, 500, 100, 5, 42
CONTAINER = "oracle-db"
UNITS = {"B": 1, "KB": 1e3, "MB": 1e6, "GB": 1e9,
         "KIB": 1024, "MIB": 1024**2, "GIB": 1024**3}

password = getpass.getpass("ai_lab password: ")


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


def sampler(stop, samples):
    while not stop.is_set():
        out = subprocess.run(
            ["podman", "stats", "--no-stream", "--format",
             "{{.CPUPerc}}|{{.MemUsage}}", CONTAINER],
            capture_output=True, text=True).stdout.strip()
        try:
            cpu, mem = out.split("|")
            samples.append((float(cpu.strip().rstrip("%")),
                            to_mb(mem.split("/")[0])))
        except Exception:
            pass


def run_once(txns):
    reset()
    stop, samples = threading.Event(), []
    t = threading.Thread(target=sampler, args=(stop, samples))
    t.start()
    out = subprocess.run(
        [sys.executable, "benchmark_oltp.py", "--concurrency", str(CONC),
         "--transactions", str(txns), "--seed", str(SEED)],
        capture_output=True, text=True).stdout
    stop.set()
    t.join()

    def g(pattern):
        m = re.search(pattern, out)
        return float(m.group(1)) if m else float("nan")

    return {
        "success": g(r"Successful transactions:\s+(\d+)"),
        "failed": g(r"Failed transactions:\s+(\d+)"),
        "duration_s": g(r"Duration:\s+([\d.]+) sec"),
        "tps": g(r"Throughput:\s+([\d.]+) TPS"),
        "avg_ms": g(r"Average:\s+([\d.]+) ms"),
        "p95_ms": g(r"p95:\s+([\d.]+) ms"),
        "p99_ms": g(r"p99:\s+([\d.]+) ms"),
        "db_cpu_pct": (statistics.mean(s[0] for s in samples)
                       if samples else float("nan")),
        "db_mem_mb": (max(s[1] for s in samples)
                      if samples else float("nan")),
        "cpu_samples": len(samples),
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
cols = ["run", "success", "failed", "duration_s", "tps", "avg_ms",
        "p95_ms", "p99_ms", "db_cpu_pct", "db_mem_mb", "cpu_samples"]

print()
print("PER-RUN RESULTS")
print("  ".join(f"{c:>10}" for c in cols))
for r in rows:
    print("  ".join(f"{r[c]:>10.2f}" for c in cols))

print()
print("SUMMARY (median, min, max, variability = stdev / mean)")
for c in ["tps", "avg_ms", "p95_ms", "p99_ms", "db_cpu_pct", "db_mem_mb"]:
    v = [r[c] for r in rows if r[c] == r[c]]
    if not v:
        print(f"{c:>10}: no data")
        continue
    cv = statistics.pstdev(v) / statistics.mean(v) * 100
    print(f"{c:>10}: median {statistics.median(v):9.2f}  "
          f"min {min(v):9.2f}  max {max(v):9.2f}  variability {cv:5.1f}%")
print(f"total failed transactions across runs: {sum(r['failed'] for r in rows):.0f}")

with open("baseline_results.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=cols)
    w.writeheader()
    w.writerows(rows)
print("Saved baseline_results.csv")
