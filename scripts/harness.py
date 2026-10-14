import os
for _v in ("VECLIB_MAXIMUM_THREADS", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS"):
    os.environ.setdefault(_v, "1")

import argparse, array, csv, getpass, multiprocessing as mp, random, re
import statistics, subprocess, sys, threading, time
import numpy as np
import oracledb
from dual_ops import dual_db_setup, dual_ext_setup
from inference_ops import inf_db_setup, inf_ext_setup

USER, DSN, CONTAINER = "ai_lab", "localhost:1521/FREEPDB1", "oracle-db"
NAN = float("nan")
UNITS = {"B": 1, "KB": 1e3, "MB": 1e6, "GB": 1e9,
         "KIB": 1024, "MIB": 1024**2, "GIB": 1024**3}

SNAP_SQL = """ALTER SESSION SET CONTAINER = FREEPDB1;
SET PAGESIZE 0 FEEDBACK OFF HEADING OFF LINESIZE 300 TRIMSPOOL ON
SELECT 'EV|'||event||'|'||total_waits||'|'||time_waited_micro FROM v$system_event WHERE wait_class <> 'Idle';
SELECT 'ST|'||name||'|'||value FROM v$sysstat WHERE name IN ('redo size','user commits');
EXIT
"""


# ------------------------------------------------------------------ workers
def oltp_setup(password, seed):
    conn = oracledb.connect(user=USER, password=password, dsn=DSN)
    cur = conn.cursor()
    rng = random.Random(seed)

    def op(k):
        customer_id = rng.randint(1, 10000)
        product_ids = rng.sample(range(1, 1001), 3)
        try:
            cur.execute("SELECT customer_id, customer_name FROM customers "
                        "WHERE customer_id = :1", (customer_id,))
            if cur.fetchone() is None:
                raise RuntimeError("customer not found")
            products = []
            for pid in product_ids:
                cur.execute("SELECT product_id, price FROM products "
                            "WHERE product_id = :1", (pid,))
                row = cur.fetchone()
                if row is None:
                    raise RuntimeError("product not found")
                products.append(row)
            quantities = [rng.randint(1, 3) for _ in products]
            total = sum(p[1] * q for p, q in zip(products, quantities))
            oid = cur.var(int)
            cur.execute("INSERT INTO orders (customer_id, order_date, status, "
                        "total_amount) VALUES (:1, SYSTIMESTAMP, 'NEW', :2) "
                        "RETURNING order_id INTO :3", (customer_id, total, oid))
            order_id = oid.getvalue()[0]
            for p, q in zip(products, quantities):
                cur.execute("INSERT INTO order_items (order_id, product_id, "
                            "quantity, unit_price) VALUES (:1, :2, :3, :4)",
                            (order_id, p[0], q, p[1]))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return op


def vec_db_setup(password, worker_id, workers):
    queries = np.load("query_vectors.npy")
    n = len(queries)
    conn = oracledb.connect(user=USER, password=password, dsn=DSN)
    cur = conn.cursor()

    def op(k):
        q = array.array("f", queries[(worker_id + k * workers) % n])
        cur.execute("SELECT vector_id FROM ai_vectors ORDER BY "
                    "VECTOR_DISTANCE(embedding, :1, COSINE) "
                    "FETCH FIRST 10 ROWS ONLY", (q,))
        if len(cur.fetchall()) != 10:
            raise RuntimeError("expected 10 rows")
    return op


def vec_ext_setup(worker_id, workers):
    corpus = np.load("ai_vectors.npy").astype(np.float32)
    corpus /= np.maximum(np.linalg.norm(corpus, axis=1, keepdims=True), 1e-12)
    queries = np.load("query_vectors.npy").astype(np.float32)
    queries /= np.maximum(np.linalg.norm(queries, axis=1, keepdims=True), 1e-12)
    n = len(queries)

    def op(k):
        scores = corpus @ queries[(worker_id + k * workers) % n]
        idx = np.argpartition(scores, -10)[-10:]
        idx = idx[np.argsort(-scores[idx])]
        _ = (idx + 1).tolist()
    return op


def worker(kind, worker_id, workers, rate, password, seed,
           start, warm_end, end, q):
    try:
        if kind == "oltp":
            op = oltp_setup(password, seed + worker_id)
        elif kind == "vec_db":
            op = vec_db_setup(password, worker_id, workers)
        elif kind == "dual_db":
            op = dual_db_setup(password, worker_id, workers)
        elif kind == "dual_ext":
            op = dual_ext_setup(password, worker_id, workers)
        elif kind == "inf_db":
            op = inf_db_setup(password, worker_id, workers)
        elif kind == "inf_ext":
            op = inf_ext_setup(password, worker_id, workers)
        else:
            op = vec_ext_setup(worker_id, workers)

        interval = workers / rate if rate > 0 else 0.0
        offset = (worker_id / workers) * interval
        rows, k, cpu_start = [], 0, None

        d = start - time.time()
        if d > 0:
            time.sleep(d)
        while True:
            if interval:
                t_sched = start + offset + k * interval
                if t_sched >= end:
                    break
                d = t_sched - time.time()
                if d > 0:
                    time.sleep(d)
            now = time.time()
            if now >= end:
                break
            if cpu_start is None and now >= warm_end:
                cpu_start = time.process_time()
            t0 = time.perf_counter()
            try:
                op(k)
                ok = True
            except Exception as e:
                ok = False
                with open("errors.log", "a") as ef:
                    ef.write(kind + ": " + str(e).splitlines()[0] + "\n")
            lat = time.perf_counter() - t0
            if now >= warm_end:
                rows.append((now, lat, ok))
            k += 1
        cpu_s = (time.process_time() - cpu_start) if cpu_start else 0.0
        q.put((kind, worker_id, rows, cpu_s, None))
    except Exception as e:
        q.put((kind, worker_id, [], 0.0, repr(e)))


# ------------------------------------------------------------------- parent
def reset(password):
    with oracledb.connect(user=USER, password=password, dsn=DSN) as c:
        cur = c.cursor()
        cut = "TIMESTAMP '2026-02-01 00:00:00'"
        cur.execute("DELETE FROM order_items WHERE order_id IN "
                    "(SELECT order_id FROM orders WHERE order_date >= " + cut + ")")
        cur.execute("DELETE FROM orders WHERE order_date >= " + cut)
        cur.execute("UPDATE orders SET status = 'SEED' WHERE order_date < "
                    + cut + " AND status <> 'SEED'")
        c.commit()


def cpu_usec():
    out = subprocess.run(["podman", "exec", CONTAINER, "cat",
                          "/sys/fs/cgroup/cpu.stat"],
                         capture_output=True, text=True).stdout
    m = re.search(r"usage_usec\s+(\d+)", out)
    return int(m.group(1)) if m else None


def snapshot():
    out = subprocess.run(["podman", "exec", "-i", CONTAINER, "sqlplus", "-S",
                          "/", "as", "sysdba"], input=SNAP_SQL,
                         capture_output=True, text=True).stdout
    events, stats = {}, {}
    for line in out.splitlines():
        if line.startswith("EV|"):
            ev, w, t = line[3:].rsplit("|", 2)
            events[ev] = (int(w), int(t))
        elif line.startswith("ST|"):
            name, v = line[3:].rsplit("|", 1)
            stats[name] = float(v)
    return events, stats


def to_mb(s):
    m = re.match(r"([\d.]+)\s*([A-Za-z]+)", s.strip())
    return float(m.group(1)) * UNITS[m.group(2).upper()] / 1024**2


def mem_sampler(stop, samples):
    while not stop.is_set():
        out = subprocess.run(
            ["podman", "stats", "--no-stream", "--format", "{{.MemUsage}}",
             CONTAINER], capture_output=True, text=True).stdout.strip()
        try:
            samples.append(to_mb(out.split("/")[0]))
        except Exception:
            pass


def sleep_until(t):
    d = t - time.time()
    if d > 0:
        time.sleep(d)


def one_run(args, password):
    reset(password)
    start = time.time() + args.setup_wait
    warm_end = start + args.warmup
    end = warm_end + args.duration

    plan = []
    if args.oltp_rate > 0:
        plan += [("oltp", w, args.oltp_workers, args.oltp_rate)
                 for w in range(args.oltp_workers)]
    if args.vec_mode != "none":
        plan += [("vec_" + args.vec_mode, w, args.vec_workers, args.vec_rate)
                 for w in range(args.vec_workers)]
    if args.dual_mode != "none":
        plan += [("dual_" + args.dual_mode, w, args.dual_workers, args.dual_rate)
                 for w in range(args.dual_workers)]
    if args.inf_mode != "none":
        plan += [("inf_" + args.inf_mode, w, args.inf_workers, args.inf_rate)
                 for w in range(args.inf_workers)]

    q = mp.Queue()
    procs = []
    for kind, w, n, rate in plan:
        p = mp.Process(target=worker, args=(kind, w, n, rate, password,
                                            args.seed, start, warm_end, end, q))
        p.start()
        procs.append(p)

    sleep_until(warm_end)
    ev0, st0 = snapshot()
    c0, w0 = cpu_usec(), time.time()
    stop, mem = threading.Event(), []
    t = threading.Thread(target=mem_sampler, args=(stop, mem))
    t.start()
    sleep_until(end)
    ev1, st1 = snapshot()
    c1, w1 = cpu_usec(), time.time()
    stop.set()
    t.join()

    collected, cpu_by_kind = {}, {}
    for _ in plan:
        kind, w, rows, cpu_s, err = q.get(timeout=args.duration + 120)
        if err:
            print(f"  worker {kind}/{w} error: {err}")
        collected.setdefault(kind, []).extend(rows)
        cpu_by_kind[kind] = cpu_by_kind.get(kind, 0.0) + cpu_s
    for p in procs:
        p.join()

    out = {}
    for kind, rows in collected.items():
        ok = [lat for _, lat, good in rows if good]
        fail = sum(1 for _, _, good in rows if not good)
        a = np.array(ok) * 1000 if ok else np.array([NAN])
        p50, p95, p99 = np.percentile(a, [50, 95, 99])
        n_ai = sum(1 for k in collected if k != "oltp")
        pref = "oltp" if kind == "oltp" else ("ai" if n_ai == 1 else kind.split("_")[0])
        out.update({f"{pref}_ok": len(ok), f"{pref}_fail": fail,
                    f"{pref}_rate": len(ok) / args.duration,
                    f"{pref}_avg_ms": float(a.mean()), f"{pref}_p50_ms": p50,
                    f"{pref}_p95_ms": p95, f"{pref}_p99_ms": p99,
                    f"{pref}_client_cpu_pct": cpu_by_kind[kind] / args.duration * 100})
    out["db_cpu_pct"] = ((c1 - c0) / ((w1 - w0) * 1e6) * 100
                         if c0 is not None and c1 is not None else NAN)
    out["db_mem_mb"] = max(mem) if mem else NAN
    out["redo_mb"] = (st1.get("redo size", NAN) - st0.get("redo size", NAN)) / 1024**2
    out["commits"] = st1.get("user commits", NAN) - st0.get("user commits", NAN)
    waits = {e: ((ev1[e][1] - ev0.get(e, (0, 0))[1]) / 1000.0,
                 ev1[e][0] - ev0.get(e, (0, 0))[0]) for e in ev1}
    return out, waits


COLS = ["oltp_ok", "oltp_fail", "oltp_rate", "oltp_avg_ms", "oltp_p50_ms",
        "oltp_p95_ms", "oltp_p99_ms", "oltp_client_cpu_pct",
        "ai_ok", "ai_fail", "ai_rate", "ai_avg_ms", "ai_p50_ms", "ai_p95_ms",
        "ai_p99_ms", "ai_client_cpu_pct",
        "db_cpu_pct", "db_mem_mb", "redo_mb", "commits"]
for _f in ("vec", "dual", "inf"):
    for _m in ("ok", "fail", "rate", "avg_ms", "p95_ms", "p99_ms", "client_cpu_pct"):
        COLS.append(f"{_f}_{_m}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--out", default="phase3")
    ap.add_argument("--oltp-rate", type=float, default=700, help="0 = no OLTP")
    ap.add_argument("--oltp-workers", type=int, default=10)
    ap.add_argument("--vec-mode", choices=["none", "db", "ext"], default="none")
    ap.add_argument("--vec-rate", type=float, default=0, help="0 = unthrottled")
    ap.add_argument("--vec-workers", type=int, default=1)
    ap.add_argument("--dual-mode", choices=["none", "db", "ext"], default="none")
    ap.add_argument("--dual-rate", type=float, default=0, help="0 = unthrottled")
    ap.add_argument("--dual-workers", type=int, default=1)
    ap.add_argument("--inf-mode", choices=["none", "db", "ext"], default="none")
    ap.add_argument("--inf-rate", type=float, default=0, help="0 = unthrottled")
    ap.add_argument("--inf-workers", type=int, default=1)
    ap.add_argument("--duration", type=int, default=60)
    ap.add_argument("--warmup", type=int, default=10)
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--setup-wait", type=int, default=20)
    args = ap.parse_args()

    password = getpass.getpass("ai_lab password: ")
    print(f"Scenario {args.label}: OLTP {args.oltp_rate} TPS, vector {args.vec_mode} "
          f"rate {args.vec_rate} x {args.vec_workers} workers, "
          f"{args.warmup}s warm-up + {args.duration}s measured, {args.runs} runs")

    rows, all_waits = [], []
    for i in range(1, args.runs + 1):
        for attempt in range(1, 4):
            print(f"Run {i}/{args.runs} (attempt {attempt})...")
            r, waits = one_run(args, password)
            if args.oltp_rate > 0 and r.get("oltp_ok", 0) == 0:
                print("  INVALID: no data collected (stalled run); "
                      "discarding and retrying")
                continue
            if (args.vec_mode != "none" or args.dual_mode != "none"
                    or args.inf_mode != "none" or args.oltp_rate <= 0):
                break
            if r.get("oltp_rate", 0) >= 0.98 * args.oltp_rate:
                break
            print(f"  INVALID: OLTP achieved {r.get('oltp_rate', 0):.1f} "
                  f"of {args.oltp_rate} TPS; discarding and retrying")
        r["label"], r["run"] = args.label, i
        rows.append(r)
        all_waits.append(waits)
        print("  " + "  ".join(f"{k}={r.get(k, NAN):.1f}" for k in
              ("oltp_rate", "oltp_p99_ms", "ai_rate", "ai_p99_ms", "db_cpu_pct")))
    reset(password)

    new = not os.path.exists(args.out + "_results.csv")
    with open(args.out + "_results.csv", "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["label", "run"] + COLS, extrasaction="ignore")
        if new:
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, NAN) for k in ["label", "run"] + COLS})

    print()
    print(f"SUMMARY for {args.label} (median, min, max, variability)")
    for c in COLS:
        v = [r[c] for r in rows if c in r and r[c] == r[c]]
        if not v:
            continue
        m = statistics.mean(v)
        cv = statistics.pstdev(v) / m * 100 if m else 0
        print(f"{c:>20}: median {statistics.median(v):10.2f}  min {min(v):10.2f}  "
              f"max {max(v):10.2f}  var {cv:5.1f}%")

    events = set().union(*[set(w) for w in all_waits])
    summ = sorted(((e, statistics.median([w.get(e, (0, 0))[0] for w in all_waits]),
                    statistics.median([w.get(e, (0, 0))[1] for w in all_waits]))
                   for e in events), key=lambda x: -x[1])
    print()
    print("TOP WAIT EVENTS (median per run)")
    for e, ms, n in summ[:6]:
        print(f"{e:<45}{ms:>12.1f}{n:>12.0f}")
    new = not os.path.exists(args.out + "_waits.csv")
    with open(args.out + "_waits.csv", "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["label", "event", "median_time_ms", "median_waits"])
        for e, ms, n in summ[:15]:
            w.writerow([args.label, e, ms, n])


if __name__ == "__main__":
    main()
