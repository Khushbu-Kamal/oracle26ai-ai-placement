import array, getpass, json, os, subprocess, sys, time
import numpy as np
import oracledb
from dual_ops import JOIN, CUT

oracledb.defaults.fetch_lobs = False
USER, DSN = "ai_lab", "localhost:1521/FREEPDB1"
password = getpass.getpass("ai_lab password: ")
os.environ["AI_LAB_PW"] = password

conn = oracledb.connect(user=USER, password=password, dsn=DSN)
cur = conn.cursor()
NAMES = ("bytes sent via SQL*Net to client",
         "bytes received via SQL*Net from client",
         "SQL*Net roundtrips to/from client")


def stats():
    cur.execute("SELECT n.name, s.value FROM v$mystat s JOIN v$statname n "
                "ON s.statistic# = n.statistic# WHERE n.name IN (:1, :2, :3)",
                NAMES)
    d = dict(cur.fetchall())
    return [d[n] for n in NAMES]


a = stats()
b = stats()
ov = [y - x for x, y in zip(a, b)]          # cost of one stats() call
results = []


def measure(label, n, fn):
    s1 = stats()
    for k in range(n):
        fn(k)
    s2 = stats()
    d = [(y - x - o) / n for x, y, o in zip(s1, s2, ov)]
    results.append((label, n, d[1], d[0], d[2]))


queries = np.load("query_vectors.npy")
sentences = json.load(open("inference_inputs.json"))
ops = json.load(open("duality_workload.json"))
reads = [o for o in ops if o["op"] == "read"][:200]
updates = [o for o in ops if o["op"] == "update"][:100]
cur.execute(f"SELECT customer_id, order_id FROM orders WHERE order_date < {CUT}")
seed = dict(cur.fetchall())


def vec_db(k):
    cur.execute("SELECT vector_id FROM ai_vectors ORDER BY VECTOR_DISTANCE("
                "embedding, :1, COSINE) FETCH FIRST 10 ROWS ONLY",
                (array.array("f", queries[k % len(queries)]),))
    cur.fetchall()


def dual_db_read(k):
    cur.execute("SELECT json_serialize(v.data RETURNING CLOB) FROM "
                "customer_orders_dv v WHERE json_value(v.data, '$._id') = :1",
                (reads[k]["customer_id"],))
    cur.fetchone()


def dual_db_update(k):
    cur.execute("UPDATE order_dv v SET v.data = json_transform(v.data, "
                "SET '$.status' = :1) WHERE json_value(v.data, '$._id') = :2",
                ("N%d" % k, seed[updates[k]["customer_id"]]))
    conn.commit()


def dual_ext_read(k):
    cur.execute(JOIN, (reads[k]["customer_id"],))
    cur.fetchall()


def dual_ext_update(k):
    oid = seed[updates[k]["customer_id"]]
    cur.execute("SELECT status, total_amount FROM orders WHERE order_id = :1",
                (oid,))
    cur.fetchone()
    cur.execute("UPDATE orders SET status = :1 WHERE order_id = :2",
                ("X%d" % k, oid))
    conn.commit()


def inf_db(k):
    cur.execute("SELECT VECTOR_EMBEDDING(ai_user.MINILM_L12 USING :1 AS data) "
                "FROM dual", (sentences[k % len(sentences)],))
    cur.fetchone()


measure("Vector search, in Oracle (per search)", 100, vec_db)
measure("Duality read, in Oracle (per read)", 200, dual_db_read)
measure("Duality read, external (per read)", 200, dual_ext_read)
measure("Duality update, in Oracle (per update)", 100, dual_db_update)
measure("Duality update, external (per update)", 100, dual_ext_update)
measure("Inference, in Oracle (per embedding)", 100, inf_db)
cur.execute(f"UPDATE orders SET status = 'SEED' WHERE order_date < {CUT}")
conn.commit()

print()
print("NETWORK TRAFFIC PER OPERATION (measured from the database session)")
print(f"{'workload':<42}{'ops':>5}{'to Oracle (B)':>15}{'from Oracle (B)':>17}{'round trips':>13}")
for label, n, rx, tx, rt in results:
    print(f"{label:<42}{n:>5}{rx:>15.0f}{tx:>17.0f}{rt:>13.2f}")
print("External vector search and external inference send nothing to Oracle per operation.")

cur.arraysize = 1000
s1 = stats()
t0 = time.perf_counter()
cur.execute("SELECT vector_id, embedding FROM ai_vectors ORDER BY vector_id")
vecs = []
while True:
    rows = cur.fetchmany(1000)
    if not rows:
        break
    vecs.extend(np.asarray(r[1], dtype=np.float32) for r in rows)
elapsed = time.perf_counter() - t0
s2 = stats()
print()
print("ONE-TIME COPY OF THE VECTOR CORPUS (Oracle to NumPy)")
print(f"Vectors copied:            {len(vecs):,}")
print(f"Data received from Oracle: {(s2[0] - s1[0]) / 1024**2:.1f} MB")
print(f"Round trips:               {s2[2] - s1[2] - ov[2]:.0f}")
print(f"Time:                      {elapsed:.1f} s")
conn.close()

PREFIX = "import resource\n"
SUFFIX = "\nprint(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)"
CODE = {
 "Python + numpy + oracledb only (baseline)":
   "import numpy, oracledb",
 "Vector search, external (NumPy)":
   "from harness import vec_ext_setup\nop = vec_ext_setup(0, 1)\n"
   "for k in range(200): op(k)",
 "Duality, external (Python)":
   "import os\nfrom dual_ops import dual_ext_setup\n"
   "op = dual_ext_setup(os.environ['AI_LAB_PW'], 0, 1)\n"
   "for k in range(300): op(k)",
 "Inference, external (sentence-transformers)":
   "import os\nfrom inference_ops import inf_ext_setup\n"
   "op = inf_ext_setup(os.environ['AI_LAB_PW'], 0, 1)\n"
   "for k in range(100): op(k)",
}
print()
print("PEAK MEMORY OF ONE EXTERNAL WORKER PROCESS")
for label, code in CODE.items():
    out = subprocess.run([sys.executable, "-c", PREFIX + code + SUFFIX],
                         capture_output=True, text=True)
    try:
        rss = float(out.stdout.strip().splitlines()[-1])
        mb = rss / 1024**2 if sys.platform == "darwin" else rss / 1024
        print(f"{label:<48}{mb:>8.0f} MB")
    except Exception:
        print(f"{label:<48}   failed: {out.stderr.strip().splitlines()[-1:]}")

with oracledb.connect(user=USER, password=password, dsn=DSN) as c:
    c.cursor().execute(f"UPDATE orders SET status = 'SEED' WHERE order_date < {CUT}")
    c.commit()
