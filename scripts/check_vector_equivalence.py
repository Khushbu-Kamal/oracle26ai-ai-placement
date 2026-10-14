import array, getpass
import numpy as np
import oracledb

corpus = np.load("ai_vectors.npy").astype(np.float32)
corpus /= np.maximum(np.linalg.norm(corpus, axis=1, keepdims=True), 1e-12)
queries = np.load("query_vectors.npy").astype(np.float32)
qn = queries / np.maximum(np.linalg.norm(queries, axis=1, keepdims=True), 1e-12)

password = getpass.getpass("ai_lab password: ")
conn = oracledb.connect(user="ai_lab", password=password,
                        dsn="localhost:1521/FREEPDB1")
cur = conn.cursor()

overlaps = []
for i in range(len(queries)):
    idx = np.argpartition(corpus @ qn[i], -10)[-10:]
    ext = set((idx + 1).tolist())
    cur.execute("SELECT vector_id FROM ai_vectors ORDER BY "
                "VECTOR_DISTANCE(embedding, :1, COSINE) FETCH FIRST 10 ROWS ONLY",
                (array.array("f", queries[i]),))
    ora = {int(r[0]) for r in cur.fetchall()}
    overlaps.append(len(ext & ora))

o = np.array(overlaps)
print(f"Queries compared:          {len(o)}")
print(f"Identical top-10 sets:     {(o == 10).sum()} ({(o == 10).mean() * 100:.1f}%)")
print(f"Mean overlap:              {o.mean():.3f} / 10")
print(f"Worst query overlap:       {o.min()} / 10")
