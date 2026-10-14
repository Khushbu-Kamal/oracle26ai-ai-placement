import array
import getpass
import time
import numpy as np
import oracledb

matrix = np.load("ai_vectors.npy")
n, dims = matrix.shape
print(f"Loaded file: {n:,} vectors, {dims} dimensions")

password = getpass.getpass("ai_lab password: ")

with oracledb.connect(user="ai_lab", password=password,
                      dsn="localhost:1521/FREEPDB1") as conn:
    cur = conn.cursor()
    cur.execute(f"""CREATE TABLE ai_vectors (
        vector_id NUMBER PRIMARY KEY,
        document_id NUMBER NOT NULL,
        embedding VECTOR({dims}, FLOAT32) NOT NULL,
        category VARCHAR2(20) NOT NULL)""")

    sql = "INSERT INTO ai_vectors VALUES (:1, :2, :3, :4)"
    start = time.perf_counter()
    rows = []
    for i in range(1, n + 1):
        rows.append((i, i, array.array("f", matrix[i - 1]),
                     f"category_{(i % 10) + 1}"))
        if len(rows) == 1000:
            cur.executemany(sql, rows)
            conn.commit()
            rows = []
            if i % 10000 == 0:
                print(f"Inserted {i:,} / {n:,}")
    if rows:
        cur.executemany(sql, rows)
        conn.commit()
    print(f"Done in {time.perf_counter() - start:.1f} seconds")
