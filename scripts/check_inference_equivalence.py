import getpass, json
import numpy as np
import oracledb
import torch
from sentence_transformers import SentenceTransformer

torch.set_num_threads(1)
sentences = json.load(open("inference_inputs.json"))
password = getpass.getpass("ai_lab password: ")
conn = oracledb.connect(user="ai_lab", password=password,
                        dsn="localhost:1521/FREEPDB1")
cur = conn.cursor()
sql = "SELECT VECTOR_EMBEDDING(ai_user.MINILM_L12 USING :1 AS data) FROM dual"

db = []
for s in sentences:
    cur.execute(sql, (s,))
    db.append(np.array(cur.fetchone()[0], dtype=np.float32))
db = np.vstack(db)

model = SentenceTransformer("sentence-transformers/all-MiniLM-L12-v2", device="cpu")
ext = model.encode(sentences, convert_to_numpy=True, batch_size=32).astype(np.float32)

cos = (db * ext).sum(1) / (np.linalg.norm(db, axis=1) * np.linalg.norm(ext, axis=1))
diff = np.abs(db - ext)
print(f"Sentences compared:             {len(sentences)} ({len(set(sentences))} distinct)")
print(f"Dimensions (Oracle / Python):   {db.shape[1]} / {ext.shape[1]}")
print(f"Minimum cosine similarity:      {cos.min():.8f}")
print(f"Mean cosine similarity:         {cos.mean():.8f}")
print(f"Largest absolute difference:    {diff.max():.2e}")
print(f"Mean absolute difference:       {diff.mean():.2e}")
