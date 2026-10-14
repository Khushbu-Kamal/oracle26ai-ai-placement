import json
import oracledb

USER, DSN = "ai_lab", "localhost:1521/FREEPDB1"
MODEL_DB = "ai_user.MINILM_L12"
MODEL_HF = "sentence-transformers/all-MiniLM-L12-v2"
DIMS = 384


def inf_db_setup(password, worker_id, workers):
    sentences = json.load(open("inference_inputs.json"))
    n = len(sentences)
    conn = oracledb.connect(user=USER, password=password, dsn=DSN)
    cur = conn.cursor()
    sql = f"SELECT VECTOR_EMBEDDING({MODEL_DB} USING :1 AS data) FROM dual"

    def op(k):
        cur.execute(sql, (sentences[(worker_id + k * workers) % n],))
        row = cur.fetchone()
        if row is None or len(row[0]) != DIMS:
            raise RuntimeError("bad embedding")
    return op


def inf_ext_setup(password, worker_id, workers):
    import torch
    torch.set_num_threads(1)
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer(MODEL_HF, device="cpu")
    sentences = json.load(open("inference_inputs.json"))
    n = len(sentences)
    for s in sentences[:3]:
        model.encode(s)

    def op(k):
        v = model.encode(sentences[(worker_id + k * workers) % n],
                         convert_to_numpy=True)
        if v.shape[0] != DIMS:
            raise RuntimeError("bad embedding")
        _ = v.tolist()
    return op
