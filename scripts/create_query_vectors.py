import numpy as np

corpus = np.load("ai_vectors.npy")
rng = np.random.default_rng(789)

idx = rng.choice(len(corpus), size=1000, replace=False)
queries = corpus[idx] + rng.normal(0, 0.05, size=(1000, corpus.shape[1]))
queries = queries.astype(np.float32)

np.save("query_vectors.npy", queries)
print("Saved query_vectors.npy:", queries.shape, queries.dtype)
