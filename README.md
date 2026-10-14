# Reproduction package: in-database versus offloaded AI execution alongside OLTP in Oracle AI Database 26ai Free

Companion to the article by Khushbu Kamal (October 2026). Archived release: https://doi.org/DOI_HERE

## Contents
- `scripts/` harness, workload and check scripts
- `data/` fixed inputs. Do not regenerate: `ai_vectors.npy` is the only reproducible copy of the vector corpus (the original loader was unseeded)
- `results/` per-run results (`phase3_*`, `phase4_*`, `baseline_*`) and `errors.log`. Rows with an OLTP rate far below 700 are stalled or empty runs that were excluded in the article (one OLTP-only run, one external-inference run)
- `sql/` privileges and the customer duality view. The original database users were created in an earlier session, so `setup.sql` lists the privileges the scripts need, not the original statements
- `archive/` an earlier trial baseline script and an unused setup script (not used for reported results)

## Environment used
MacBook Pro (Mac15,3), Apple M3, 24 GiB, macOS 26.1. Podman machine on the Apple Hypervisor: 4 CPUs, 8 GiB. Oracle AI Database 26ai Free 23.26.2.0.0, image `container-registry.oracle.com/database/free:latest`, digest `sha256:696eee2ee8985af25ef0dc4cbcac14cdaadfd4545150a87d82d9724ce43c7a77`; container name `oracle-db`; service `FREEPDB1`; port 1521; cpu_count 2, SGA 1.5 GiB, PGA target 512 MiB, vector memory pool 0. Python 3.9.6, numpy 2.0.2, sentence-transformers 5.1.2, torch 2.8.0, oracledb 4.0.2.

Oracle's pre-built ONNX model `all_MiniLM_L12_v2` is not redistributed (Oracle licence). Download it from Oracle's blog post "Use our prebuilt ONNX model now available for embedding generation in Oracle Database 23ai".

## Credentials
Scripts prompt for the `ai_lab` password. `benchmark_oltp.py` run on its own needs `export AI_LAB_PASSWORD=...` (`run_baseline_v2.py` sets it). The harness collects CPU and wait events with `podman exec oracle-db` (SYSDBA by operating-system authentication), so keep the container name or edit `CONTAINER` in `harness.py`.

## Setup order (run scripts from a folder containing the files in `data/`)
1. Start the container; run `sql/setup.sql` as SYSDBA (set passwords).
2. `python3 setup_schema.py`, then `python3 restore_vectors.py`, then `python3 setup_workloads.py`.
3. Run `sql/customer_orders_dv.sql` as ai_lab, then `python3 setup_duality.py` (adds an index, creates `order_dv`, checks duality equivalence: expect 600 of 600).
4. As ai_user, load the ONNX model: `EXECUTE DBMS_VECTOR.LOAD_ONNX_MODEL('DM_DUMP', 'all_MiniLM_L12_v2.onnx', 'MINILM_L12');` (copy the file into the container's /tmp first); then run the commented grant in `setup.sql`.
5. `python3 check_vector_equivalence.py` (expect 1,000 of 1,000 identical) and `python3 check_inference_equivalence.py` (expect minimum cosine similarity about 0.9999999).

## Experiments (labels as in the article and result files)
- OLTP-only references: `python3 harness.py --label S0c_oltp_only`
- Vector: `--vec-mode db|ext --vec-rate 30|50 --vec-workers 2`
- Duality: `--dual-mode db|ext --dual-rate 500|1500 --dual-workers 4`
- Inference: `--inf-mode db|ext --inf-rate 50|100 --inf-workers 2` (external: add `--setup-wait 90`)
- Mixed: `--out phase4 --vec-mode X --vec-rate 15|30 --vec-workers 2 --dual-mode X --dual-rate 250|500 --dual-workers 4 --inf-mode X --inf-rate 25|50 --inf-workers 2` with X the same for all three
- Single-worker probes: `--oltp-rate 0 --duration 30 --warmup 5 --runs 3` with the feature flags and rate 0
- Resource accounting: `python3 measure_phase5.py`
Results go to `phase3_results.csv` (or `<out>_results.csv`).

## Licence
Code: MIT. Data files and result files: CC BY 4.0.
