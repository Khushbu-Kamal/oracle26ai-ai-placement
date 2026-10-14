import json
import oracledb

USER, DSN = "ai_lab", "localhost:1521/FREEPDB1"
CUT = "TIMESTAMP '2026-02-01 00:00:00'"
JOIN = ("SELECT c.customer_name, o.order_id, o.order_date, o.status, "
        "o.total_amount FROM customers c LEFT JOIN orders o "
        "ON o.customer_id = c.customer_id WHERE c.customer_id = :1 "
        "ORDER BY o.order_id")


def _common(password):
    oracledb.defaults.fetch_lobs = False
    ops = json.load(open("duality_workload.json"))
    conn = oracledb.connect(user=USER, password=password, dsn=DSN)
    cur = conn.cursor()
    cur.execute(f"SELECT customer_id, order_id FROM orders WHERE order_date < {CUT}")
    seed = dict(cur.fetchall())
    return ops, conn, cur, seed


def dual_db_setup(password, worker_id, workers):
    ops, conn, cur, seed = _common(password)
    n = len(ops)

    def op(k):
        o = ops[(worker_id + k * workers) % n]
        if o["op"] == "read":
            cur.execute("SELECT json_serialize(v.data RETURNING CLOB) "
                        "FROM customer_orders_dv v "
                        "WHERE json_value(v.data, '$._id') = :1",
                        (o["customer_id"],))
            row = cur.fetchone()
            if row is None or not row[0]:
                raise RuntimeError("no document")
        else:
            try:
                cur.execute("UPDATE order_dv v SET v.data = "
                            "json_transform(v.data, SET '$.status' = :1) "
                            "WHERE json_value(v.data, '$._id') = :2",
                            ("U%d-%d" % (worker_id, k), seed[o["customer_id"]]))
                if cur.rowcount != 1:
                    raise RuntimeError("update changed %d rows" % cur.rowcount)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
    return op


def dual_ext_setup(password, worker_id, workers):
    ops, conn, cur, seed = _common(password)
    n = len(ops)

    def op(k):
        o = ops[(worker_id + k * workers) % n]
        if o["op"] == "read":
            cur.execute(JOIN, (o["customer_id"],))
            rows = cur.fetchall()
            if not rows:
                raise RuntimeError("no customer")
            doc = {"_id": o["customer_id"], "customerName": rows[0][0],
                   "orders": [{"orderId": int(r[1]),
                               "orderDate": r[2].isoformat(),
                               "status": r[3], "totalAmount": float(r[4])}
                              for r in rows if r[1] is not None]}
            _ = json.dumps(doc)
        else:
            try:
                oid = seed[o["customer_id"]]
                cur.execute("SELECT status, total_amount FROM orders "
                            "WHERE order_id = :1", (oid,))
                row = cur.fetchone()
                if row is None:
                    raise RuntimeError("no order")
                doc = {"_id": oid, "status": row[0],
                       "totalAmount": float(row[1])}
                doc["status"] = "U%d-%d" % (worker_id, k)
                cur.execute("UPDATE orders SET status = :1 "
                            "WHERE order_id = :2", (doc["status"], oid))
                if cur.rowcount != 1:
                    raise RuntimeError("update changed %d rows" % cur.rowcount)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
    return op
