import getpass, json
import oracledb

oracledb.defaults.fetch_lobs = False
CUT = "TIMESTAMP '2026-02-01 00:00:00'"
password = getpass.getpass("ai_lab password: ")
conn = oracledb.connect(user="ai_lab", password=password,
                        dsn="localhost:1521/FREEPDB1")
cur = conn.cursor()

# 1. Reset to the seed-only state
cur.execute(f"DELETE FROM order_items WHERE order_id IN "
            f"(SELECT order_id FROM orders WHERE order_date >= {CUT})")
cur.execute(f"DELETE FROM orders WHERE order_date >= {CUT}")
cur.execute(f"UPDATE orders SET status = 'SEED' "
            f"WHERE order_date < {CUT} AND status <> 'SEED'")
conn.commit()
cur.execute(f"SELECT COUNT(*) FROM orders WHERE order_date < {CUT}")
print("Seed orders (expect 500):", cur.fetchone()[0])

# 2. Index on orders(customer_id)
try:
    cur.execute("CREATE INDEX orders_customer_ix ON orders(customer_id)")
    print("Created index orders_customer_ix")
except oracledb.DatabaseError as e:
    print("Index step skipped:", str(e).splitlines()[0])

# 3. Order-level duality view for updates
cur.execute("""CREATE OR REPLACE JSON RELATIONAL DUALITY VIEW order_dv AS
SELECT JSON {'_id': o.order_id, 'status': o.status,
             'totalAmount': o.total_amount}
FROM orders o WITH UPDATE""")
print("Created order_dv")

cur.execute(f"SELECT customer_id, order_id FROM orders WHERE order_date < {CUT}")
seed = dict(cur.fetchall())

JOIN = ("SELECT c.customer_name, o.order_id, o.order_date, o.status, "
        "o.total_amount FROM customers c LEFT JOIN orders o "
        "ON o.customer_id = c.customer_id WHERE c.customer_id = :1 "
        "ORDER BY o.order_id")


def ext_doc(cid):
    cur.execute(JOIN, (cid,))
    rows = cur.fetchall()
    return {"_id": cid, "customerName": rows[0][0],
            "orders": [{"orderId": int(r[1]), "orderDate": r[2].isoformat(),
                        "status": r[3], "totalAmount": float(r[4])}
                       for r in rows if r[1] is not None]}


def db_doc(cid):
    cur.execute("SELECT json_serialize(v.data RETURNING CLOB) "
                "FROM customer_orders_dv v "
                "WHERE json_value(v.data, '$._id') = :1", (cid,))
    return json.loads(cur.fetchone()[0])


def norm(d):
    return (d["customerName"],
            [(o["orderId"], str(o["orderDate"])[:19], o["status"],
              float(o["totalAmount"]))
             for o in sorted(d["orders"], key=lambda o: o["orderId"])])


# 4. Read equivalence: 500 customers with one order, 100 with none
ids = list(range(1, 501)) + list(range(9901, 10001))
bad = [i for i in ids if norm(db_doc(i)) != norm(ext_doc(i))]
print(f"Read equivalence: {len(ids) - len(bad)} of {len(ids)} identical")
if bad:
    print("First mismatch, customer", bad[0])
    print("  duality view:", norm(db_doc(bad[0])))
    print("  external:    ", norm(ext_doc(bad[0])))

# 5. Update checks, customer 1
oid = seed[1]
cur.execute("UPDATE order_dv v SET v.data = json_transform(v.data, "
            "SET '$.status' = :1) WHERE json_value(v.data, '$._id') = :2",
            ("PACKED", oid))
print("In-DB update rows changed (expect 1):", cur.rowcount)
conn.commit()
cur.execute("SELECT status FROM orders WHERE order_id = :1", (oid,))
print("Table status after view update (expect PACKED):", cur.fetchone()[0],
      "| external read sees:", ext_doc(1)["orders"][0]["status"])
cur.execute("UPDATE orders SET status = 'SHIPPED' WHERE order_id = :1", (oid,))
conn.commit()
print("View read after table update (expect SHIPPED):",
      db_doc(1)["orders"][0]["status"])
cur.execute("UPDATE orders SET status = 'SEED' WHERE order_id = :1", (oid,))
conn.commit()
print("Restored seed order.")
