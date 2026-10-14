
import getpass
import json
import random
import oracledb

# ---------- 1. Inference inputs (fixed seed) ----------
rng = random.Random(123)
subjects = ["The customer", "A warehouse manager", "Our support team",
            "The new product", "A delivery driver", "The finance report",
            "An online order", "The shipping partner"]
predicates = ["reported a delay in the shipment",
              "asked for a refund on the last invoice",
              "praised the quality of the packaging",
              "needs an updated delivery address",
              "found a pricing error in the catalogue",
              "is waiting for a replacement item",
              "requested a summary of monthly sales",
              "flagged an unusual spike in returns",
              "confirmed that payment was received",
              "wants to change the order quantity"]
sentences = [f"{rng.choice(subjects)} {rng.choice(predicates)}."
             for _ in range(1000)]
with open("inference_inputs.json", "w") as f:
    json.dump(sentences, f, indent=0)

# ---------- 2. JSON duality workload (fixed seed) ----------
rng = random.Random(456)
ops = ([{"op": "read", "customer_id": rng.randint(1, 10000)} for _ in range(800)]
       + [{"op": "update", "customer_id": rng.randint(1, 500),
           "status": rng.choice(["PACKED", "SHIPPED"])} for _ in range(200)])
rng.shuffle(ops)
with open("duality_workload.json", "w") as f:
    json.dump(ops, f, indent=0)

# ---------- 3. Select AI question set ----------
questions = [
 ("How many customers are there?", "SELECT COUNT(*) FROM customers"),
 ("How many products are there?", "SELECT COUNT(*) FROM products"),
 ("What is the average product price?", "SELECT ROUND(AVG(price), 2) FROM products"),
 ("Which five products are the most expensive?",
  "SELECT product_id, price FROM products ORDER BY price DESC FETCH FIRST 5 ROWS ONLY"),
 ("How many orders are there for each status?",
  "SELECT status, COUNT(*) FROM orders GROUP BY status"),
 ("What is the total value of all orders?", "SELECT SUM(total_amount) FROM orders"),
 ("What is the average order value?", "SELECT ROUND(AVG(total_amount), 2) FROM orders"),
 ("How many order items are there in total?", "SELECT COUNT(*) FROM order_items"),
 ("How many orders did customer 122 place?",
  "SELECT COUNT(*) FROM orders WHERE customer_id = 122"),
 ("Which five customers placed the most orders?",
  "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id "
  "ORDER BY COUNT(*) DESC, customer_id FETCH FIRST 5 ROWS ONLY"),
 ("Which product has been sold in the greatest quantity?",
  "SELECT product_id, SUM(quantity) FROM order_items GROUP BY product_id "
  "ORDER BY SUM(quantity) DESC, product_id FETCH FIRST 1 ROWS ONLY"),
 ("Which customer has the highest total order value?",
  "SELECT customer_id, SUM(total_amount) FROM orders GROUP BY customer_id "
  "ORDER BY SUM(total_amount) DESC, customer_id FETCH FIRST 1 ROWS ONLY"),
]
with open("select_ai_questions.json", "w") as f:
    json.dump([{"id": i + 1, "question": q, "reference_sql": s}
               for i, (q, s) in enumerate(questions)], f, indent=1)

# ---------- 4. Seed orders for the duality updates ----------
password = getpass.getpass("ai_lab password: ")
with oracledb.connect(user="ai_lab", password=password,
                      dsn="localhost:1521/FREEPDB1") as conn:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM orders WHERE status IN ('SEED','PACKED','SHIPPED')")
    if cur.fetchone()[0] == 0:
        cur.executemany(
            "INSERT INTO orders (customer_id, order_date, status, total_amount) "
            "VALUES (:1, TIMESTAMP '2026-01-01 00:00:00', 'SEED', 100)",
            [(i,) for i in range(1, 501)])
        conn.commit()
        print("Seeded 500 orders.")
    else:
        print("Seed orders already present, skipped.")

print("Wrote inference_inputs.json (1000), duality_workload.json (1000), "
      "select_ai_questions.json (12).")
