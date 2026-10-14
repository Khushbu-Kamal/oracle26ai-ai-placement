import getpass
import random
import oracledb

random.seed(42)
password = getpass.getpass("ai_lab password: ")

with oracledb.connect(user="ai_lab", password=password,
                      dsn="localhost:1521/FREEPDB1") as conn:
    cur = conn.cursor()

    cur.execute("""CREATE TABLE customers (
        customer_id NUMBER PRIMARY KEY,
        customer_name VARCHAR2(100) NOT NULL)""")

    cur.execute("""CREATE TABLE products (
        product_id NUMBER PRIMARY KEY,
        price NUMBER(10,2) NOT NULL)""")

    cur.execute("""CREATE TABLE orders (
        order_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        customer_id NUMBER NOT NULL REFERENCES customers,
        order_date TIMESTAMP NOT NULL,
        status VARCHAR2(20) NOT NULL,
        total_amount NUMBER(12,2) NOT NULL)""")

    cur.execute("""CREATE TABLE order_items (
        order_id NUMBER NOT NULL REFERENCES orders,
        product_id NUMBER NOT NULL REFERENCES products,
        quantity NUMBER NOT NULL,
        unit_price NUMBER(10,2) NOT NULL)""")

    cur.execute("CREATE INDEX order_items_order_ix ON order_items(order_id)")

    cur.executemany("INSERT INTO customers VALUES (:1, :2)",
                    [(i, f"Customer {i}") for i in range(1, 10001)])
    cur.executemany("INSERT INTO products VALUES (:1, :2)",
                    [(i, round(random.uniform(5, 500), 2)) for i in range(1, 1001)])
    conn.commit()

print("Created customers (10,000), products (1,000), orders, order_items.")
