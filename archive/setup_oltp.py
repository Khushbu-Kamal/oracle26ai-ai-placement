import getpass
import oracledb

password = getpass.getpass("Oracle password: ")

with oracledb.connect(
    user="ai_lab",
    password=password,
    dsn="localhost:1521/FREEPDB1"
) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE ai_lab_oltp_accounts (
                account_id NUMBER PRIMARY KEY,
                balance NUMBER NOT NULL
            )
        """)
        cur.executemany(
            "INSERT INTO ai_lab_oltp_accounts VALUES (:1, :2)",
            [(i, 1000) for i in range(1, 10001)]
        )
    conn.commit()

print("Created 10,000 test accounts.")

