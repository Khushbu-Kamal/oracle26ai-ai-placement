import os
import oracledb
import time
import statistics
import random
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed


DB_USER = "ai_lab"
DB_PASSWORD = os.environ["AI_LAB_PASSWORD"]
DB_DSN = "localhost:1521/FREEPDB1"


def run_transaction(pool, customer_count, product_count, rng):

    connection = pool.acquire()

    try:
        cursor = connection.cursor()

        start = time.perf_counter()

        # Random customer and products.
        customer_id = rng.randint(1, customer_count)

        product_ids = rng.sample(
            range(1, product_count + 1),
            3
        )

        # --------------------------------------------------
        # 1. Read customer
        # --------------------------------------------------

        cursor.execute("""
            SELECT customer_id, customer_name
            FROM customers
            WHERE customer_id = :1
        """, (customer_id,))

        customer = cursor.fetchone()

        if customer is None:
            raise RuntimeError("Customer not found")

        # --------------------------------------------------
        # 2. Read three products
        # --------------------------------------------------

        products = []

        for product_id in product_ids:

            cursor.execute("""
                SELECT product_id, price
                FROM products
                WHERE product_id = :1
            """, (product_id,))

            product = cursor.fetchone()

            if product is None:
                raise RuntimeError("Product not found")

            products.append(product)

        # --------------------------------------------------
        # 3. Calculate order total
        # --------------------------------------------------

        quantities = [
            rng.randint(1, 3)
            for _ in products
        ]

        total_amount = sum(
            product[1] * quantity
            for product, quantity in zip(products, quantities)
        )

        # --------------------------------------------------
        # 4. Insert order
        # --------------------------------------------------

        order_id_var = cursor.var(int)

        cursor.execute("""
            INSERT INTO orders (
                customer_id,
                order_date,
                status,
                total_amount
            )
            VALUES (
                :1,
                SYSTIMESTAMP,
                'NEW',
                :2
            )
            RETURNING order_id INTO :3
        """, (
            customer_id,
            total_amount,
            order_id_var
        ))

        order_id = order_id_var.getvalue()[0]

        # --------------------------------------------------
        # 5. Insert order items
        # --------------------------------------------------

        for product, quantity in zip(products, quantities):

            product_id = product[0]
            unit_price = product[1]

            cursor.execute("""
                INSERT INTO order_items (
                    order_id,
                    product_id,
                    quantity,
                    unit_price
                )
                VALUES (
                    :1,
                    :2,
                    :3,
                    :4
                )
            """, (
                order_id,
                product_id,
                quantity,
                unit_price
            ))

        # --------------------------------------------------
        # 6. Commit transaction
        # --------------------------------------------------

        connection.commit()

        latency = time.perf_counter() - start

        return latency, True

    except Exception as e:

        connection.rollback()

        return None, False

    finally:

        cursor.close()
        pool.release(connection)


def worker(
    pool,
    transactions,
    customer_count,
    product_count,
    seed
):

    rng = random.Random(seed)
    results = []

    for _ in range(transactions):

        results.append(
            run_transaction(
                pool,
                customer_count,
                product_count,
                rng
            )
        )

    return results


def percentile(values, percentile):

    values = sorted(values)

    if not values:
        return 0

    index = int(len(values) * percentile / 100)

    if index >= len(values):
        index = len(values) - 1

    return values[index]


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--concurrency",
        type=int,
        default=10
    )

    parser.add_argument(
        "--transactions",
        type=int,
        default=100
    )

    parser.add_argument(
        "--customers",
        type=int,
        default=10000
    )

    parser.add_argument(
        "--products",
        type=int,
        default=1000
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42
    )

    args = parser.parse_args()

    total_transactions = (
        args.concurrency *
        args.transactions
    )

    print()
    print("Oracle AI Database 26ai - OLTP Benchmark")
    print("----------------------------------------")
    print(f"Concurrency:          {args.concurrency}")
    print(f"Transactions/worker:  {args.transactions}")
    print(f"Total transactions:   {total_transactions}")
    print()

    # Connection pool
    pool = oracledb.create_pool(
        user=DB_USER,
        password=DB_PASSWORD,
        dsn=DB_DSN,
        min=args.concurrency,
        max=args.concurrency,
        increment=1
    )

    all_results = []

    benchmark_start = time.perf_counter()

    try:

        with ThreadPoolExecutor(
            max_workers=args.concurrency
        ) as executor:

            futures = [
                executor.submit(
                    worker,
                    pool,
                    args.transactions,
                    args.customers,
                    args.products,
                    args.seed + worker_id
                )
                for worker_id in range(args.concurrency)
            ]

            for future in as_completed(futures):

                all_results.extend(
                    future.result()
                )

    finally:

        pool.close()

    benchmark_duration = (
        time.perf_counter() -
        benchmark_start
    )

    latencies = [
        latency
        for latency, success in all_results
        if success
    ]

    successes = sum(
        1
        for _, success in all_results
        if success
    )

    failures = total_transactions - successes

    print("RESULTS")
    print("----------------------------------------")

    print(
        f"Successful transactions: {successes}"
    )

    print(
        f"Failed transactions:     {failures}"
    )

    print(
        f"Duration:                "
        f"{benchmark_duration:.3f} sec"
    )

    print(
        f"Throughput:              "
        f"{successes / benchmark_duration:.2f} TPS"
    )

    print()

    if latencies:

        print("LATENCY")
        print("----------------------------------------")

        print(
            f"Average:                 "
            f"{statistics.mean(latencies) * 1000:.2f} ms"
        )

        print(
            f"p50:                     "
            f"{percentile(latencies, 50) * 1000:.2f} ms"
        )

        print(
            f"p95:                     "
            f"{percentile(latencies, 95) * 1000:.2f} ms"
        )

        print(
            f"p99:                     "
            f"{percentile(latencies, 99) * 1000:.2f} ms"
        )


if __name__ == "__main__":
    main()