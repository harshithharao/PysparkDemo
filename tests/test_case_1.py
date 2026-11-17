
from pyspark.sql import SparkSession
from main import validate_orders


def test_order_validation():
    spark = (
        SparkSession.builder
        .appName("TestOrderValidation")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        customers = spark.createDataFrame(
            [
                ("C001", "Aarav", "Hyderabad"),
                ("C002", "Priya", "Bengaluru"),
            ],
            ["customer_id", "name", "city"],
        )

        products = spark.createDataFrame(
            [
                ("P001", "Laptop", "Electronics", 1000.0),
                ("P002", "Mouse", "Accessories", 100.0),
            ],
            ["product_id", "product_name", "category", "price"],
        )

        orders = spark.createDataFrame(
            [
                ("O001", "C001", "P001", 2, "2026-10-01"),
                ("O002", "C002", "P002", 1, "2026-10-02"),
                ("O003", "C001", "P001", 0, "2026-10-03"),
                ("O004", "C001", "P001", -1, "2026-10-04"),
                ("O005", None, "P001", 1, "2026-10-05"),
                ("O006", "C999", "P001", 1, "2026-10-06"),
                ("O007", "C001", "P999", 1, "2026-10-07"),
                ("O008", "C001", "P001", 1, "2026-10-08"),
                ("O008", "C002", "P002", 1, "2026-10-09"),
                ("O009", "C001", "P001", 2, "2026-10-10"),
("O009", "C001", "P001", 0, "2026-10-11"),
            ],
            ["order_id", "customer_id", "product_id", "quantity", "order_date"],
        )

        valid_orders = validate_orders(
            orders, customers, products
        )

        actual_ids = {
            row["order_id"]
            for row in valid_orders.select("order_id").collect()
        }

        expected_ids = {"O001", "O002"}

        assert actual_ids == expected_ids, (
            f"Expected valid order IDs {expected_ids}, but got {actual_ids}"
        )

    finally:
        spark.stop()
