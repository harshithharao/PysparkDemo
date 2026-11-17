from pyspark.sql import SparkSession
from main import create_order_details


def test_order_transformation():
    spark = (
        SparkSession.builder
        .appName("TestOrderTransformation")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        orders = spark.createDataFrame(
            [
                ("O001", "C001", "P001", 2, "2026-10-01"),
                ("O002", "C002", "P002", 3, "2026-10-02"),
            ],
            ["order_id", "customer_id", "product_id", "quantity", "order_date"],
        )

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

        result = create_order_details(orders, customers, products)

        actual = {
            row["order_id"]: (
                row["customer_name"],
                row["product_name"],
                row["sales_amount"],
            )
            for row in result.collect()
        }

        expected = {
            "O001": ("Aarav", "Laptop", 2000.0),
            "O002": ("Priya", "Mouse", 300.0),
        }

        assert actual == expected, (
            f"Expected {expected}, but received {actual}"
        )

    finally:
        spark.stop()