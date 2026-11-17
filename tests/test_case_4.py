from pyspark.sql import SparkSession
from main import calculate_city_sales


def test_city_sales_aggregation():
    spark = (
        SparkSession.builder
        .appName("TestCitySales")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        order_details = spark.createDataFrame(
            [
                ("O001", "Hyderabad", 2000.0),
                ("O002", "Hyderabad", 500.0),
                ("O003", "Bengaluru", 300.0),
                ("O003", "Bengaluru", 300.0),
            ],
            ["order_id", "city", "sales_amount"],
        )

        result = calculate_city_sales(order_details)

        actual = {
            row["city"]: (
                row["total_orders"],
                row["total_sales"],
            )
            for row in result.collect()
        }

        expected = {
            "Hyderabad": (2, 2500.0),
            "Bengaluru": (1, 600.0),
        }

        assert actual == expected, (
            f"Expected {expected}, but received {actual}"
        )

    finally:
        spark.stop()