from pyspark.sql import SparkSession
from main import calculate_category_sales


def test_category_sales_aggregation():
    spark = (
        SparkSession.builder
        .appName("TestCategorySales")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        order_details = spark.createDataFrame(
            [
                ("O001", "Electronics", 2, 1000.0),
                ("O002", "Electronics", 1, 500.0),
                ("O003", "Accessories", 3, 100.0),
            ],
            ["order_id", "category", "quantity", "sales_amount"],
        )

        result = calculate_category_sales(order_details)

        actual = {
            row["category"]: (
                row["total_quantity"],
                row["total_sales"],
            )
            for row in result.collect()
        }

        expected = {
    "Electronics": (3, 1500.0),
    "Accessories": (3, 100.0),
}

        assert actual == expected, (
            f"Expected {expected}, but received {actual}"
        )

    finally:
        spark.stop()