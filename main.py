from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def load_datasets(spark, data_dir):
    """Load customer, product, and order datasets."""

    customers = spark.read.csv(
        str(data_dir / "customers.csv"),
        header=True,
        inferSchema=True
    )

    products = spark.read.csv(
        str(data_dir / "products.csv"),
        header=True,
        inferSchema=True
    )

    orders = spark.read.csv(
        str(data_dir / "orders.csv"),
        header=True,
        inferSchema=True
    )

    return customers, products, orders



def validate_orders(
    orders: DataFrame,
    customers: DataFrame,
    products: DataFrame
) -> DataFrame:
    """Remove invalid orders and orders with duplicate IDs."""

    # Identify duplicate order IDs before applying other filters
    duplicate_ids = (
        orders
        .filter(
            F.col("order_id").isNotNull()
            & (F.trim(F.col("order_id").cast("string")) != "")
        )
        .groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
        .select("order_id")
    )

    # Exclude every record belonging to a duplicate order ID
    valid_orders = orders.join(
        duplicate_ids,
        on="order_id",
        how="left_anti"
    )

    # Remove records with missing required values
    required_columns = [
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "order_date"
    ]

    for column_name in required_columns:
        valid_orders = valid_orders.filter(
            F.col(column_name).isNotNull()
            & (F.trim(F.col(column_name).cast("string")) != "")
        )

    # Remove zero or negative quantities
    valid_orders = valid_orders.filter(F.col("quantity") > 0)

    # Exclude orders with unknown customer IDs
    valid_orders = valid_orders.join(
        customers.select("customer_id").distinct(),
        on="customer_id",
        how="left_semi"
    )

    # Exclude orders with unknown product IDs
    valid_orders = valid_orders.join(
        products.select("product_id").distinct(),
        on="product_id",
        how="left_semi"
    )

    return valid_orders



def create_order_details(
    orders: DataFrame,
    customers: DataFrame,
    products: DataFrame
) -> DataFrame:
    """Join datasets and calculate sales amounts."""

    order_details = (
        orders
        .join(customers, on="customer_id", how="inner")
        .join(products, on="product_id", how="inner")
        .withColumn(
            "sales_amount",
            F.col("quantity") * F.col("price")
        )
        .select(
            "order_id",
            "order_date",
            "customer_id",
            F.col("name").alias("customer_name"),
            "city",
            "product_id",
            "product_name",
            "category",
            "quantity",
            "price",
            "sales_amount"
        )
    )

    return order_details


def calculate_category_sales(order_details: DataFrame) -> DataFrame:
    """Calculate total quantity and sales by category."""

    return (
        order_details
        .groupBy("category")
        .agg(
            F.sum("quantity").alias("total_quantity"),
            F.sum("sales_amount").alias("total_sales")
        )
    )


def calculate_city_sales(order_details: DataFrame) -> DataFrame:
    """Calculate distinct order counts and total sales by city."""

    return (
        order_details
        .groupBy("city")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("sales_amount").alias("total_sales")
        )
    )


def main():
    project_dir = Path(__file__).resolve().parent
    data_dir = project_dir / "data"

    spark = (
        SparkSession.builder
        .appName("RetailSalesPySparkDemo")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n=== LOADING DATASETS ===")

        customers, products, orders = load_datasets(spark, data_dir)

        print(f"Customers: {customers.count()}")
        print(f"Products: {products.count()}")
        print(f"Orders: {orders.count()}")

        print("\n=== VALIDATING ORDERS ===")

        valid_orders = validate_orders(orders, customers, products)

        print(f"Valid orders: {valid_orders.count()}")

        print("\n=== ORDER DETAILS ===")

        order_details = create_order_details(
            valid_orders,
            customers,
            products
        )

        order_details.orderBy("order_id").show(10, truncate=False)

        print("\n=== CATEGORY-WISE SALES ===")

        category_sales = calculate_category_sales(order_details)

        category_sales.orderBy("category").show(truncate=False)

        print("\n=== CITY-WISE SALES ===")

        city_sales = calculate_city_sales(order_details)

        city_sales.orderBy("city").show(truncate=False)

        print("\nSUCCESS: PySpark processing completed!")
        print("\nCLEAN ORDER DETAILS")
        order_details.show(100, truncate=False)

        print("\nCATEGORY-WISE SALES SUMMARY")
        category_sales.show(truncate=False)

        print("\nCITY-WISE SALES SUMMARY")
        city_sales.show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()