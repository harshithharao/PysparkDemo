# Retail Sales Data Processing Using PySpark

## Problem Statement

A retail company maintains customer, product, and order information in three CSV files. The company wants to combine this information and generate useful sales summaries.

Your task is to build a PySpark program that processes these datasets using Spark DataFrames.

## Input Datasets

All input files are available in the `data/` directory.

- `customers.csv`: Contains customer IDs, names, and cities.
- `products.csv`: Contains product IDs, product names, categories, and prices.
- `orders.csv`: Contains order IDs, customer IDs, product IDs, quantities, and order dates.

## Requirements

Implement the following operations using PySpark:

### 1. Load the Datasets

Load all three CSV files into separate Spark DataFrames. Ensure that the column names are read from the CSV headers and numeric columns have appropriate data types.

### 2. Validate Orders

Remove orders that have missing required values or a quantity less than or equal to zero.

Also remove orders that reference customer IDs or product IDs that do not exist in their respective datasets.

If an order ID occurs more than once, exclude all records with that order ID.

### 3. Generate Order Details

Join the valid orders with the customer and product datasets.

Calculate `sales_amount` using:

`sales_amount = quantity * price`

The resulting DataFrame must contain these columns:

- `order_id`
- `order_date`
- `customer_id`
- `customer_name`
- `city`
- `product_id`
- `product_name`
- `category`
- `quantity`
- `price`
- `sales_amount`

### 4. Generate Sales Summaries

Create two summary DataFrames.

**Category-wise sales**

Group the order details by product category and calculate:

- `total_quantity`: Total quantity of products sold in the category.
- `total_sales`: Total sales amount for the category.

**City-wise sales**

Group the order details by customer city and calculate:

- `total_orders`: Number of distinct valid orders associated with the city.
- `total_sales`: Total sales amount for the city.

## Technical Requirements

- Use PySpark DataFrames for data processing.
- Use DataFrame operations such as filtering, joins, column transformations, and aggregations.
- Do not convert the complete datasets into Python lists or Pandas DataFrames for processing.
- Do not hardcode the expected output values.
- Keep the implementation organized and reusable.

## Expected Deliverables

The program must produce:

1. A DataFrame containing the transformed order details.
2. A category-wise sales summary.
3. A city-wise sales summary.

The program must run using:

`python main.py`
