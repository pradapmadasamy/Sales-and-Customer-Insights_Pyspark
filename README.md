# Sales-and-Customer-Insights_Pyspark
#Importing required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, desc, month, year , countDistinct,  to_date, lit
from pyspark.sql.types import StringType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Sales and Customer Insights") \
    .getOrCreate()

# Define Headers for Each CSV File
customer_columns = ["CustomerId", "Name", "City", "State", "ZipCode"]
sales_columns = ["SalesTxnId", "CategoryId", "CategoryName", "ProductId", "ProductName", "Price", "Quantity", "CustomerId"]

# Load CSV Files Without Headers
customers_df = spark.read.csv("/content/sample_data/customers.csv", header=False, inferSchema=True).toDF(*customer_columns)
sales_df = spark.read.csv("/content/sample_data/salestxns.csv", header=False, inferSchema=True).toDF(*sales_columns)

# Join Both Tables on CustomerId
combined_df = customers_df.join(sales_df, "CustomerId")

# Verify Combined DataFrame
combined_df.show(20)

# 1. Total Number of Customers
total_customers = customers_df.select(countDistinct("CustomerId").alias("TotalUniqueCustomers"))
total_customers.show()


# 2. Total Sales by State
total_sales_by_state = combined_df.groupBy("State") \
    .agg(sum(col("Price") * col("Quantity")).alias("TotalSales")) \
    .orderBy(col("TotalSales").desc())
total_sales_by_state.show()

# 3. Top 10 Most Purchased Products
top_products = combined_df.groupBy("ProductName") \
    .agg(sum("Quantity").alias("TotalQuantity")) \
    .orderBy(col("TotalQuantity").desc()) \
    .limit(10)
top_products.show()

# 4. Average Transaction Value
avg_transaction_value = combined_df.withColumn("TransactionValue", col("Price") * col("Quantity")) \
    .agg(avg("TransactionValue").alias("AvgTransactionValue"))
avg_transaction_value.show()

# 5. Top 5 Customers by Expenditure
top_customers = combined_df.withColumn("TotalSpent", col("Price") * col("Quantity")) \
    .groupBy("CustomerId", "Name") \
    .agg(sum("TotalSpent").alias("TotalSpent")) \
    .orderBy(col("TotalSpent").desc()) \
    .limit(5)
top_customers.show()


# 6. Product Purchases by a Specific Customer (e.g., CustomerId = 256)
specific_customer_purchases = combined_df.filter(col("CustomerId") == 256) \
    .withColumn("TotalSpent", col("Price") * col("Quantity")) \
    .groupBy("ProductName") \
    .agg(sum("Quantity").alias("TotalQuantity"), sum("TotalSpent").alias("TotalSpent")) \
    .orderBy(desc("TotalSpent"))
specific_customer_purchases.show()

# 7. Monthly Sales Trends
# Add a mock 'Date' column for analysis
combined_df_with_date = combined_df.withColumn("Date", lit("2024-01-01").cast(StringType()))  # Add a fixed date for demonstration
monthly_sales_trends = combined_df_with_date.withColumn("Month", month(to_date("Date", "yyyy-MM-dd"))) \
    .withColumn("Year", year(to_date("Date", "yyyy-MM-dd"))) \
    .withColumn("TotalSales", col("Price") * col("Quantity")) \
    .groupBy("Year", "Month") \
    .agg(sum("TotalSales").alias("TotalSales")) \
    .orderBy(col("TotalSales").desc())
monthly_sales_trends.show(1)

# 8. Category with Highest Sales
highest_sales_category = combined_df.groupBy("CategoryName") \
    .agg(sum(col("Price") * col("Quantity")).alias("TotalSales")) \
    .orderBy(col("TotalSales").desc()) \
    .limit(1)
highest_sales_category.show()


# 9. State-wise Sales Comparison (e.g., Salem vs. Kent)
state_sales_comparison = combined_df.filter(col("State").isin("OR", "WA")) \
    .groupBy("State") \
    .agg(sum(col("Price") * col("Quantity")).alias("TotalSales")) \
    .orderBy(col("TotalSales").desc())
state_sales_comparison.show()

# 10. Detailed Customer Purchase Report
customer_purchase_report = combined_df.withColumn("TotalSpent", col("Price") * col("Quantity")) \
    .groupBy("CustomerId", "Name") \
    .agg(
        sum("TotalSpent").alias("TotalPurchases"),
        count("SalesTxnId").alias("TotalTransactions"),
        (sum("TotalSpent") / count("SalesTxnId")).alias("AvgTransactionValue")
    )
customer_purchase_report.show()
