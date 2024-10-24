from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, avg, year, desc

# Initialize Spark session
spark = SparkSession.builder.appName("Car Sales Analysis").getOrCreate()

# 1. Load car sales data into a Spark DataFrame
def load_data(file_path):
    """
    Load car sales data from a CSV file.
    """
    return spark.read.option("header", "true").csv(file_path)

# 2. Clean the data (remove rows with missing values and correct data types)
def clean_data(df):
    """
    Clean the car sales data by removing nulls and converting columns to appropriate data types.
    """
    df = df.dropna()
    df = df.withColumn("price", col("price").cast("double")) \
           .withColumn("quantity", col("quantity").cast("int"))
    return df

# 3. Calculate total sales for all cars
def calculate_total_sales(df):
    """
    Calculate total sales (price * quantity) for all car sales.
    """
    total_sales = df.withColumn("total_sales", col("price") * col("quantity")) \
                    .agg(_sum("total_sales")).collect()[0][0]
    return total_sales

# 4. Get the top-selling car model
def top_selling_model(df):
    """
    Find the top-selling car model by total sales.
    """
    df = df.withColumn("total_sales", col("price") * col("quantity"))
    top_model = df.groupBy("model").agg(_sum("total_sales").alias("total_sales")) \
                   .orderBy(desc("total_sales")).first()
    return top_model["model"]

# 5. Calculate average sales price for a specific brand
def avg_sales_price_by_brand(df, brand):
    """
    Calculate the average sales price for a specific car brand.
    """
    avg_price = df.filter(col("brand") == brand).agg(avg("price")).collect()[0][0]
    return avg_price

# 6. Count the total number of cars sold within a price range
def count_cars_in_price_range(df, min_price, max_price):
    """
    Count the total number of cars sold within a specific price range.
    """
    total_cars = df.filter((col("price") >= min_price) & (col("price") <= max_price)) \
                   .agg(_sum("quantity")).collect()[0][0]
    return total_cars



# 8. Get summary statistics (total quantity sold, average price)
def sales_summary_statistics(df):
    """
    Get total quantity sold and average price for all car sales.
    """
    total_quantity_sold = df.agg(_sum("quantity")).collect()[0][0]
    avg_price = df.agg(avg("price")).collect()[0][0]
    return total_quantity_sold, avg_price

# 9. Get total sales for a specific year
def total_sales_for_year(df, year_value):
    """
    Calculate total sales for a specific year.
    Assumes there is a 'date' column in the format 'yyyy-MM-dd'.
    """
    df = df.withColumn("year", year(col("date")))
    total_sales = df.filter(col("year") == year_value) \
                    .withColumn("total_sales", col("price") * col("quantity")) \
                    .agg(_sum("total_sales")).collect()[0][0]
    return total_sales

# 10. Get the least selling car model
def least_selling_model(df):
    """
    Find the least-selling car model by total sales.
    """
    df = df.withColumn("total_sales", col("price") * col("quantity"))
    least_model = df.groupBy("model").agg(_sum("total_sales").alias("total_sales")) \
                    .orderBy("total_sales").first()
    return least_model["model"]

# Example usage of the functions
if __name__ == "__main__":
    # Path to the CSV file containing car sales data
    file_path = "car.csv"

    # Load and clean data
    sales_df = load_data(file_path)
    sales_df = clean_data(sales_df)

    # Perform analysis and get single values as output
    total_sales = calculate_total_sales(sales_df)
    top_model = top_selling_model(sales_df)
    avg_price_toyota = avg_sales_price_by_brand(sales_df, "Toyota")
    cars_in_range = count_cars_in_price_range(sales_df, 20000, 50000)

    total_quantity, avg_price = sales_summary_statistics(sales_df)
    total_sales_2023 = total_sales_for_year(sales_df, 2023)
    least_model = least_selling_model(sales_df)

    # Show results
    print(f"Total Sales: ${total_sales}")
    print(f"Top Selling Model: {top_model}")
    print(f"Average Price for Toyota: ${avg_price_toyota}")
    print(f"Cars Sold in Price Range $20,000 - $50,000: {cars_in_range}")
    print(f"Total Quantity Sold: {total_quantity}")
    print(f"Average Price: ${avg_price}")
    print(f"Total Sales in 2023: ${total_sales_2023}")
    print(f"Least Selling Model: {least_model}")
