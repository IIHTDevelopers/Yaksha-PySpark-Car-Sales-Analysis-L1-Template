from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, year, desc

# Initialize Spark session
spark = SparkSession.builder.appName("Car Sales Analysis").getOrCreate()

# 1. Load car sales data into a Spark DataFrame
def load_data(file_path):
    """
    Load car sales data from a CSV file.
    """
    pass
    # Placeholder, returning None
    return None

# 2. Clean the data (remove rows with missing values and correct data types)
def clean_data(df):
    """
    Clean the car sales data by removing nulls and converting columns to appropriate data types.
    """
    pass
    # Placeholder, returning None
    return None

# 3. Calculate total sales for all cars
def calculate_total_sales(df):
    """
    Calculate total sales (price * quantity) for all car sales.
    """
    pass
    # Placeholder, returning None
    return None

# 4. Get the top-selling car model
def top_selling_model(df):
    """
    Find the top-selling car model by total sales.
    """
    pass
    # Placeholder, returning None
    return None

# 5. Calculate average sales price for a specific brand
def avg_sales_price_by_brand(df, brand):
    """
    Calculate the average sales price for a specific car brand.
    """
    pass
    # Placeholder, returning None
    return None

# 6. Count the total number of cars sold within a price range
def count_cars_in_price_range(df, min_price, max_price):
    """
    Count the total number of cars sold within a specific price range.
    """
    pass
    # Placeholder, returning None
    return None

# 7. Get summary statistics (total quantity sold, average price)
def sales_summary_statistics(df):
    """
    Get total quantity sold and average price for all car sales.
    """
    pass
    # Placeholder, returning None
    return None, None

# 8. Get total sales for a specific year
def total_sales_for_year(df, year_value):
    """
    Calculate total sales for a specific year.
    """
    pass
    # Placeholder, returning None
    return None

# 9. Get the least selling car model
def least_selling_model(df):
    """
    Find the least-selling car model by total sales.
    """
    pass
    # Placeholder, returning None
    return None

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

    # Show results, handling None return values
    print(f"Total Sales: {total_sales if total_sales is not None else 'Not available'}")
    print(f"Top Selling Model: {top_model if top_model is not None else 'Not available'}")
    print(f"Average Price for Toyota: {avg_price_toyota if avg_price_toyota is not None else 'Not available'}")
    print(f"Cars Sold in Price Range $20,000 - $50,000: {cars_in_range if cars_in_range is not None else 'Not available'}")
    print(f"Total Quantity Sold: {total_quantity if total_quantity is not None else 'Not available'}")
    print(f"Average Price: {avg_price if avg_price is not None else 'Not available'}")
    print(f"Total Sales in 2023: {total_sales_2023 if total_sales_2023 is not None else 'Not available'}")
    print(f"Least Selling Model: {least_model if least_model is not None else 'Not available'}")
