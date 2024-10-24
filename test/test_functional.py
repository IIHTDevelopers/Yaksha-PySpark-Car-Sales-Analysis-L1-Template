import unittest
from test.TestUtils import TestUtils
from main import *

class CarSalesAnalysisTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Assuming the data is loaded and cleaned in the test setup
        # Replace 'path/to/car_sales.csv' with the actual file path to the test data.
        cls.sales_df = load_data('car.csv')
        cls.sales_df = clean_data(cls.sales_df)

    def test_total_sales(self):
        # Expected total sales
        expected_total_sales = 2051000.0
        result = calculate_total_sales(self.sales_df)
        test_obj = TestUtils()
        test_obj.yakshaAssert("TestTotalSales", result == expected_total_sales, "functional")
        print(f"TestTotalSales = {'Passed' if result == expected_total_sales else 'Failed'}")

    def test_top_selling_model(self):
        # Expected top-selling model
        expected_top_model = "RAV4"
        result = top_selling_model(self.sales_df)
        test_obj = TestUtils()
        test_obj.yakshaAssert("TestTopSellingModel", result == expected_top_model, "functional")
        print(f"TestTopSellingModel = {'Passed' if result == expected_top_model else 'Failed'}")

    def test_avg_price_toyota(self):
        # Expected average price for Toyota
        expected_avg_price_toyota = 26750.0
        result = avg_sales_price_by_brand(self.sales_df, "Toyota")
        test_obj = TestUtils()
        test_obj.yakshaAssert("TestAvgPriceToyota", result == expected_avg_price_toyota, "functional")
        print(f"TestAvgPriceToyota = {'Passed' if result == expected_avg_price_toyota else 'Failed'}")

    def test_cars_in_price_range(self):
        # Expected cars sold in the price range $20,000 - $50,000
        expected_cars_in_range = 70
        result = count_cars_in_price_range(self.sales_df, 20000, 50000)
        test_obj = TestUtils()
        test_obj.yakshaAssert("TestCarsInPriceRange", result == expected_cars_in_range, "functional")
        print(f"TestCarsInPriceRange = {'Passed' if result == expected_cars_in_range else 'Failed'}")


    def test_sales_summary(self):
        # Expected total quantity sold and average price
        expected_total_quantity = 81
        expected_avg_price = 26566.666666666668
        total_quantity, avg_price = sales_summary_statistics(self.sales_df)
        test_obj = TestUtils()
        test_obj.yakshaAssert("TestSalesSummaryTotalQuantity", total_quantity == expected_total_quantity, "functional")
        print(f"TestSalesSummaryTotalQuantity = {'Passed' if total_quantity == expected_total_quantity else 'Failed'}")
        test_obj.yakshaAssert("TestSalesSummaryAvgPrice", avg_price == expected_avg_price, "functional")
        print(f"TestSalesSummaryAvgPrice = {'Passed' if avg_price == expected_avg_price else 'Failed'}")

    def test_total_sales_2023(self):
        # Expected total sales in 2023
        expected_total_sales_2023 = 457000.0
        result = total_sales_for_year(self.sales_df, 2023)
        test_obj = TestUtils()
        test_obj.yakshaAssert("TestTotalSales2023", result == expected_total_sales_2023, "functional")
        print(f"TestTotalSales2023 = {'Passed' if result == expected_total_sales_2023 else 'Failed'}")

    def test_least_selling_model(self):
        # Expected least-selling model
        expected_least_model = "Fit"
        result = least_selling_model(self.sales_df)
        test_obj = TestUtils()
        test_obj.yakshaAssert("TestLeastSellingModel", result == expected_least_model, "functional")
        print(f"TestLeastSellingModel = {'Passed' if result == expected_least_model else 'Failed'}")


# Example of the boundary test case based on your sample format



# The following lines are for running the tests when this script is executed
if __name__ == "__main__":
    unittest.main()
