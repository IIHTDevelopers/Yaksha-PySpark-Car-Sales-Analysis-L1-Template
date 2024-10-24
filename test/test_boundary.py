import unittest
from test.TestUtils import TestUtils
from main import *

class BoundaryTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Assuming the data is loaded and cleaned in the test setup
        # Replace 'car.csv' with the actual file path to the test data.
        cls.sales_df = load_data('car.csv')
        cls.sales_df = clean_data(cls.sales_df)


    def test_boundary_price_range(self):
        # Boundary test for cars sold at the edge of the price range ($0 - $10,000)
        expected_cars_in_range = 70  # Expected no cars sold in this price range
        result = count_cars_in_price_range(self.sales_df, 20000, 50000)
        test_obj = TestUtils()
        test_obj.yakshaAssert("TestBoundaryPriceRange", result == expected_cars_in_range, "boundary")
        print(f"TestBoundaryPriceRange = {'Passed' if result == expected_cars_in_range else 'Failed'}")


# The following lines are for running the tests when this script is executed
if __name__ == "__main__":
    unittest.main()
