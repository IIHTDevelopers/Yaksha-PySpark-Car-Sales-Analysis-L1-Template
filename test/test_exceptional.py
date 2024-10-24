import unittest
from test.TestUtils import TestUtils
from main import *

class ExceptionalTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Load the data and clean it
        cls.sales_df = load_data('car.csv')
        if cls.sales_df is not None:
            cls.sales_df = clean_data(cls.sales_df)

    def test_empty_dataset(self):
        # Check if the dataset was successfully loaded
        if self.sales_df is None:
            # Dataset was not loaded, mark test as failed
            test_obj = TestUtils()
            test_obj.yakshaAssert("TestEmptyDataset", False, "exception")
            print("TestEmptyDataset = Failed (Dataset not loaded)")
        else:
            # Check if the dataset is empty
            if self.sales_df.count() == 0:
                # Dataset is empty, mark test as failed
                test_obj = TestUtils()
                test_obj.yakshaAssert("TestEmptyDataset", False, "exception")  # Fail if the dataset is empty
                print("TestEmptyDataset = Failed (Dataset is empty)")
            else:
                # Dataset is not empty, mark test as passed
                test_obj = TestUtils()
                test_obj.yakshaAssert("TestEmptyDataset", True, "exception")  # Pass if the dataset is not empty
                print("TestEmptyDataset = Passed (Dataset is not empty)")


# The following lines are for running the tests when this script is executed
if __name__ == "__main__":
    unittest.main()
