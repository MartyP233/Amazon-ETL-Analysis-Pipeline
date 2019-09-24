import unittest
import os.path

from kindle import unzip_data

class TestZip(unittest.TestCase):
    def test_csv_unzip(self):
        """
        Test that function can unzip a csv file
        """
        unzip_data.unzip_file(zip_file = 'Data/test_files/testzip.zip', directory = 'Data/test_files/', filename = 'testzip.csv')
        result = os.path.isfile('Data/test_files/testzip.csv')
        os.remove('Data/test_files/testzip.csv')
        self.assertTrue(result)

if __name__ == '__main__':
    unittest.main()