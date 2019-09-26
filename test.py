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

import boto3
from kindle import load_files
import configparser
import warnings

config = configparser.ConfigParser()
config.read_file(open("kindle/dwh.cfg"))

KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")

def ignore_warnings(test_func):
    def do_test(self, *args, **kwargs):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ResourceWarning)
            test_func(self, *args, **kwargs)
    return do_test

class TestUpload(unittest.TestCase):
    @ignore_warnings
    def test_upload_json_files(self):
        s3 = boto3.client('s3', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
        # upload file
        load_files.upload_to_aws('Data/test_files/test_upload.csv', 'kindle-reviews-and-sales', 'test_upload.csv')
        # test file upload success
        result = s3.head_object(Bucket='kindle-reviews-and-sales', Key='test_upload.csv')
        self.assertTrue(result)
        # delete object
        s3.delete_object(Bucket='kindle-reviews-and-sales', Key='test_upload.csv')

if __name__ == '__main__':
    unittest.main()

