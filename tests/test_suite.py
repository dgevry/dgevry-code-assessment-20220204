"""
# Title : Code Assessment - Test Suites
# Description : This is utility to generate test data sets
# Author : David Gevry
# Date : 2022-02-04
# Version : 1.0
"""
import unittest
import os
import creator
import hashlib
import sys

TEST_ROOT = os.path.join(os.getcwd(),'tests')
SIMPLE_DATA = os.path.join(TEST_ROOT,'simple-data')
DYNAMIC_DATA = os.path.join(TEST_ROOT,'dynamic-data')

class GeneralTestCase(unittest.TestCase):
    def __init__(self, methodName, label=None, category=None):
        super(GeneralTestCase, self).__init__(methodName)
        self.category = category
        if self.category == 'simple':
            self.test_folder = SIMPLE_DATA
        else:
            self.test_folder = DYNAMIC_DATA
        self.label = label

    def md5(self, file_path):
        """
        Generate MD5 for provided path
        """
        try:
            md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(61440), b""):
                    md5.update(chunk)
            return md5.hexdigest()
        except Exception as err:
            print(f"Error for {file_path} checksum generation - {err}")
            sys.exit(1)
        
    def runTest(self):
        print(self.label)
        
        dataset1_path = os.path.join(self.test_folder,self.label,'input1.csv')
        dataset2_path = os.path.join(self.test_folder,self.label,'input2.csv')
        result_path = os.path.join(self.test_folder,self.label,'result.csv')
        expected_path = os.path.join(self.test_folder,self.label,'expected.csv')
        
        test_param = {
            'dataset1_path': dataset1_path,
            'dataset2_path': dataset2_path,
            'result_path': result_path,
            'show': 'No'
        }
        creator.main(test_param)
        if self.category == "simple":
            with open(expected_path) as expected_file:
                with open(result_path) as result_file:
                    self.assertEqual(expected_file.read(),result_file.read())
        else:
            # Due to volume comparison with the expected is done via checksum
            self.assertEqual(self.md5(result_path),self.md5(expected_path))

def load_tests(loader, tests, pattern):
    """
    Loads tests from TEST_DATA Folder
    """
    test_cases = unittest.TestSuite()
    simple_test_list=[d for d in os.listdir(SIMPLE_DATA) if os.path.isdir(os.path.join(SIMPLE_DATA,d))]
    dynamic_test_list=[d for d in os.listdir(DYNAMIC_DATA) if os.path.isdir(os.path.join(DYNAMIC_DATA,d))]
    print(f"Simple Datasets: {simple_test_list}")
    for test_label in simple_test_list:
        test_cases.addTest(GeneralTestCase('runTest', test_label,'simple'))
    print(dynamic_test_list)
    print(f"Dynamic Datasets: {dynamic_test_list}")
    for test_label in dynamic_test_list:
        test_cases.addTest(GeneralTestCase('runTest', test_label,'dynamic'))
    return test_cases