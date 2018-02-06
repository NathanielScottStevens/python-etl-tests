import pytest
import pandas as pd
import pandas.util.testing as pdt
import unittest
import testable as t

class TableGetterStub:

    def __init__(self, df):
        self.df = df

    def get_table(self, table_name):
        return self.df


# Ugly output when using pandas.util.testing
def test_filters_table():
    original_data = {'a': [1, 1, 5], 
                     'b': [5, 5, 6]}
    original = pd.DataFrame(data=original_data)

    
    expected_data = {'a': [1, 1], 
                     'b': [5, 5]}
    expected = pd.DataFrame(data=expected_data)

    subject = t.ETL()
    actual = subject.transform_table(TableGetterStub(original))

    pdt.assert_frame_equal(expected, actual)

#def test_fail_one_value_off():
#    original_data = {'a': [1, 1, 5], 
#                     'b': [5, 5, 6]}
#    original = pd.DataFrame(data=original_data)
#
#    
#    expected_data = {'a': [1, 1, 10], 
#                     'b': [5, 5, 6]}
#    expected = pd.DataFrame(data=expected_data)
#
#    pdt.assert_frame_equal(original, expected)
#
# Goes column by column and outputs only the first failed column
#def test_fail_two_value_off():
#    original_data = {'a': [1, 1, 5], 
#                     'b': [5, 5, 6]}
#    original = pd.DataFrame(data=original_data)
#
#    
#    expected_data = {'a': [1, 5, 5], 
#                     'b': [5, 2, 12]}
#    expected = pd.DataFrame(data=expected_data)
#
#    pdt.assert_frame_equal(original, expected)

def test_calc_new_column():
    original_data = {'a': [1, 1, 5], 
                     'b': [5, 5, 6]}
    original = pd.DataFrame(data=original_data)

    expected = pd.Series([6, 6, 11], name='sum')

    subject = t.ETL()
    actual = subject.calc_column(TableGetterStub(original))

    pdt.assert_series_equal(actual['sum'], expected)


### Using UnitTest ###
# Prettier output but it's unittest. Which is better?
# Might prefer getting used to ugly output and using pytest
class TestMethods(unittest.TestCase):

    def test_filters_table(self):
        original_data = {'a': [1, 1, 5], 
                         'b': [5, 5, 6]}
        original = pd.DataFrame(data=original_data)

        
        expected_data = {'a': [1, 1], 
                         'b': [5, 5]}
        expected = pd.DataFrame(data=expected_data)

        subject = t.ETL()
        actual = subject.transform_table(TableGetterStub(original))

        #self.assertEqual(original, expected)
        #self.assertTrue(original.equals(expected))
        pdt.assert_frame_equal(actual, expected)


if __name__ == '__main__':
    unittest.main();
