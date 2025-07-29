import unittest
from calculator import Calculator

class TestCalculations(unittest.TestCase):
    def setUp(self):
        self.operator = Calculator(a=8,b=2)
    
    def test_sum(self):
        self.assertEqual(self.operator.get_sum(),10,'The calculation is wrong')

    def test_product(self):
        self.assertEqual(self.operator.get_product(),20,'The calculation is wrong')


def tearDown(self):
    print('All tests ran. Goodbye.')

if __name__ == '__main__':
    unittest.main()