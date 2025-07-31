import unittest
from calculator import Calculator
class TestOperations(unittest.TestCase):
    def test_sum(self):
        mycal = Calculator(2,2)
        answer = mycal.get_sum()
        self.assertEqual(answer,4, 'The sum is wrong')

    def test_difference(self):
        mycal = Calculator(2,2)
        answer = mycal.get_difference()
        self.assertEqual(answer,0, 'The sum is wrong')

    def test_product(self):
        mycal = Calculator(2,2)
        answer = mycal.get_product()
        self.assertEqual(answer,4, 'The sum is wrong')

    def test_quotient(self):
        mycal = Calculator(2,2)
        answer = mycal.get_quotient()
        self.assertEqual(answer,1, 'The sum is wrong')


if __name__ == '__main__':
    unittest.main()