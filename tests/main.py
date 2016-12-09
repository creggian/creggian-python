import unittest
from creggian.main import *


class TestStringMethods(unittest.TestCase):

    def test_which(self):
        a = [True, True, False]
        b = [True]
        self.assertEqual(which(a), [0,1])
        self.assertEqual(which(b), [0])

    def test_overlaps_any(self):
        a = [True, True, False]
        b = [True]
        self.assertEqual(which(a), [0,1])
        self.assertEqual(which(b), [0])


if __name__ == '__main__':
    unittest.main()