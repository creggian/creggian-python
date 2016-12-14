import unittest
from creggian.main import *


class TestMain(unittest.TestCase):

    def test_which(self):
        a = [True, True, False]
        b = [True]
        self.assertEqual(which(a), [0,1])
        self.assertEqual(which(b), [0])

    def test_bin_coords(self):
        self.assertEqual(bin_coords('chr4', 10, 20, bin_size=100), ['4.0'])
        self.assertEqual(bin_coords('chrX', 10, 200, bin_size=100), ['X.0', 'X.1', 'X.2'])
        self.assertEqual(bin_coords('chrQ', 10, 20, bin_size=3), ['Q.3', 'Q.4', 'Q.5', 'Q.6'])

    def test_to_kv(self):
        a = ('chr1', 10, 20)
        self.assertEqual(to_kv(a, bin_size=10), [('1.1', a), ('1.2', a)])
        self.assertEqual(to_kv(a, bin_size=100), [('1.0', a)])

    def test_to_tsv_line(self):
        self.assertEqual(to_tsv_line(('my', 'name', 'is')), 'my\tname\tis')

    def test_flatten_list(self):
        a = [('chr1', 10, 20), 1, 'str', [1, 2, 'test']]
        self.assertEqual(flatten_list(a), [('chr1', 10, 20), 1, 'str', 1, 2, 'test'])


if __name__ == '__main__':
    unittest.main()