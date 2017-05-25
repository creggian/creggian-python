import unittest
from creggian.main import *


class TestMain(unittest.TestCase):

    def test_which(self):
        a = [True, True, False]
        b = [True]
        self.assertEqual(which(a), [0,1])
        self.assertEqual(which(b), [0])

    def test_coords2bin(self):
        self.assertEqual(coords2bin('chr4', 10, 20, bin_size=100), ['4.0'])
        self.assertEqual(coords2bin('chrX', 11, 200, bin_size=100), ['X.0', 'X.1', 'X.2'])
        self.assertEqual(coords2bin('chrQ', 8, 20, bin_size=3), ['Q.2', 'Q.3', 'Q.4', 'Q.5', 'Q.6'])
        self.assertEqual(coords2bin('chrQ', 9, 20, bin_size=3), ['Q.3', 'Q.4', 'Q.5', 'Q.6'])
        self.assertEqual(coords2bin('chrQ', 10, 20, bin_size=3), ['Q.3', 'Q.4', 'Q.5', 'Q.6'])
        self.assertEqual(coords2bin('chrQ', 11, 20, bin_size=3), ['Q.3', 'Q.4', 'Q.5', 'Q.6'])
        self.assertEqual(coords2bin('chrQ', 12, 20, bin_size=3), ['Q.4', 'Q.5', 'Q.6'])
        self.assertEqual(coords2bin('chrQ', 8, 21, bin_size=3), ['Q.2', 'Q.3', 'Q.4', 'Q.5', 'Q.6', 'Q.7'])
        self.assertEqual(coords2bin('chrQ', 8, 22, bin_size=3), ['Q.2', 'Q.3', 'Q.4', 'Q.5', 'Q.6', 'Q.7'])
        self.assertEqual(coords2bin('chrQ', 8, 23, bin_size=3), ['Q.2', 'Q.3', 'Q.4', 'Q.5', 'Q.6', 'Q.7'])
        self.assertEqual(coords2bin('chrQ', 8, 24, bin_size=3), ['Q.2', 'Q.3', 'Q.4', 'Q.5', 'Q.6', 'Q.7', 'Q.8'])

    def test_bin2coords(self):
        self.assertEqual(bin2coords('1.1', bin_size=10), ('chr1', 10, 19))
        self.assertEqual(bin2coords('1.1', bin_size=100), ('chr1', 100, 199))
        self.assertEqual(bin2coords('X.2', bin_size=100), ('chrX', 200, 299))

    def test_coord2bin2coords(self):
        c2b_list = coords2bin('chr4', 10, 20, bin_size=100)
        self.assertEqual(bin2coords(c2b_list[0], bin_size=100), ('chr4', 0, 99))
        self.assertEqual(bin2coords(c2b_list[0], bin_size=10), ('chr4', 0, 9))

    def test_to_kv(self):
        a = ('chr1', 10, 20)
        self.assertEqual(to_kv(a, bin_size=10), [('1.1', a), ('1.2', a)])
        self.assertEqual(to_kv(a, bin_size=100), [('1.0', a)])

    def test_to_tsv_line(self):
        self.assertEqual(to_tsv_line(('my', 'name', 'is')), 'my\tname\tis')

    def test_flatten_list(self):
        a = [('chr1', 10, 20), 1, 'str', [1, 2, 'test']]
        self.assertEqual(flatten_list(a), [('chr1', 10, 20), 1, 'str', 1, 2, 'test'])

    def test_merged_bin(self):
        bins = ["1.10", "1.11", "1.13", "1.20", "10.10", "10.13", "10.14", "11.21", "12.21"]
        bins.sort()
        self.assertEqual(merged_bin(bins), [('1', 10, 0), ('1', 11, 0), ('1', 13, 1), ('1', 20, 2), ('10', 10, 3), ('10', 13, 4), ('10', 14, 4), ('11', 21, 5), ('12', 21, 6)])

if __name__ == '__main__':
    unittest.main()