import unittest
import findspark
from pyspark.context import SparkContext

from creggian.main import *
from creggian.bed import *

findspark.init()


# http://www.nocountryforolddata.com/unit-testing-spark-jobs-in-python/
class TestBed(unittest.TestCase):

    def setUp(self):
        self.sc = SparkContext('local[1]')

    def tearDown(self):
        self.sc.stop()

    def test_overlaps_any(self):

        self.assertEqual(overlaps_any(('chr1', 10, 20), ('chr1', 10, 20)), True)
        self.assertEqual(overlaps_any(('chr1', 10, 20), ('chr2', 10, 20)), False)

        a = [('chr1', 10, 20), ('chr2', 10, 20), ('chr2', 100, 200)]
        b = [('chr11', 10, 20), ('chr2', 50, 150), ('chr2', 150, 160), ('chr2', 155, 265)]

        res_ab = [overlaps_any(x, b) for x in a]
        res_ba = [overlaps_any(x, a) for x in b]

        self.assertEqual(res_ab, [False, False, True])
        self.assertEqual(res_ba, [False, True, True, True])

    def test_leftjoin_overlap(self):

        a = ('chr1', 10, 20)
        b = ('chr1', 15, 25)
        self.assertEqual(leftjoin_overlap(a, [b]), a + b)
        self.assertEqual(leftjoin_overlap(a, [b, b]), [a + b, a + b])
        self.assertEqual(leftjoin_overlap(a, [a, b]), [a + a, a + b])

        a = ('chr2', 100, 200)
        b = [('chr11', 10, 20), ('chr2', 50, 150), ('chr2', 150, 160), ('chr2', 155, 265)]
        self.assertEqual(leftjoin_overlap(a, b), [a + b[1], a + b[2], a + b[3]])

    def test_leftjoin_overlap_window(self):
        rdd1 = self.sc.parallelize([('chr1', 10, 20), ('chr2', 10, 20), ('chr2', 100, 200)])
        rdd2 = self.sc.parallelize([('chr11', 10, 20), ('chr2', 50, 150), ('chr2', 150, 160), ('chr2', 155, 265)])

        result = leftjoin_overlap_window(rdd1, rdd2, bin_func=bin_coords, bin_size=10000)

        output = result.collect()

        # it is not possible to test the exact results for two reasons
        # - the order may vary
        # - the uid assigned to each entry is aleatory
        self.assertEqual(len(output), 5)
        self.assertEqual(all([type(x) is tuple for x in output]), True)  # all elements of output are tuples
        self.assertEqual(sorted([len(x) for x in output]), [4, 4, 8, 8, 8])  # sorted length sizes are deterministic


if __name__ == '__main__':
    unittest.main()