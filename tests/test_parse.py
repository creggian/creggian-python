import unittest
from creggian.parse import *


class TestParse(unittest.TestCase):
    def test_split_tsv(self):
        self.assertEqual(split_tsv('my\tname\tis'), ['my', 'name', 'is'])

    def test_split_cage_entry(self):
        self.assertEqual(split_cage_entry('chr10:100180637..100180657,-'), ('chr10', 100180637, 100180657, '-'))

    def test_parse_narrow_peak(self):
        entry = (u'chr1',
                 u'839806',
                 u'840204',
                 u'test_peak_6',
                 u'42',
                 u'.',
                 u'4.07729',
                 u'6.25019',
                 u'4.29897',
                 u'206',
                 u'Brain_Angular_Gyrus',
                 u'Histone_H3K4me3',
                 u'BI.Brain_Angular_Gyrus.H3K4me3.149.bed.gz.pp.bed',
                 u'BI.Brain_Angular_Gyrus.Input.149.bed.gz.pp.bed')

        entry_parsed = (u'chr1',
                        839806,
                        840204,
                        u'test_peak_6',
                        42,
                        u'.',
                        4.07729,
                        6.25019,
                        4.29897,
                        206,
                        u'Brain_Angular_Gyrus',
                        u'Histone_H3K4me3',
                        u'BI.Brain_Angular_Gyrus.H3K4me3.149.bed.gz.pp.bed',
                        u'BI.Brain_Angular_Gyrus.Input.149.bed.gz.pp.bed')

        self.assertEqual(parse_narrow_peak(entry), entry_parsed)

    def test_parse_tfbs_ucsc(self):
        entry = (u'chr1', u'839806', u'840204', u'name', u'845')
        entry_parsed = (u'chr1', 839806, 840204, u'name', 845)

        self.assertEqual(parse_tfbs_ucsc(entry), entry_parsed)
