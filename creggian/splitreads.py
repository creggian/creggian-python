from creggian.bed import *


def max_y(qstart, qend, sstart, send, y):
    """
    retrieve the max 'y' value of those subject regions
    overlapping with the query region

    :param qstart:
    :param qend:
    :param sstart:
    :param send:
    :param y:
    :return:
    """
    if (len(sstart) != len(send)) or (len(sstart) != len(y)):
        raise RuntimeError('max_y function: sstart, send and y parameters must have same length')

    if (len(qstart) != len(qend)) or (len(qstart) != 1):
        raise RuntimeError('max_y function: qstart and qend parameters must have length 1')

    sro_query_bool = overlaps_any2(sstart, send, qstart, qend)
    return max([y[idx] for idx, value in enumerate(sro_query_bool) if value])


def reduced_regions(split_reads, qchr, qstart, qend, minypeak):
    """
    'split_reads' it is a collection of split read regions. Because they
    come from multiple reads (that have different mapping), this function
    reduces overlapping regions into a large one.

    I suggest to append to every tuple the BAM filename from which this
    reduced region (and hence reads) come from and 'minypeak' value. These
    two information will make the tuple unique among different BAM files
    and parameter settings (i.e. minypeak).

    :param split_reads:     [(chr, start, end), (chr, start, end), ...]
    :param qchr:            string
    :param qstart:          integer
    :param qend:            integer
    :param minypeak:        integer
    :return:                [(qchr, qstart, qend, max_y_query, schr, sstart, send, max_y_subject)]
    """
    reduced = []
    if len(split_reads) <= 0:
        return reduced

    split_starts = [x[1] for x in split_reads]
    split_ends = [x[2] for x in split_reads]

    sro_start, sro_end = disjoint(split_starts, split_ends)
    y = count_overlaps(sro_start, sro_end, split_starts, split_ends)

    if not all([(sro_end[idx] - sro_start[idx]) >= 0 for idx in range(0, len(sro_start))]):
        raise Exception('not all TRUE ERROR !')

    max_y_query = max_y([qstart], [qend], sro_start, sro_end, y)

    # data_raw = [(start, end, width, y), (start, end, width, y), ...], TODO: do we use width value?
    data_raw = zip(sro_start, sro_end, [sro_end[idx] - sro_start[idx] + 1 for idx in range(0, len(sro_start))], y)

    # remove low level transcription, i.e. noise, so reduced regions are well defined
    data = [x for x in data_raw if x[3] > minypeak]

    if len(data) > 1:
        adjacent = [data[idx][1] + 1 == data[idx + 1][0] for idx, value in enumerate(data[0:(len(data) - 1)])]
        gaps = which([not x for x in adjacent])

        peak_start = 0
        if len(gaps) > 0:
            for gap in gaps:
                region = (qchr, data[peak_start][0], data[gap][1])
                max_y_region = max_y([region[1]], [region[2]], sro_start, sro_end, y)
                peak_start = gap + 1
                reduced += [(qchr, qstart, qend, max_y_query) + region + (max_y_region, )]

        region = (qchr, data[peak_start][0], data[-1][1])
        max_y_region = max_y([region[1]], [region[2]], sro_start, sro_end, y)
        reduced += [(qchr, qstart, qend, max_y_query) + region + (max_y_region, )]

    if len(data) == 1:
        region = (qchr, data[0][0], data[-1][1])
        max_y_region = max_y([region[1]], [region[2]], sro_start, sro_end, y)
        reduced += [(qchr, qstart, qend, max_y_query) + region + (max_y_region, )]

    return reduced


def get_split_reads(read_chr, read_start, read_end, read_cigar, qchr, qstart, qend):
    """
    'read_start' and 'read_end' scalar values represent the
    beginning and the end of the (paired-end) read. The
    difference between the two values includes the mapped
    parts and the unmapped partes in betweens. In order to
    retrieve only the mapped regions, this function is split
    the CIGAR string of the BAM read.

    Target coordinates are required because some reads pass
    through the target region without splicing into it (i.e.
    the mapped regions start before and end afterwards,
    without overlapping the target region).

    :param read_chr:    read contig name (i.e. chr)
    :param read_start:  read start
    :param read_end:    read end
    :param read_cigar:  read cigar
    :param qchr:        target contig name
    :param qstart:      target start
    :param qend:        target end
    :return:            [(chr, start, end), (chr, start, end), ...]
    """
    import re
    from itertools import compress

    read_cigar_split = re.findall('[0-9]+[A-Z]{1}', read_cigar)

    # remove S tags from CIGAR and update 5' or 3'
    #
    # soft-clipped nucleotides are located before the start position (https://www.biostars.org/p/81261/)
    # therefore, for the purpose of finding the split-reads I can simply remove them from the CIGAR string
    cigar_not_s_bool = [not re.findall('(S$)', x) for x in read_cigar_split]
    read_cigar_split = list(compress(read_cigar_split, cigar_not_s_bool))

    cigar_not_i_bool = [not re.findall('(I$)', x) for x in read_cigar_split]
    read_cigar_split = list(compress(read_cigar_split, cigar_not_i_bool))

    read_cigar_split_letters = [x[len(x) - 1] for x in read_cigar_split]

    cigar_letters_valid = ['D', 'M', 'N']
    if not all([x in cigar_letters_valid for x in read_cigar_split_letters]):
        raise Exception("cigar_letters_valid !! " + str(read_cigar))

    def cigar_get_int(x):
        return int(re.findall('[0-9]+', x)[0])

    def cigar_get_letter(x):
        return str(re.findall('[A-Z]+', x)[0])

    read_splits_chr = [read_chr]
    read_splits_start = [read_start]
    read_splits_end = []

    for idx in range(0, len(read_cigar_split)):
        cigar_item = read_cigar_split[idx]
        cigar_int = cigar_get_int(cigar_item)
        cigar_let = cigar_get_letter(cigar_item)

        if cigar_let == 'D':
            read_splits_start[-1] += cigar_int

        if cigar_let == 'M':
            read_splits_end += [read_splits_start[-1] + cigar_int]
            read_splits_start += [read_splits_start[-1] + cigar_int]
            read_splits_chr += [read_chr]

        if cigar_let == 'N':
            read_splits_start[-1] += cigar_int

    read_splits_chr = read_splits_chr[0:(len(read_splits_chr) - 1)]
    read_splits_start = read_splits_start[0:(len(read_splits_start) - 1)]
    read_splits_start = [x + 1 for x in read_splits_start]

    # 'qstart' and 'qend' parameters are required to filter out
    # those reads that pass through, hence without splicing, the target region
    ret = zip(read_splits_chr, read_splits_start, read_splits_end)
    if not overlaps_any2((qstart,), (qend,), read_splits_start, read_splits_end)[0]:
        ret = []

    return ret
