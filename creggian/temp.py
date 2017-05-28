from creggian.splitreads import *


def pool_func(args):
    target = args[0]
    param = args[1]

    qchr = target[0]
    qstart = int(target[1])
    qend = int(target[2])

    read_chr, read_start, read_end, read_cigar = param

    ret = []
    if (read_start <= qend) and (read_end >= qstart) and (read_chr == qchr):
        #split_reads = get_split_reads(read_chr, read_start, read_end, read_cigar, qchr, qstart, qend)
        #reduced_meta = [(qchr, qstart, qend) + x for x in split_reads]
        #ret = reduced_meta
        ret = []
    return ret


def bed_key(chr, start, end):
    return str(chr) + "." + str(start) + "." + str(end)


def split_reads_reduce(k, v, filename, minypeak):
    qchr, qstart, qend = k.split(".")

    split_reads = [(x[3], x[4], x[5]) for x in v]
    reduced = reduced_regions(split_reads, qchr, qstart, qend, minypeak)
    reduced_meta = [x + (filename, minypeak) for x in reduced]

    return reduced_meta
