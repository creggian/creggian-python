def split_tsv(x):
    return x.split("\t")


def split_cage_entry(x):
    coords, strand = x.split(",")
    chrom, start_end = coords.split(":")
    start, end = start_end.split("..")
    return chrom, int(start), int(end), strand


def parse_narrow_peak(x):
    return (x[0], int(x[1]), int(x[2]), x[3], int(x[4]), x[5], float(x[6]), float(x[7]), float(x[8]), int(x[9])) + tuple(x[10:],)


def parse_tfbs_ucsc(x):
    return (x[0], int(x[1]), int(x[2]), x[3], int(x[4])) + tuple(x[5:],)