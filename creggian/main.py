def which(x):
    return [index for index, item in enumerate(x) if item]


def coords2bin(chrom, start, end, bin_size=10000):
    """
    chrom: string
    start: int
    end: int

    hg19 length = 3,137,161,264
    chrom sizes = https://genome.ucsc.edu/goldenpath/help/hg19.chrom.sizes
    """
    key = chrom[3:]  # 'chr1' => '1'
    min_bin = start / bin_size
    max_bin = end / bin_size
    r = range(min_bin, max_bin + 1)

    return [key + "." + str(x) for x in r]


def bin2coords(bin, bin_size=10000):
    chrom_id, bin_num_str = bin.split(".")

    chrom = "chr" + chrom_id
    bin_num = int(bin_num_str)

    start = bin_num * bin_size
    end = (((bin_num + 1) * bin_size) - 1)

    return chrom, start, end


def to_kv(x, bin_func=coords2bin, bin_size=10000):
    x_key = bin_func(x[0], x[1], x[2], bin_size)
    return [(k, x) for k in x_key]


def to_tsv_line(data):
    return '\t'.join(str(d) for d in data)


def flatten_list(l):
    ret = []
    for elem in l:
        if type(elem) is list:
            for elem2 in elem:
                ret = ret + [elem2]
        else:
            ret = ret + [elem]
    return ret
