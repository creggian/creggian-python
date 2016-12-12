def which(x):
    return [index for index, item in enumerate(x) if item]


def overlaps_any(x, y, x_strand=None, y_strand=None):
    if type(y) == tuple:
        if x_strand is None and y_strand is None:
            return x[1] <= y[2] and y[1] <= x[2] and x[0] == y[0]
        else:
            return x[1] <= y[2] and y[1] <= x[2] and x[0] == y[0] and x[x_strand] == y[y_strand]
    if type(y) == list:
        return any([overlaps_any(x, z, x_strand=x_strand, y_strand=y_strand) for z in y])
    return False


def leftjoin_overlap_bed(x, y, x_strand=None, y_strand=None, o_type="any"):
    """
    :param x: tuple
    :param y: list of tuples
    :param x_strand: integer
    :param y_strand: integer
    :param o_type: string
    :return:
    """
    if o_type == "any":
        idx = which([overlaps_any(x, z, x_strand=x_strand, y_strand=y_strand) for z in y])
    else:
        raise ValueError("'o_type' parameter must be: 'any'")

    if len(idx) == 1:
        return x + y[idx[0]]
    elif len(idx) > 1:
        return [x + y[i] for i in idx]
    else:
        return x


def to_tsv_line(data):
    return '\t'.join(str(d) for d in data)


def split_tsv(x):
    return x.split("\t")


def parse_narrow_peak(x):
    return (x[0], int(x[1]), int(x[2]), x[3], int(x[4]), x[5], float(x[6]), float(x[7]), float(x[8]), int(x[9])) + (x[10:],)


def parse_tfbs_ucsc(x):
    return (x[0], int(x[1]), int(x[2]), x[3], int(x[4])) + (x[5:],)
