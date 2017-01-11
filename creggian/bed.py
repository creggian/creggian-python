from creggian.main import *


def overlaps_any(x, y, x_strand=None, y_strand=None):
    """

    :param x: tuple
    :param y: list of tuple
    :param x_strand: integer
    :param y_strand: integer
    :return: boolean
    """
    if type(y) == tuple:
        if x_strand is None and y_strand is None:
            return x[1] <= y[2] and y[1] <= x[2] and x[0] == y[0]
        else:
            return x[1] <= y[2] and y[1] <= x[2] and x[0] == y[0] and x[x_strand] == y[y_strand]
    if type(y) == list:
        return any([overlaps_any(x, z, x_strand=x_strand, y_strand=y_strand) for z in y])
    return False


def leftjoin_overlap(x, y, x_strand=None, y_strand=None, o_type="any"):
    """
    in memory leftjoin
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
        return x + y[idx[0]]  # tuple
    elif len(idx) > 1:
        return [x + y[i] for i in idx]  # list of tuple
    else:
        return x  # tuple


def leftjoin_overlap_window(x, y, x_strand=None, y_strand=None, o_type="any", bin_func=coords2bin, bin_size=10000):
    """
    distributed leftjoin. It has some redundant tuples, because
    regions may fall in several bins.

    x, y are two BED RDDs
    doc: http://spark.apache.org/docs/2.0.2/api/python/pyspark.html

    Example
        def hash_key_x(x):
            key = "."
            if (14 < len(x)):
                key = str(x[14]) # this must exists
            #if (18 < len(x)):
            #    key = key + "." + str(x[18])
            return key

        res = leftjoin_overlap_window(H3K4me3_AD_wCAGE, tfbs)
        res_unique = res \
            .map(lambda x: (hash_key_x(x), x)) \
            .filter(lambda x: x[0] != ".") \
            .reduceByKey(lambda x, y: x) \
            .map(lambda x: x[1][0:14])
    """

    # append unique id, uid
    x_uid = x.zipWithUniqueId().map(lambda x: x[0] + (x[1],))
    y_uid = y.zipWithUniqueId().map(lambda x: x[0] + (x[1],))

    # 0, and 1, are tags for later
    x_kv = x_uid.flatMap(lambda t: to_kv(t, bin_func=bin_func, bin_size=bin_size)).mapValues(lambda t: (0,) + t)
    y_kv = y_uid.flatMap(lambda t: to_kv(t, bin_func=bin_func, bin_size=bin_size)).mapValues(lambda t: (1,) + t)
    u = x_kv.union(y_kv)

    def leftjoin(k, l):
        # get back the original x and y in this window, without tag
        x_orig = [z[1:] for z in l if z[0] == 0]
        y_orig = [z[1:] for z in l if z[0] == 1]

        if len(x_orig) > 0:
            if len(y_orig) > 0:
                res = [leftjoin_overlap(z, y_orig, x_strand=x_strand, y_strand=y_strand, o_type=o_type) for z in x_orig]
                return flatten_list(res)
            else:
                return x_orig  # simple list
        else:
            return []  # simple empty list

    x_leftjoin = u.groupByKey() \
        .flatMap(lambda t: leftjoin(t[0], list(t[1]))) \
        .filter(lambda t: len(t) > 0)

    return x_leftjoin
