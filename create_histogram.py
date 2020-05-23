#!/usr/bin/env python3

import argparse
import matplotlib.pylab as plt
import warnings

from decimal import Decimal


def decimal_range(x, y, count):
    if count == 1:
        return [x, y]
    ret = []
    step = (y - x) / count

    while x <= y:
        ret.append(x)
        x += step
    ret[len(ret) - 1] = y.quantize(Decimal("1.00"))

    return ret


def create_histogram(input_file, count_of_column, file_name):
    data = dict()
    step = Decimal("1")

    if count_of_column != '1':
        step /= count_of_column

    cur_range = Decimal("0")
    data[cur_range] = 0

    with open(input_file, 'r') as f:
        for line in f:
            a, b = line.split('\t')
            a, b = Decimal(a), int(b)
            while a > cur_range:
                cur_range += step
                data[cur_range] = 0
            data[cur_range] = b

    plt.title("Histogram")
    plt.xlabel('number of values')
    plt.ylabel('value range')
    plt.xlim([-0.1, 1.1])
    ticks = None

    if len(data.keys()) <= 13:
        ticks = list(data.keys())
        ticks.insert(0, Decimal("0"))
        ticks[len(ticks) - 1] = Decimal("1")
        for i in range(len(ticks)):
            ticks[i] = ticks[i].quantize(Decimal("1.00"))
    else:
        ticks = decimal_range(Decimal("0"), Decimal("1"), 10)

    plt.xticks(ticks)

    plt.bar([el - step / 2 for el in list(data.keys())], data.values(), width=step, color='k')
    plt.savefig(file_name + ".png")


def parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', help='Count of column')
    parser.add_argument('-i', help='Input file name')
    parser.add_argument('-o', help='Output file name')
    return parser.parse_args()


if __name__ == "__main__":
    warnings.simplefilter("ignore")
    args = parse()

    if args.n and args.i and args.o:
        create_histogram(args.i, int(args.n), args.o)
    else:
        raise AttributeError("Incorrect number of argument")

