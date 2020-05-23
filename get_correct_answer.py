#!/usr/bin/env python3

import argparse
from decimal import Decimal, getcontext


def create_answer_file(number_of_column, input_file, output_file):
    step, cur_count = Decimal("1") / Decimal(number_of_column), 0
    range_data, count_of_elements_per_range = list(), dict()
    i = 1

    while step * i <= Decimal("1"):
        range_data.append(step * i)
        i += 1

    for el in range_data:
        count_of_elements_per_range[el] = 0

    with open(input_file, 'r') as file:
        for line in file:
            key, value = map(str, line.split())
            key = Decimal(key)
            for el in range_data:
                if key <= el:
                    count_of_elements_per_range[el] += 1
                    break

    with open(output_file, 'w') as file:
        for el in range_data:
            file.write(str(el) + "\t" + str(count_of_elements_per_range[el]) + "\n")


def parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', help='Count of column')
    parser.add_argument('-i', help='Input file name')
    parser.add_argument('-o', help='Output file name')
    return parser.parse_args()


if __name__ == "__main__":
    getcontext().prec = 8
    args = parse()
    if args.n and args.i and args.o:
        create_answer_file(args.n, args.i, args.o)
    else:
        raise AttributeError("Incorrect number of argument")
