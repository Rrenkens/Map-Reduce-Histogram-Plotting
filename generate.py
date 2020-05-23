#!/usr/bin/env python3

import argparse
import random


def create_test_file(number_of_line, file_name):
    random.seed()
    with open(file_name, 'w') as f:
        for _ in range(number_of_line):
            f.write("{0:.8f}\t\"\"\n".format(random.random()))


def parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', help='Number of lines')
    parser.add_argument('-d', help='File name')
    return parser.parse_args()


if __name__ == "__main__":
    args = parse()
    if args.n and args.d:
        create_test_file(int(args.n), args.d)
    else:
        raise AttributeError("Incorrect number of argument")
