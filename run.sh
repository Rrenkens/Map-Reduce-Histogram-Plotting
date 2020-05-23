#!/bin/bash

g++ map.cpp -o map
g++ reduce.cpp -o reduce
g++ mapreduce.cpp -o mapreduce -lboost_system -lboost_filesystem

chmod +x create_histogram.py

./mapreduce map ./map data/input.txt data/map_output.txt 7
./mapreduce reduce ./reduce data/map_output.txt data/reduce_output.txt 7
./create_histogram.py -n 7 -i data/reduce_output.txt -o data/example
