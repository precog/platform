#!/usr/bin/env python
import json
import sys

def newline_json(in_file, out_file):
    for line in json.load(in_file):
        json.dump(line, out_file)
        out_file.write('\n')

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: python newlinejson.py [path to ordinary json file]"
        sys.exit(1)

    f = open(sys.argv[1], 'r')
    try:
        newline_json(f, sys.stdout)
    finally:
        f.close()
