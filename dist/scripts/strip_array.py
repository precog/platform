import sys
import ijson

for obj in ijson.items(sys.stdin, 'item'):
    print obj
