import json
import sys


def main():
    if len(sys.argv) > 1:
        f = open(sys.argv[1], "r")
    else:
        f = sys.stdin
    run = json.load(f)
    for test in run:
        if test[u'delta'] != 'insignificant' and u'query' in test:
            print u"--------------------------------------------------------------"
            print test[u'query']
            print u"Was: %s" % test[u'baseline'][u'mean']
            print u"Now: %s" % test[u'stats'][u'mean']
            print u"Change: %s" % test[u'delta']
            print
            print

if __name__ == "__main__":
    main()
