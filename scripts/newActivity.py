#!/usr/bin/env python

from datetime import datetime
from pymongo import MongoClient
import re
from subprocess import call
import sys

# minutes
window = 30

if len(sys.argv) != 2:
    print 'Usage: %s <logfile>' % sys.argv[0]
    sys.exit(1)

now = datetime.now()

logformat = re.compile('(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d).*Created new projection.*?ColumnDescriptor\(/(\d{10})/.*')

active = set()

with open(sys.argv[1]) as input:
    for line in input:
        hit = logformat.match(line)
        if hit:
            timestamp = datetime.strptime(hit.group(1), '%Y-%m-%d %H:%M:%S')
            if abs((now - timestamp).seconds) < 60*window:   # 30 minute window for now
                active.add(hit.group(2))
                #print 'Activity at %s for %s: %s' % (hit.group(1), hit.group(2), hit.group(0))

if len(active) > 0:
    print 'Active accounts with new columns in the last %d minutes:' % window

    conn = MongoClient('localhost', 27017)

    accounts = {}

    for acct in conn['accounts_v1_2_2']['accounts'].find():
        accounts[acct['accountId']] = acct['email']

    for acct in sorted(list(active)):
        print '  %s (%s)' % (acct, accounts[acct])

    conn.close()
