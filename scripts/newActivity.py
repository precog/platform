#!/usr/bin/env python

from datetime import datetime
from pymongo import MongoClient
import re
from subprocess import call
import sys

# minutes
window = 60

if len(sys.argv) != 2:
    print 'Usage: %s <logfile>' % sys.argv[0]
    sys.exit(1)

now = datetime.now()

# jdbm: 2013-02-11 12:09:16,980 [patcher-75] I c.p.y.m.FileMetadataStorage {} - Created new projection for ProjectionDescriptor(1,List(ColumnDescriptor(/0000000066/patheerdb/jobs/,.PositionHeader.Competency[16].@name,CString,Authorities(Set(0000000066)))))
#logformat = re.compile('(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d).*Created new projection.*?ColumnDescriptor\(/(\d{10})/.*')

# nihdb: 2013-03-01 13:10:36,564 [atcher-563] D c.p.n.NIHDBActor {} - Inserting 1 rows for 0:57776977 at /srv/nihdb/data/data/0000000066/patheerdb/jobs
#logformat = re.compile('(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d).*Inserting \d+ rows for \d+:\d+ at /srv/nihdb/data/data/(\d{10})/.*')

# currentnewdb: 2013-03-18 15:00:11,171 [tcher-2242] D c.p.n.NIHDBActor {} - Inserting 8 rows, skipping 0 rows at offset 16420452320 for /srv/precog/shard-beta/data/0000000840/APIAnalytics/events/ANALYTICS_TYPE_MISC_EVENT/perAuthProjections/d88dcee23d1aaeade620bbe07e2f2c71d0bba9e2
logformat = re.compile('(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d).*Inserting \d+ rows, skipping \d+ rows at offset \d+ for /srv/shard/data/data/(\d{10})/.*')

active = set()
filtered = set(['0000000056', '0000000069', '0000000070', '0000000071'])

with open(sys.argv[1]) as input:
    for line in input:
        hit = logformat.match(line)
        if hit:
            timestamp = datetime.strptime(hit.group(1), '%Y-%m-%d %H:%M:%S')
            if abs((now - timestamp).seconds) < 60*window:   # 60 minute window for now
                active.add(hit.group(2))
                #print 'Activity at %s for %s: %s' % (hit.group(1), hit.group(2), hit.group(0))

toReport = sorted(list(active - filtered))

if len(toReport) > 0:
    print 'Active accounts with new columns in the last %d minutes:' % window

    conn = MongoClient('localhost', 27017)

    accounts = {}

    for acct in conn['accounts_v1_2_2']['accounts'].find():
        accounts[acct['accountId']] = acct['email']

    for acct in toReport:
        print '  %s (%s)' % (acct, accounts[acct])

    conn.close()
