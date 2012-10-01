#!/usr/bin/env python

from os import getcwd
from os.path import realpath, join, exists
import platform
from sys import exit
from urllib2 import urlopen

def die(msg): print msg; exit(1)

subproject = join(realpath(getcwd()), "jprofiler")
jnilib_path = join(subproject, "jprofiler.jnilib")

if not exists(subproject): die("couldn't find 'jprofiler' directory")

if exists(jnilib_path): exit(0)

archs = {'Darwin': 'osx.jnilib', 'Linux': 'linux.jnilib'}
name = archs.get(platform.system())
if name is None: die("unknown arch: %s" % arch)

url = "http://s3.amazonaws.com/ops.reportgrid.com/jprofiler/%s" % name
print "getting jnilib from %s" % url

u = urlopen(url)
open(jnilib_path, 'wb').write(u.read())
