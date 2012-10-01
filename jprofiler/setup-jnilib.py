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

print "getting jnilib"
if exists(jnilib_path): exit(0)

archs = {'Darwin': 'osx.jnilib', 'Linux': 'linux.jnilib'}
name = archs.get(platform.system())
if name is None: die("unknown arch: %s" % arch)

u = urlopen("http://s3.amazonaws.com/ops.reportgrid.com/jprofiler/%s" % name)
open(jnilib_path, 'wb').write(u.read())
