#!/bin/bash

java \
  -cp ragnarok/target/ragnarok-assembly-*.jar \
  com.precog.ragnarok.test.DisjunctionTestSuite \
  --root-dir /home/miles/projects/eclipse/precog-2.9.2/workspace/platform/jprofiler/jprofiler.db --runs 1 --dry-runs 0 --timeout 28800 --json

