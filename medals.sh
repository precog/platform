#!/bin/bash

java \
  -cp ragnarok/target/ragnarok-assembly-*.jar \
  com.precog.ragnarok.test.MedalsTestSuite \
  --root-dir /home/miles/projects/eclipse/precog-2.9.2/workspace/platform/jprofiler/jprofiler.db --runs 2 --dry-runs 1 --timeout 28800 --json

