#!/bin/bash

java \
  -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -cp ragnarok/target/ragnarok-assembly-2.0.1-SNAPSHOT.jar \
  com.precog.ragnarok.test.AthletesTestSuite \
  --root-dir /home/miles/projects/eclipse/precog-2.9.2/workspace/platform/jprofiler/jprofiler.db --runs 1 --dry-runs 0 --timeout 28800 --json
