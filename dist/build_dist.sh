#!/bin/bash

ACCOUNT=$1
ACC_DIR="./precog_platform-$ACCOUNT"

mkdir $ACC_DIR
cp README.md $ACC_DIR

cp version $ACC_DIR
cp config $ACC_DIR
cp common.sh $ACC_DIR
cp setup.sh $ACC_DIR
cp run.sh $ACC_DIR

cp -r ijson $ACC_DIR
cp import $ACC_DIR
cp export $ACC_DIR
cp strip_array.py $ACC_DIR

cp -r lib $ACC_DIR
cp -r templates $ACC_DIR

mkdir $ACC_DIR/artifacts
cp -r artifacts/* $ACC_DIR/artifacts

mkdir $ACC_DIR/dump
cp -r account_dumps/$ACCOUNT/* $ACC_DIR/dump/

tar cvfz precog_platform-$ACCOUNT.tgz $ACC_DIR
