#!/bin/bash

set -e

mkdir -p ./build/package
cp -R ./build/src/* ./build/package

cp \
  ./package.json \
  ./README.md \
  ./LICENSE \
  ./build/package

sed -i 's/"private": true/"private": false/g' ./build/package/package.json
