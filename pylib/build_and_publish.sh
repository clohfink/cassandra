#!/bin/bash -ex
cp -v ../bin/cqlsh.py cqlshlib/cqlshbin.py
newt package
newt publish
