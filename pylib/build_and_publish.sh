#!/bin/bash
cp -v ../bin/cqlsh.py cqlshlib/cqlshbin.py
newt package
newt publish
