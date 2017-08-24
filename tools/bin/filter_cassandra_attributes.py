#!/usr/bin/env python2

import sys;
from yaml import load, dump, load_all
from cassandra_attributes import *

def main():
    attributes = dict()
    for i in range(1, len(sys.argv)):
        attributes.update(load(open(sys.argv[i], 'r')))
    print dump(dict(filter(lambda (a, b): a in cassandra_attributes, attributes.items())))                   

if __name__ == "__main__":
    main()
