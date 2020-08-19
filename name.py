#!/usr/bin/env python3

import sys

if __name__ == "__main__":
    name  = sys.argv[1]
    print("_".join([x.lower() for x in name.split(" ")]))
