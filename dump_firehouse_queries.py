#!/usr/bin/env python3

import sys
import argparse
import dbf


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="dump firehouse queries")
    parser.add_argument("dbf_file")
    parser.add_argument("--limit", type=int)

    args = parser.parse_args()

    table = dbf.Table(args.dbf_file)
    table.open()

    hdr = table.field_names

    for i, rec in enumerate(table):
        if args.limit and i >= args.limit:
            break

        print("------------------------------ %d ------------------------------" % (i + 1))
        for h in table.field_names:
            print(h, rec[h])
