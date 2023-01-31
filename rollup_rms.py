#!/usr/bin/env python3

import sys
import argparse
import collections
import csv

def get_row_status(r):
    c = r["Keep/Delete"]
    status = c.strip().lower()

    if status == "k":
        return "KEEP"

    elif status == "d":
        return "DROP"
    else:
        return "NORMAL"


def get_name(r):
    fname = r.get("first_name", "")
    lname = r.get("last_name", "")

    name = " ".join([fname, lname])

    return name.strip()


# dump group of related records
def dump_records(w, records, addr, begin, end):
    # status, addr = get_row_status(r)
    crng = "{b}:{e}".format(b=begin, e=end)
    disp = {"NORMAL": [], "KEEP": [], "DROP": []}

    for r in records:
        status = get_row_status(r)

        disp[status].append(r)

    #if len(records) > 1:
    #    print(begin, end, records[0]["occ_name"], file=sys.stderr)

    # sanity checks
    if len(disp["NORMAL"]) > 1:
        print("WARN", crng, addr, "normal status but recset > 1", file=sys.stderr)

    if len(disp["KEEP"]) > 1:
        print("WARN", crng, records[0]["occ_name"], "more than one keep in recset", file=sys.stderr)
        for r in records:
            print(r, file=sys.stderr)

    if len(disp["KEEP"]) == 0 and len(disp["DROP"]) > 0:
        print("WARN", crng, addr, "no KEEP in recset", file=sys.stderr)
        print("addr", addr, file=sys.stderr)
        for r in records:
            print(r, file=sys.stderr)

    notes = []

    for r in disp["DROP"]:
        note = "{b}: {n} {p}".format(
            b=r["occ_name"] or "",
            n=get_name(r) or "",
            p=r["phone"] or "")
        notes.append(note)

    for r in disp["NORMAL"] + disp["KEEP"]:
        orec = dict([[k, r[k] or ""] for k in r])

        if notes:
            n = "|".join(notes)

            if "Notes" not in orec:
                orec["Notes"] = ""

            orec["Notes"] += n

        w.writerow(orec)

    return


def get_address(r):
    addr = r["Street Address"].strip()
    addr = addr.lower()
    addr = addr.replace("plz", "plaza")

    return addr
    

def get_group(rownum, r):
    color = get_row_status(r)

    if color == "NORMAL":
        return "NORMAL"
    else:
        return color


def new_group(pgrp, grp):
    if not pgrp:
        return False

    if pgrp == "NORMAL" or grp == "NORMAL":
        return True

    if pgrp in ["KEEP", "DROP"]:
        return False
    
    return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("csv_rms_file")

    args = parser.parse_args()

    fp = open(args.csv_rms_file, errors="ignore")
    rows = csv.DictReader(fp)

    w = None
    recset = []
    paddr = None
    begin = end = 0
    inrecs = outrecs = 0

    for rownum, r in enumerate(rows):
        if not w:
            hdr = list(r.keys()) + ["Notes"]
            w = csv.DictWriter(sys.stdout, hdr, lineterminator="\n")
            w.writeheader()

        addr = get_group(rownum, r)
        #print(rownum, r["occ_name"], addr)

        # dump records if new group
        if paddr and new_group(paddr, addr):
            end = rownum + 1

            #print("processing group")
            #print("------------------------------------------------------------")
            #for r2 in recset:
            #    print([r2[k] for k in r2][:10])
            #print("------------------------------------------------------------")
            
            dump_records(w, recset, paddr, begin, end)
            recset = []
            begin = rownum

        recset.append(r)
        paddr = addr
    # end for

    end = rownum + 1
    dump_records(w, recset, paddr, begin, end)
