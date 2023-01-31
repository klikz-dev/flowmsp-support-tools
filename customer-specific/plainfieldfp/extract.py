#!/usr/bin/env python3

import sys
import glob
from openpyxl import load_workbook
#import xlrd
import csv
import collections

def xlsx_DictReader(filename):
    wb = load_workbook(filename)
    s = wb.worksheets[0]
    rows = s.rows
    raw_hdr = next(rows)
    hdr = []

    for rh in raw_hdr:
        if not rh.value:
            break
        hdr.append(rh.value)

    
    for r in rows:
        orec = collections.OrderedDict()

        for i, h in enumerate(hdr):
            orec[h] = r[i]

        yield orec

    return


def get_address(r):
    address = "{number} {st_prefix} {street} {st_type} {st_suffix}".format(
        number=r["number"].value or "",
        st_prefix=r["st_prefix"].value or "",
        street=r["street"].value or "",
        st_type=r["st_type"].value or "",
        st_suffix=r["st_suffix"].value or ""
    ).strip()

    return address.lower()


def get_row_status(r):
    c = r["occ_name"]
    cell = "{c}{r}".format(c=c.column_letter, r=c.row)

    # blue row
    if c.fill.bgColor.value == 64:
        # print("KEEP", cell, c.value)
        return "KEEP"

    # red row
    elif c.style == "Bad":
        # print("DROP", cell, c.value)
        return "DROP"
    else:
        # print("NORMAL", cell, c.value)
        return "NORMAL"


def get_cell(r):
    c = r["occ_name"]
    cell = "{c}{r}".format(c=c.column_letter, r=c.row)

    return cell


def make_contact(irec):
    fname = irec["first_name"].value or ""
    lname = irec["last_name"].value or ""

    return " ".join([fname, lname]).strip()


def make_phone(irec):
    phone = irec["phone"].value or ""
    ext = irec["ext"].value or ""

    return " ".join([phone, ext]).strip()


# dump group of related records
def dump_records(w, records, addr):
    # status, addr = get_row_status(r)
    begin = get_cell(records[0])
    end = get_cell(records[-1])
    crng = "{b}:{e}".format(b=begin, e=end)
    disp = {"NORMAL": [], "KEEP": [], "DROP": []}

    for r in records:
        status = get_row_status(r)

        disp[status].append(r)

    # sanity checks
    if len(disp["NORMAL"]) > 1:
        print("WARN", crng, addr, "normal status but recset > 1", file=sys.stderr)

    if len(disp["KEEP"]) > 1:
        print("WARN", crng, addr, "more than one keep in recset", file=sys.stderr)

    if len(disp["KEEP"]) == 0 and len(disp["DROP"]) > 0:
        print("WARN", crng, addr, "no KEEP in recset", file=sys.stderr)

    notes = []

    for r in disp["DROP"]:
        contact = make_contact(r)
        phone = make_phone(r)
    
        note = "{b}: {n} {p}".format(b=r["occ_name"].value, n=contact, p=phone)
        notes.append(note)

    for r in disp["NORMAL"] + disp["KEEP"]:
        orec = {"notes": "|".join(notes)}

        for k in r:
            orec[k] = r[k].value or ""
 
        w.writerow(orec)



    return


if __name__ == "__main__":
    rows = xlsx_DictReader(sys.argv[1])
    w = None
    recset = []
    paddr = None

    for rownum, r in enumerate(rows):
        if not w:
            hdr = list(r.keys()) + ["notes"]
            w = csv.DictWriter(sys.stdout, hdr, lineterminator="\n")
            w.writeheader()

        addr = get_address(r)

        # dump records if new group
        if paddr and addr != paddr:
            dump_records(w, recset, paddr)
            recset = []

        recset.append(r)
        paddr = addr
    # end for

    dump_records(w, recset, paddr)

