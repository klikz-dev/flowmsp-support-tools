#!/usr/bin/env python3

import sys
import csv
from collections import namedtuple

ohdr = "Polygon,Name,Street Address,Street Address2,City,State,Zip code,Roof Area (Sq. Ft),Occupancy Type,Construction Type,Roof Type,Roof Construction,Roof Material,Normal Population,Sprinklered,Stand Pipe,Fire Alarm,Hours of Operation,Owner Contact,Owner Phone,Notes,Storey Above,Storey Below".split(",")

if __name__ == "__main__":
    r = csv.reader(sys.stdin)
    w = csv.writer(sys.stdout, lineterminator="\n")
    w.writerow(ohdr)

    hdr = next(r)
    RMS = namedtuple("RMS", hdr)

    # 32.902806:-80.664873|32.901022:-80.664616|32.899151:-80.668108|32.902977:-80.668317|32.902806:-80.664873|32.904049:-80.667062
    for rec in r:
        rms = RMS._make(rec)

        orec = ["" for h in ohdr]

        # Name
        orec[1] = rms.Name

        # Street Address
        orec[2] = " ".join([rms.Number, rms.Dir, rms.Street, rms.Type])

        # Street Address2
        orec[3] = rms.Suite

        # City
        orec[4] = "Urbana"

        # State
        orec[5] = "IL"

        # Zip Code, there are really three zip codes
        orec[6] = "61801"

        # save Occup_Id + extra fields as note
        flds = ["NOMATCH", rms.Occup_Id, rms.FPP, rms.LIQ, rms.HAZ, rms.U116, rms.UIUC]

        orec[20] = ",".join([f for f in flds if f])

        w.writerow(orec)
