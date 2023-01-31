#!/usr/bin/env python3

import os
import sys

messages = """eraymond:TEST MESSAGE; 201 E FRONT ST; GILMAN; THIS IS A TEST MESSAGE NO ACTION IS REQUIRED
MICHAEL:crib fire; 2335 n state route 49; ashkum; 18-140 box level
ASHLEYL:AMBULANCE CALL; 213 N HARTWELL; GILMAN; 66 DAVID TUCKER LIFT ASSISTANCE
TRAVIS:Fire Alarm; 2702 N 1500 East Rd; Clifton; St. John Baptist Church; Smoke Alarm in Community Center; Gilman due with engine, tender, and chief to scene
TRAVIS:Smoke Detector; 519 E 2nd; Gilman; Mildren Schunke Residence; Activated Smoke Detector, no smoke showing
COURTNEY:controlled burn; 601 N MAPLE; GILMAN; per addison brown with city of gilman burning brush pile
COURTNEY:semi rollover; E US HWY 24 / N 1100 EAST RD GILMAN; unknown injuries
COURTNEY:single vehicle on fire; I57 SB MM283;
MICHAEL:possible electrical fire; 624 hwy 24 west; gilman; pd on scene 21-200
COURTNEY:manpower attempt to locate; 106 e 6th; gilman; suicidal subject orgin is residence pd is already on scene subject left northbound on the tracks
MICHAEL:single vehicle accident; mann/maple; gilman; one injury"""

def print_field(k, v, m):
    print("%*.*s: <%s>" % (m, m, k, v.strip()))
    return


def parse_message(msg):
    name, dispatch = msg.split(":", 1)

    flds = dispatch.split(";")
    code = flds.pop(0)
    addr = flds.pop(0)
    status = flds.pop(-1)

    city = business = description = ""

    if len(flds) == 3:
        city, business, description = flds

    elif len(flds) == 2:
        city, business = flds

    elif len(flds) == 1:
        city = flds[0]

    m = max([len(n) for n in ["name", "code", "addr", "city", "business", "description", "status"]])
    print("------------------------------------------------------------")
    print_field("name", name, m)
    print_field("code", code, m)
    print_field("addr", addr, m)
    print_field("city", city, m)
    print_field("business", business, m)
    print_field("description", description, m)
    print_field("status", status, m)
    print()

    return


if __name__ == "__main__":
    for line in messages.split("\n"):
        print(line.strip())
        parse_message(line.strip())


        
