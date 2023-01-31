#!/usr/bin/env jython

from __future__ import print_function

import os
import sys
import argparse
import glob
import csv

jardir = "/Users/doug/repos/flowmsp/api-server/target/api-server-2.42.1-deploy-folder"

def add_to_classpath(path):
    for j in glob.glob("%s/*.jar" % path):
        sys.path.append(j)

    return


def d():
    add_to_classpath(jardir)
    add_to_classpath(jardir + "/lib")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="test dispatch parsers")
    parser.add_argument("slug")
    parser.add_argument("email_format")
    parser.add_argument("--outfile")
    parser.add_argument("--boundSWLat", type=float)
    parser.add_argument("--boundSWLon", type=float)
    parser.add_argument("--boundNELat", type=float)
    parser.add_argument("--boundNELon", type=float)

    args = parser.parse_args()

    add_to_classpath(jardir)
    add_to_classpath(jardir + "/lib")

    from com.flowmsp.domain.customer import Customer
    from com.flowmsp.service import MongoUtil
    from com.flowmsp.db import LocationDao
    from com.flowmsp.db import CustomerDao
    from com.flowmsp.db import MessageDao
    from com.flowmsp.db import DebugInfoDao
    from com.flowmsp.service.Message import MessageService
    from com.flowmsp.service.MessageParser import ParserFactory
    from com.flowmsp import SlugContext
    from com.flowmsp.service.Message import MessageResult

    # we should be able to get these from SSM directly
    uri = os.environ["MONGO_URI"]
    map_key = os.environ["GOOGLE_MAP_API_KEY"]

    db = MongoUtil.initializeMongo(uri, "FlowMSP")

    locationDao = LocationDao(db)
    customerDao = CustomerDao(db)
    messageDao = MessageDao(db)
    debugInfoDao = DebugInfoDao(db)

    c = Customer()
    c.slug = args.slug

    if args.boundSWLat:
        c.boundSWLat = args.boundSWLat

    if args.boundSWLon:
        c.boundSWLon = args.boundSWLon

    if args.boundNELat:
        c.boundNELat = args.boundNELat

    if args.boundNELon:
        c.boundNELon = args.boundNELon

    ms = MessageService(customerDao,
                        messageDao,
                        locationDao,
                        debugInfoDao,
                        None,
                        map_key)

    parser = ParserFactory.CreateObject("email", args.email_format)
    SlugContext.setSlug(args.slug);

    if args.outfile:
        w = csv.writer(open(args.outfile, "w"), lineterminator="\n")

        w.writerow(["ErrorFlag",
                    "Code",
                    "latitude",
                    "longitude",
                    "Address",
                    "text"])

    inrecs = 0

    for line in sys.stdin:
        inrecs += 1
        msg = line.strip()

        r = parser.Process(c, msg, ms)

        newRow = MessageResult()

        #newRow.messageID = MessageID;
	#newRow.emailGateway = From;
	#newRow.messageRaw = Body;

        # msgResult.add(newRow)

        if args.outfile:
            w.writerow([r.ErrorFlag,
                        r.Code,
                        r.messageLatLon.coordinates.values[1],
                        r.messageLatLon.coordinates.values[0],
                        r.Address,
                        r.text])
        else:
            print(msg)
            print("ErrorFlag=%s" % r.ErrorFlag)

            try:
                print("Latitude=%s" % r.messageLatLon.coordinates.values[1])
            except:
                print("Latitude=%s" % "na")

            try:
                print("Longitude=%s" % r.messageLatLon.coordinates.values[0])
            except:
                print("Longitude=%s" % "na")
                
            print("location=%s" % r.location)
            print("Code=%s" % r.Code)
            print("Address=%s" % r.Address)
            print("text=%s" % r.text)
            print("------------------------------------------------------------")
        
    print("inrecs=%d" % inrecs, file=sys.stderr)
