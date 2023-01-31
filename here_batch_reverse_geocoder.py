#!/usr/bin/env python3

import os
import sys
import csv
import time
import argparse
import requests
import pprint
import xml.etree.ElementTree as ET

# https://developer.here.com/documentation/batch-geocoder/topics/endpoints.html

app_id="WaGiLAKzJQzySaXFJm9o"
app_code="bO4lKri2pJuiCxnRuD0-KQ"
base_url = "https://batch.geocoder.api.here.com/6.2/jobs"

def parse_xml(text, tag):
    root = ET.fromstring(text)

    for e in root.iter(tag):
        #print(e.tag, e.text)
        return e.text

# recId|prox
# 0001|52.505308,13.327739,250
# 0002|49.917257,8.486488,250
# 0003|51.119593,7.399356,250
# 0004|49.20012,8.12326,250
# 0005|51.116845,7.397318,250
def submit_batch(records):
    url = "{b}?app_id={app_id}&app_code={app_code}".format(
        b=base_url,
        app_id=app_id,
        app_code=app_code
    )

    url += "&indelim=%7C&outdelim=%7C&action=run"

    outcols = ["displayLatitude", "displayLongitude", "locationLabel",
               "houseNumber", "street", "district", "city", "postalCode",
               "county", "state", "country"]

    url += "&outcols={cols}".format(cols=",".join(outcols))
    url += "&outputcombined=false"
    url += "&mode=retrieveAddresses"
    url += "&maxresults=1"
    url += "&minresults=1"

    body = ["|".join(r) for r in records]
    data = "\n".join(body)

    # print(url)
    # print(data)
    # raise SystemExit(0)

    r = requests.post(url, data=data)

    return r


def get_status(request_id):
    url = "{b}/{r}?action=status&app_code={c}&app_id={app_id}".format(
        b=base_url,
        r=request_id,
        c=app_code,
        app_id=app_id)

    # print(url)

    r = requests.get(url)

    if r.ok:
        return parse_xml(r.text, "Status")


def get_results(request_id, output="result"):
    url = "{b}/{r}/{output}?app_id={app_id}&app_code={app_code}".format(
        b=base_url,
        output=output,
        r=request_id,
        app_id=app_id,
        app_code=app_code)
        
    url += "&outputcompressed=false"

    # print(url)
    r = requests.get(url)

    if r.ok:
        return r


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    #parser.add_argument("profile", help="aws profile",
    #                    choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("--request-id")
    parser.add_argument("--nap-time", type=int, default=30)
    parser.add_argument("--limit", type=int)

    args = parser.parse_args()

    if args.request_id:
        request_id = args.request_id
        
    else:
        r = csv.DictReader(sys.stdin)
    
        # hdr = ["recId", "prox"]
        hdr = ["prox"]
        records = [hdr]
    
        for rownum, rec in enumerate(r):
            if args.limit and rownum >= args.limit:
                break
    
            # orec = ",".join(["%09d" % rownum, rec["lat"], rec["lon"]])
            orec = [",".join([rec["lat"], rec["lon"], "250"])]
            records.append(orec)
    
        print("inrecs=%d" % len(records), file=sys.stderr)
        r = submit_batch(records)
    
        if not r.ok:
            print("submit failed")
            raise SystemExit(1)
        
        request_id = parse_xml(r.text, "RequestId")

    print("request_id", request_id, file=sys.stderr)
    status = get_status(request_id)

    while status != "completed":
        print("status", status, file=sys.stderr)
        time.sleep(args.nap_time)
        status = get_status(request_id)

    print("status", status, file=sys.stderr)

    r = get_results(request_id)
    print(r.text, end="")

