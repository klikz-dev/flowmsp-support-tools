#!/usr/bin/env python3

import os
import sys
import argparse
import pprint
import requests
import geojson

base_url = "xx"

url = "https://xyz.api.here.com/hub/spaces?owner=others&access_token=ALDWYTthUIHjDBjlb-zM9A8"

# tags=city@glen_carbon,state@il
def count(tags):
    #base_url = "https://xyz.api.here.com/hub/spaces/R4QDHvd1/count?&tags=postalcode@62040&access_token=ALDWYTthUIHjDBjlb-zM9A8")
    url = "https://xyz.api.here.com/hub/spaces/R4QDHvd1/count?tags={tags}&access_token=ALDWYTthUIHjDBjlb-zM9A8"

    r = requests.get(url.format(tags=tags))

    if r.ok:
        return r.json()

    else:
        print(r.status_code)


# postalcode@62040
def call_xyz(action, tag_list, limit, or_tags):
    base_url = "https://xyz.api.here.com/hub/spaces/R4QDHvd1/{action}?tags={tags}&limit={lim}&access_token=ALDWYTthUIHjDBjlb-zM9A8"

    if or_tags:
        tags = ",".join(tag_list)
    else:
        tags = "+".join(tag_list)

    url = base_url.format(action=action, tags=tags, lim=limit)
    features = []

    r = requests.get(url)

    if not r.ok:
        print(r.status_code, file=sys.stderr)
        return r.ok, features

    results = r.json()
    features += [f for f in results["features"]]

    while "handle" in results:
        print("handle", results["handle"], file=sys.stderr)
        url2 = url + "&handle=%s" % results["handle"]
        r = requests.get(url2)

        if not r.ok:
            print(r.status_code, file=sys.stderr)
            return r.ok, features

        results = r.json()
        features += [f for f in results["features"]]
        
    return r.ok, features
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    #parser.add_argument("action", choices=["count", "search", "iterate"])
    parser.add_argument("tags", nargs="+")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--multi-line", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--limit", type=int, default=15000)
    parser.add_argument("--or-tags", action="store_true",
                        help="combine tags with ORs, not ANDs")

    args = parser.parse_args()

    rc, features = call_xyz("iterate", args.tags, args.limit, args.or_tags)

    if rc:
        collection = geojson.FeatureCollection(features)
        print("found %d features" % len(features), file=sys.stderr)
        geojson.dump(collection, sys.stdout)


# these are both using the MS data
# here xyz show XHmWfTCt  --limit 15 --raw

# this one has the estimated house numbers
#here xyz show R4QDHvd1  --limit 1 --raw --tags "city@glen_carbon state@il"

#for i, j in enumerate(jdata):
#    print(i, "------------------------------------------------------------")
#    pprint.pprint(j)



# r = requests.get("https://xyz.api.here.com/hub/spaces/R4QDHvd1/search?limit=5&access_token=ALDWYTthUIHjDBjlb-zM9A8")
#r = requests.get("https://xyz.api.here.com/hub/spaces/R4QDHvd1/search?limit=5&tags=city@glen_carbon,state@il&access_token=ALDWYTthUIHjDBjlb-zM9A8")
# r = requests.get("https://xyz.api.here.com/hub/spaces/R4QDHvd1/search?&tags=city@glen_carbon,state@il&access_token=ALDWYTthUIHjDBjlb-zM9A8")
# I was expecting about 5000
#jdata = r.json()
#>>> jdata.keys()
#dict_keys(['type', 'etag', 'streamId', 'features'])
#>>> len(jdata["features"])
#30000
#
#we should be able to do something with the above
