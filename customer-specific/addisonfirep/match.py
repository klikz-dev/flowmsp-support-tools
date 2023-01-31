#!/usr/bin/env python3

import sys
import argparse
import json
import pprint
import geojson
import shapely
import shapely.geometry


def within_polygon(loc, polygon):
    # polygon = shapely.geometry.asShape(feature["geometry"])

    return polygon.contains(loc["centroid"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("existing_locations")
    parser.add_argument("traced_polygons")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--matches", action="store_true")
    args = parser.parse_args()

    locations = json.load(open(args.existing_locations))
    traced = geojson.load(open(args.traced_polygons))

    print("traced=%d" % len(traced["features"]), file=sys.stderr)
    print("locations=%d" % len(locations), file=sys.stderr)

    for loc in locations:
        polygon = shapely.geometry.asShape(loc["geoOutline"])
        loc["centroid"] = polygon.centroid
        loc["matches"] = 0

    inrecs = num_matches = skipped = 0

    features = []

    for tnum, t in enumerate(traced["features"]):
        inrecs += 1

        if args.limit and tnum >= args.limit:
            break

        props = t.get("properties", {})
        tags = t["properties"]["@ns:com:here:xyz"]["tags"]

        keep = True
        
        for tag in 'country@usa', 'state@il', 'county@dupage', 'city@addison', 'postalcode@60101', 'illinois':
            if tag not in tags:
                keep = False

        if keep is False:
            skipped += 1
            continue


        # print("------------------------------------------------------------")
        # print(props)

        polygon = shapely.geometry.asShape(t["geometry"])

        matches = []
        for loc in locations:
            if within_polygon(loc, polygon):
                loc["matches"] += 1
                matches.append(loc)

        if matches:
            num_matches += 1

        if args.matches:
            if matches:
                print("traced", props)

                for m in matches:
                    print("  ", "location", m.get("address"))
        else:
            if not matches:
                print("traced", props)

    print("inrecs=%d skipped=%d num_matches=%d" % (inrecs, skipped, num_matches), file=sys.stderr)

    # not found
    for loc in locations:
        if loc["matches"] == 0:
            print("nomatch", loc.get("address"), file=sys.stderr)
        

                          
