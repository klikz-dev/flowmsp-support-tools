#!/usr/bin/env python3

import sys
import json
import pprint

if __name__ == "__main__":
    locations = json.load(sys.stdin)

    auto = locations[0]
    doug = locations[1]

    print("auto", auto["address"])
    pprint.pprint(auto["geoOutline"])
    print()

    print("doug", doug["address"])
    pprint.pprint(doug["geoOutline"])
    print()

    a_geo = auto["geoOutline"]["coordinates"][0]
    d_geo = doug["geoOutline"]["coordinates"][0]

    for a, d in zip(a_geo, d_geo):
        print(a, d, a[0] - d[0], a[1] - d[1])
        
    
