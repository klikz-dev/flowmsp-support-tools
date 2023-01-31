#!/bin/bash -eu

BASE_URL=https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1

if [ $# -lt 1 ]
then
    printf "Usage: %s [state-name]\n" $(basename $0) >&2
    exit 1
fi

NAME="$1"

cd $HOME/Downloads

while read state_name ; do
    if [ "${NAME}" = "${state_name}" ]
    then
	name=$(echo $state_name | sed -e 's/ //g')
	url=${BASE_URL}/${name}.zip
	outfile=$(basename $url)

	(set -x ; curl -o $outfile $url)
    fi
done <<EOF
Alabama
Alaska
Arizona
Arkansas
California
Colorado
Connecticut
Delaware
District Of Columbia
Florida
Georgia
Hawaii
Idaho
Illinois
Indiana
Iowa
Kansas
Kentucky
Louisiana
Maine
Maryland
Massachusetts
Michigan
Minnesota
Mississippi
Missouri
Montana
Nebraska
Nevada
New Hampshire
New Jersey
New Mexico
New York
North Carolina
North Dakota
Ohio
Oklahoma
Oregon
Pennsylvania
Rhode Island
South Carolina
South Dakota
Tennessee
Texas
Utah
Vermont
Virginia
Washington
West Virginia
wisconsin
Wyoming
EOF

exit 0

https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/DistrictofColumbia.zip

https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/NewHampshire.zip
