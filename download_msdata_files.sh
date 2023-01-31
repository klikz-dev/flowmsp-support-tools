#!/bin/bash -eu

# https://github.com/Microsoft/USBuildingFootprints

DST=s3://flowmsp-prod-doug/Microsoft/USBuildingFootprints

while read state_abbr url ; do
    outfile=$(basename $url)
    gzfile=${state_abbr}.geojson.gz

    echo -- $state_abbr $url $outfile $gzfile --
    curl -s -o $outfile $url
    unzip -p $outfile | gzip > $gzfile
    aws --profile flowmsp-prod s3 cp $gzfile ${DST}/
    rm -f $outfile $gzfile
done <<EOF
al https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Alabama.zip
ak https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Alaska.zip
az https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Arizona.zip
ar https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Arkansas.zip
ca https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/California.zip
co https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Colorado.zip
ct https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Connecticut.zip
de https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Delaware.zip
dc https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/DistrictofColumbia.zip
fl https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Florida.zip
ga https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Georgia.zip
hi https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Hawaii.zip
id https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Idaho.zip
il https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Illinois.zip
in https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Indiana.zip
ia https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Iowa.zip
ks https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Kansas.zip
ky https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Kentucky.zip
la https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Louisiana.zip
me https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Maine.zip
md https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Maryland.zip
ma https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Massachusetts.zip
mi https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Michigan.zip
mn https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Minnesota.zip
ms https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Mississippi.zip
mo https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Missouri.zip
mt https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Montana.zip
ne https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Nebraska.zip
nv https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Nevada.zip
nh https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/NewHampshire.zip
nj https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/NewJersey.zip
nm https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/NewMexico.zip
ny https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/NewYork.zip
nc https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/NorthCarolina.zip
nd https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/NorthDakota.zip
oh https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Ohio.zip
ok https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Oklahoma.zip
or https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Oregon.zip
pa https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Pennsylvania.zip
ri https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/RhodeIsland.zip
sc https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/SouthCarolina.zip
sd https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/SouthDakota.zip
tn https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Tennessee.zip
tx https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Texas.zip
ut https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Utah.zip
vt https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Vermont.zip
va https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Virginia.zip
wa https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Washington.zip
wv https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/WestVirginia.zip
wi https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Wisconsin.zip
wy https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/Wyoming.zip
EOF

exit 0

