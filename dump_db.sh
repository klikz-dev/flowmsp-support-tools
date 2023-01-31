#!/bin/bash -eu

get_parm ()
{
    profile=$1 name=$2

    aws --profile=$profile ssm get-parameter --name $name --with-decryption --query 'Parameter.Value' --output text
}

if [ $# -lt 1 ]
then
    printf "Usage: %s [profile]\n" $(basename $0) >&2
    exit 1
fi

PROFILE=$1
: ${MONGO_URI:=$(get_parm $PROFILE mongo_uri)}
YMD=$(date +%Y%m%d)

echo $MONGO_URI

DBHOST=$(echo $MONGO_URI | cut -d @ -f 2 | cut -d / -f 1)
DBUSER=$(echo $MONGO_URI | cut -d : -f 2 | cut -d / -f 3)
DBPASS=$(echo $MONGO_URI | cut -d @ -f 1 | cut -d : -f 3)

echo DBHOST=$DBHOST
echo DBUSER=$DBUSER
echo DBPASS=$DBPASS

(
set -x
mongodump -h $DBHOST \
    -u $DBUSER \
    -p $DBPASS \
    -ssl \
    --out=/tmp/flowMSP_${PROFILE}.${YMD}
)

exit 0
