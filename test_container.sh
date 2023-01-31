#!/bin/bash -eu

if [ $# -lt 1 ]
then
    printf "Usage: %s [docker-commands...]\n" $(basename $0) >&2
    exit 1
fi

RESULT=$(aws sts get-session-token --query "Credentials.[AccessKeyId, SecretAccessKey, SessionToken]" --output text)

AWS_ACCESS_KEY_ID=$(echo $RESULT | awk '{ print $1 }')
AWS_SECRET_ACCESS_KEY=$(echo $RESULT | awk '{ print $2 }')
AWS_SESSION_TOKEN=$(echo $RESULT | awk '{ print $3 }')
: ${AWS_DEFAULT_REGION=$(aws configure list | grep region | awk '{ print $2 }')}

: ${LAST_DT:=$(date +%Y-%m-%d --date yesterday)}


set -x
docker run -it \
       --rm \
       --env AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
       --env AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
       --env AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN \
       --env AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION \
       "$@"

exit 0

#       --network host \
