#!/bin/bash -eu

if [ $# -lt 3 ]
then
    printf "Usage: %s [profile] [ecr-repository] [version]\n\n" $(basename $0) >&2
    printf "move latest tag to [version]\n" >&2
    exit 1
fi

PROFILE=$1 IMAGE=$2 VERSION=$3

MAN=$(aws --profile $PROFILE ecr batch-get-image \
	  --repository-name $IMAGE \
	  --image-ids imageTag=$VERSION \
	  --query 'images[].imageManifest' \
	  --output text)

aws --profile $PROFILE ecr put-image \
    --repository-name $IMAGE \
    --image-tag latest \
    --image-manifest "$MAN"

exit 0

