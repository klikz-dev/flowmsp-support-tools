#!/bin/bash -eu

get_functions ()
{
    aws --profile $PROFILE_NAME lambda list-functions --query 'Functions[].[FunctionName]' --output text
}

#------------------------------------------------------------
# main
#------------------------------------------------------------
if [ $# -lt 2 ]
then
    printf "Usage: %s [aws-profile] [function-name]\n" $(basename $0) >&2
    exit 1
fi

PROFILE_NAME=$1 FUNCTION_NAME=$2

case $PROFILE_NAME in
    flowmsp-prod|flowmsp-dev)
    ;;

    *)
	printf "%s: unrecognized profile name\n" $PROFILE_NAME >&2
	exit 1
esac

location=$(aws --profile $PROFILE_NAME lambda get-function --function-name $FUNCTION_NAME \
	       --query Code.Location --output text)

curl -s $location > ${PROFILE_NAME}.${FUNCTION_NAME}.zip

echo function saved to ${PROFILE_NAME}.${FUNCTION_NAME}.zip

exit 0

