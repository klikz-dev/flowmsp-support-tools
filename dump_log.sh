#!/bin/bash -eu

#------------------------------------------------------------
# get most recent stream
#------------------------------------------------------------
get_log_stream ()
{
    prefix=$1

    aws --profile flowmsp-prod logs describe-log-streams \
	--log-group-name FlowServerLogs \
	--query 'logStreams[].[logStreamName, lastEventTimestamp]' \
	--output text | sort -k 2,2rn | \
	awk "/$prefix/ { print \$1 }" | \
	head -1
}

#------------------------------------------------------------
# main
#------------------------------------------------------------
if [ $# -lt 1 ]
then
    printf "Usage: %s [api-server or ui-server]\n" $(basename $0) >&2
    exit 1
fi

PREFIX=$1
log_stream=$(get_log_stream $PREFIX)

aws --profile flowmsp-prod logs get-log-events \
    --log-group-name FlowServerLogs \
    --log-stream-name $log_stream \
    --query 'events[].[timestamp, message]' --output text

exit 0



