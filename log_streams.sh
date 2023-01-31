#!/bin/bash -eu

aws --profile flowmsp-prod logs describe-log-streams \
    --log-group-name FlowServerLogs \
    --query 'logStreams[].[logStreamName, lastEventTimestamp]' \
    --output text | sort -k 2,2rn | \
    head

exit 0

