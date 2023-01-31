#!/usr/bin/env python3

import os
import sys
import argparse
import time
import boto3

def convert_tstamp(in_tstamp):
    t = time.gmtime(in_tstamp/1000)
    tstamp = time.strftime("%Y-%m-%d %H:%M:%S UTC", t)
    return tstamp


def get_latest_stream(session, log_group, prefix):
    logs = session.client("logs")

    args = {"logGroupName": log_group,
            "orderBy": "LastEventTime",
            "descending": True,
            "limit": 1}

    if prefix:
        args["logStreamNamePrefix"] = prefix

    r = logs.describe_log_streams(**args)
    return r["logStreams"][0]["logStreamName"]


def dump_log(session, log_group, log_stream, start_tstamp=None):
    logs = session.client("logs")

    args = {"logGroupName": log_group, "logStreamName": log_stream}

    if start_tstamp:
        args["startTime"] = start_tstamp

    r = logs.get_log_events(**args)

    for e in r["events"]:
        #print(convert_tstamp(e['timestamp']), e['message'])
        print(e['message'].strip())

    if len(r["events"]) > 0:
        last_tstamp = r['events'][-1]['timestamp']
    else:
        last_tstamp = start_tstamp

    return last_tstamp

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("group_name")
    parser.add_argument("--prefix")
    parser.add_argument("--stream-name")
    parser.add_argument("--follow", action="store_true")
    parser.add_argument("--start-time")
    parser.add_argument("--sleep-seconds", type=int, default=5)

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)

    stream_name = args.stream_name or get_latest_stream(session, args.group_name, args.prefix)

    print("stream_name=%s" % stream_name, file=sys.stderr)

    last_tstamp = dump_log(session, args.group_name, stream_name)
    #print("last_tstamp=<%s>" % last_tstamp)

    if args.follow:
        pstream = stream_name

        while True:
            time.sleep(args.sleep_seconds)
            stream_name = args.stream_name or get_latest_stream(session, args.group_name, args.prefix)

            if stream_name != pstream:
                print("stream_name=%s" % stream_name, file=sys.stderr)
            
            last_tstamp = dump_log(session, args.group_name, stream_name, last_tstamp+1)
            #print("last_tstamp=<%s>" % last_tstamp)
            pstream = stream_name

    
