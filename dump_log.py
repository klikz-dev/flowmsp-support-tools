#!/usr/bin/env python3

import os
import sys
import time
import argparse
import pprint
import logging
import boto3

logging.basicConfig()
logger = logging.getLogger("dump_log")
logger.setLevel(logging.INFO)


def ms_to_tstamp(ms):
    t = int(ms/1000)

    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))


def list_log_streams(log_group, prefix=None):
    logs = boto3.client("logs")

    #"orderBy": "LastEventTime",
    args = {"logGroupName": log_group}

    if prefix:
        args["logStreamNamePrefix"] = prefix

    while True:
        r = logs.describe_log_streams(**args)
        logger.debug(r.keys())

        for s in r["logStreams"]:
            if "firstEventTimestamp" not in s:
                continue

            print(s["logStreamName"],
                  ms_to_tstamp(s["firstEventTimestamp"]),
                  ms_to_tstamp(s["lastEventTimestamp"]))

        if "nextToken" not in r:
            break
        
        args["nextToken"] = r["nextToken"]
            

    return


def get_stream_name(job_id):
    b = boto3.client("batch")

    r = b.describe_jobs(jobs=[job_id])

    # get log stream for most recent attempt (this is the -1)
    if r["jobs"][0]["attempts"]:
        return r["jobs"][0]["attempts"][-1]["container"].get("logStreamName")

    else:
        return r["jobs"][0]["container"].get("logStreamName")


def dump_log(log_group, stream_or_job_id, **kwargs):
    logs = boto3.client("logs")

    # should be a regex pattern
    if "-" in os.path.basename(stream_or_job_id):
        stream_name = get_stream_name(stream_or_job_id)
    else:
        stream_name = stream_or_job_id

    args = {
        "logGroupName": log_group,
        "logStreamName": stream_name,
        "startFromHead": True,
        "startTime": 0
    }
    
    ptoken = None

    while True:
        r = logs.get_log_events(**args)

        logger.debug(r.keys())
        logger.debug("len(events)=%d" % len(r.get("events", [])))

        if len(r.get("events", [])) == 0:
            if "follow" not in kwargs:
                break

            logger.debug("waiting")
            time.sleep(4)
            continue

        for e in r["events"]:
            t = e["timestamp"]
            tstamp = ms_to_tstamp(e["timestamp"])
            print(tstamp, e["message"])

        if "nextForwardToken" not in r:
            break

        args["nextToken"] = r["nextForwardToken"]

    return
    

def get_tstamp():
    return time.strftime("%Y-%m-%d %H:%M:%S")
    

def main():
    parser = argparse.ArgumentParser(description="dump batch log")
    parser.add_argument("stream_or_job_id", nargs="*")
    parser.add_argument("--log-group", "-g", default="/aws/batch/job")
    parser.add_argument("--list", "-l", action="store_true")
    parser.add_argument("--follow", "-f", action="store_true")
    parser.add_argument("--prefix", "-p")
    parser.add_argument("--debug", action="store_true")

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    if args.list:
        list_log_streams(args.log_group, args.prefix)
        return

    elif args.stream_or_job_id:
        if args.follow:
            dump_log(args.log_group, args.stream_or_job_id[0], follow=True)
        else:
            dump_log(args.log_group, args.stream_or_job_id[0])
        return


if __name__ == "__main__":
    main()

