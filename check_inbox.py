#!/usr/bin/env python

from __future__ import print_function

# for running in a container
# docker run -it ubuntu bash
# apt-get update
# apt-get install python-pip
# pip install --upgrade google-api-python-client
# pip install --upgrade google-cloud-pubsub
import os
import sys
import json
import argparse
import tempfile
import time
import pprint
import boto3
from googleapiclient.discovery import build


def now():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def create_credentials(session):
    creds = {
        "client_id":     get_parm(session, "CLIENT_ID"),
        "client_secret": get_parm(session, "CLIENT_SECRET"),
        "refresh_token": get_parm(session, "REFRESH_TOKEN"),
        "type":          "authorized_user"
        }

    fp = tempfile.NamedTemporaryFile()

    json.dump(creds, fp)

    fp.flush()

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = fp.name

    return fp


def get_messages(gmail, query, nap_time):
    result = gmail.users().messages().list(userId='me', q=" ".join(query)).execute()
    print(result.keys())

    if "messages" in result:
        yield result["messages"]
    else:
        return

    while "nextPageToken" in result:
        if nap_time:
            print("%s: waiting %d seconds" % (now(), nap_time))
            time.sleep(nap_time)

        pt = result["nextPageToken"]
        result = gmail.users().messages().list(userId='me', q=" ".join(query), pageToken=pt).execute()

        yield result["messages"]

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check unread msgs in INBOX")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])

    args = parser.parse_args()
    session = boto3.session.Session(profile_name=args.profile)
    fp = create_credentials(session)

    gmail = build('gmail', 'v1')

    query = ["label:INBOX", "is:unread"]

    print(query, file=sys.stderr)

    num_msgs = 0

    for msgs in get_messages(gmail, query, 1):
        num_msgs += len(msgs)

    print("num_msgs=%d" % num_msgs)

