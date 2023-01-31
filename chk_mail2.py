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
import pprint
import base64
import boto3
from googleapiclient.discovery import build
from BeautifulSoup import BeautifulSoup as bs

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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("--unread", action="store_true")
    parser.add_argument("--slug")
    parser.add_argument("--from-email")
    parser.add_argument("--to-email")
    parser.add_argument("--subject")
    parser.add_argument("--before")
    parser.add_argument("--body-contains")
    parser.add_argument("--body-not-contains")
    parser.add_argument("--limit", type=int)

    args = parser.parse_args()
    session = boto3.session.Session(profile_name=args.profile)
    fp = create_credentials(session)

    gmail = build('gmail', 'v1')

    query = []

    if args.unread:
        query.append("is:unread")

    if args.from_email:
        query.append("from:%s" % args.from_email)

    if args.to_email:
        query.append("to:%s" % args.to_email)

    if args.subject:
        query.append("subject:%s" % args.subject)

    if args.before:
        query.append("before:%s" % args.before)

    print(query, file=sys.stderr)

    # https://developers.google.com/gmail/api/v1/reference/users/messages/list#examples
    # from:someuser@example.com
    result = gmail.users().messages().list(userId='me', q=" ".join(query)).execute()

    messages = result["messages"]

    while "nextPageToken" in result:
        pt = result["nextPageToken"]
        result = gmail.users().messages().list(userId='me', q=" ".join(query), pageToken=pt).execute()
        messages += result["messages"]

    print("len(messages)=%d" % len(messages), file=sys.stderr)

    for i, m in enumerate(messages):
        if args.limit and i > args.limit:
            break
        
        # print("------------------------------------------------------------")
        message = gmail.users().messages().get(userId="me", id=m["id"], format="full").execute()
        # message = gmail.users().messages().get(userId="me", id=m["id"],
        #                                       format='raw').execute()
        # msg_str = base64.urlsafe_b64decode(message['raw'].encode('ASCII'))
        # print(msg_str)

        # print(message["payload"]["body"]["data"])
        # print(m["id"], message["labelIds"])

        # print(message["payload"].keys())
        #pprint.pprint(message["payload"])

        # print(message["payload"]["mimeType"])

        if "parts" in message["payload"]:
            print("num parts = %d" % len(message["payload"]["parts"]))

        # print("parts", len(message["payload"]["parts"]))

        # print([h["name"] for h in message["payload"]["headers"]])

        # for h in message["payload"]["headers"]:
        #    if h["name"] in ("To", "From", "Subject", "Date"):
        #        print(h["name"], h["value"])

        # pprint.pprint(message["payload"]["body"])
        msg_str = base64.urlsafe_b64decode(message["payload"]["body"]["data"].encode("utf-8"))

        cleantext = bs(msg_str).text

        if args.body_contains:
            if args.body_contains not in cleantext:
                continue

        if args.body_not_contains:
            if args.body_not_contains in cleantext:
                continue

        # print(msg_str)
        # print(m["id"], cleantext.encode("utf-8"))
        print(cleantext.encode("utf-8"))

        if "parts" in message["payload"]:
            for p in message["payload"]["parts"]:
                print("============================================================")
                pprint.pprint(p)

        # print(message["snippet"])
        #print()
        # ans = input("> ")

    raise SystemExit(0)

    h = gmail.users().history().list(userId="me", startHistoryId=id).execute()
    pprint.pprint(h)

        
    
