#!/usr/bin/env python3

import os
import sys
import argparse
import boto3
import json
import pprint
import base64
import re
import logging
from pymongo import MongoClient


logger = None

def get_ids(slug, key):
    base, ext = os.path.splitext(key)

    image_id = os.path.basename(base)
    location_id = os.path.basename(os.path.dirname(base))

    return slug, location_id, image_id


def connect(profile):
    if profile == "flowmsp-prod":
        return MongoClient(prod_url)
    else:
        return MongoClient(dev_url)


def print_info(image, keys):
    for k in keys:
        print("  %s=<%s>" % (k, image[k]))

    return


def call_function(profile, slug, location_id, image, function):
    if function == "createAnnotatedImageAndThumbnail":
        payload = {
            "slug": slug,
            "locationId": location_id,
            "imageId": image["_id"],
            "annotationMetadataJSON": json.loads(image["annotationMetadata"])
            }

    elif function == "resize-image":
        url = image["hrefOriginal"]

        _, _, _, b, new_key = url.split("/", 4)

        print("url", url)
        print("b", b)
        print("new_key", new_key)

        payload = {
            "Records": [
                {
                    "s3": {
                        "configurationId": "testConfigRule",
                        "object": {
                            "eTag": "c1e0946156638cad4c6ac699e90f54d2",
                            "sequencer": "0A1B2C3D4E5F678901",
                            "key": new_key,
                            "size": 1024
                        },
                        "bucket": {
                            "arn": "arn:aws:s3:::tuple-source",
                            "name": b,
                            "ownerIdentity": {
                                "principalId": "tuple-demo"
                            }
                        }
                    },
                    "eventName": "ObjectCreated:Put"
                }
            ]
        }
    else:
        print("unsupported lambda function: %s" % function, file=sys.stderr)
        raise SystemExit(1)

    logger.debug(payload)

    sess = boto3.session.Session(profile_name=profile)
    c = sess.client("lambda")

    data = json.dumps(payload).encode()

    runtype = "RequestResponse"
    r = c.invoke(FunctionName=function,
                 InvocationType="RequestResponse",
                 LogType="Tail",
                 Payload=data)

    logger.debug(r)
    logger.debug(r["StatusCode"])

    msg = base64.decodebytes(r["LogResult"].encode())

    logger.debug(msg.decode())

    return


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="fix missing thumbnails")
    parser.add_argument("profile", choices=["flowmsp-dev", "flowmsp-prod"])
    parser.add_argument("slug")
    parser.add_argument("location_id")
    parser.add_argument("--image-id")
    parser.add_argument("--debug", action="store_true")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")

    client = MongoClient(mongo_uri)

    logging.basicConfig()
    logger = logging.getLogger("fix_thumbnails")

    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    db = client.FlowMSP

    row = db["%s.Location" % args.slug].find_one({"_id": args.location_id})

    logger.info(row["address"])
    
    for i, image in enumerate(row["images"]):
        if args.image_id and image["_id"] != args.image_id:
            continue

        logger.info("%d of %d" % (i + 1, len(row["images"])))

        if "annotationMetadata" in image:
            call_function(args.profile,
                          args.slug,
                          args.location_id,
                          image,
                          "createAnnotatedImageAndThumbnail")

