#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
from pymongo import MongoClient
import boto3
import botocore
import logging

logging.basicConfig()
logger = logging.getLogger("clone_data")
logger.setLevel(logging.INFO)


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


class EnvInfo:
    def __init__(self, profile_name, slug):
        self.slug = slug
        self.profile_name = profile_name
        self.session = boto3.session.Session(profile_name=profile_name)
        self.s3url = get_parm(self.session, "image_uri")
        self.mongo_url = get_parm(self.session, "mongo_uri")
        self.client = MongoClient(self.mongo_url)
        self.db = self.client.FlowMSP

        self.get_customer()


    def get_customer(self):
        c = self.db.Customer.find_one({"slug": self.slug})

        self.customerId = c["_id"]


def copy_table(src, dst, table, truncate, func=None):
    s = "{slug}.{table}".format(slug=src.slug, table=table)
    d = "{slug}.{table}".format(slug=dst.slug, table=table)

    if truncate:
        r = dst.db[d].delete_many({})
        logger.info("%s: deleted %d rows" % (d, r.deleted_count))

    batch = []
    rows = 0

    for row in src.db[s].find():
        rows += 1

        row["customerId"] = dst.customerId
        row["customerSlug"] = dst.slug

        if func:
            func(src, dst, row)

        batch.append(row)

        if len(batch) >= 500:
            r = dst.db[d].insert_many(batch)
            batch = []

    if batch:
        r = dst.db[d].insert_many(batch)
    
    logger.info("%s: inserted %d rows" % (d, rows))

    return


def update_image_urls(src, dst, row):
    s = "/{slug}/".format(slug=src.slug)
    d = "/{slug}/".format(slug=dst.slug)

    for i, img in enumerate(row["images"]):
        for k in "href", "hrefAnnotated", "hrefOriginal", "hrefThumbnail":
            if k in row["images"][i]:
                row["images"][i][k] = row["images"][i][k].replace(s, d)

    return


def split_url(url):
    if url.startswith("s3://"):
        _, _, bucket, key = url.split("/", 3)
    else:
        bucket, key = url.split("/", 1)

    return bucket, key


def get_s3_file_list(url, sess):
    s3 = sess.resource("s3")
    bucket, key = split_url(url)

    keys = s3.Bucket(bucket).objects.filter(Prefix=key)

    # return ["s3://{b}/{k}".format(b=bucket, k=k.key) for k in keys]
    return [k for k in keys]
    

def run_cmd(cmd):
    logger.info(cmd)

    rc = os.system(cmd)

    if rc != 0:
        logger.warn("rc=%d" % rc)

    return


def copy_images(src, dst, prefix):
    src_s3 = src.session.client("s3")
    dst_s3 = dst.session.client("s3")

    src_url = "%s/%s/%s" % (src.s3url, prefix, src.slug)
    dst_url = "%s/%s/%s" % (dst.s3url, prefix, dst.slug)

    if src.profile_name == dst.profile_name:
        cmd = "aws --profile {profile_name} s3 sync {src_url} {dst_url} --only-show-errors".format(
            profile_name=src.profile_name,
            src_url=src_url,
            dst_url=dst_url)

        run_cmd(cmd)

    else:
        pull = "aws --profile {profile_name} s3 sync {src_url} {prefix} --only-show-errors".format(
            profile_name=src.profile_name,
            src_url=src_url,
            prefix=prefix)

        run_cmd(pull)

        push = "aws --profile {profile_name} s3 sync {prefix} {dst_url} --only-show-errors".format(
            profile_name=src.profile_name,
            prefix=prefix,
            dst_url=dst_url)

        run_cmd(push)

        rm = "rm -rf {prefix}".format(prefix=prefix)

        run_cmd(rm)

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="copy data from prod to test")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("src")
    parser.add_argument("dst")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--limit", action="store_true")
    parser.add_argument("--truncate", action="store_true")

    parser.add_argument("--skip-images", action="store_true")

    args = parser.parse_args()

    src = EnvInfo(args.profile, args.src)
    dst = EnvInfo(args.profile, args.dst)
    
    copy_table(src, dst, "Hydrant", args.truncate)
    copy_table(src, dst, "Location", args.truncate, update_image_urls)

    if not args.skip_images:
        for prefix in "annotated", "originals", "processed":
            copy_images(src, dst, prefix)



    
