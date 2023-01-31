#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
from pymongo import MongoClient
import boto3
import botocore
from utils import get_db


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


class EnvInfo:
    def __init__(self, profile, db_url):
        self.profile_name = profile
        self.session = boto3.session.Session(profile_name=self.profile_name)
        self.s3url = get_parm(self.session, "image_uri")
        self.client, self.db = get_db(self.session, db_url)

        return


def copy_customer(src, dst, customer_name):
    src_row = src.db.Customer.find_one({"slug": customer_name})

    result = dst.db.Customer.delete_one({"_id": src_row["_id"]})

    if result:
        print("Customer: %s: deleted %d rows" % (customer_name, result.deleted_count))

    if src_row is None:
        print("%s not found" % customer_name)
        return

    result = dst.db.Customer.insert_one(src_row)

    print("id", src_row["_id"])
    return src_row
    

def copy_password(src, dst, customer_name):
    result = dst.db.Password.delete_many({"customerSlug": customer_name})

    if result:
        print("Password: %s: deleted %d rows" % (customer_name, result.deleted_count))

    rows = 0

    for src_row in src.db.Password.find({"customerSlug": customer_name}):
        rows += 1

        try:
            dst.db.Password.insert_one(src_row)
        except:
            print("insert failed", src_row)

    print("Password: inserted %d rows" % rows)
    
    return


def copy_partners(src, dst, customer_id):
    result = dst.db.Partners.delete_many({"customerId": customer_id})

    if result:
        print("Partners: %s: deleted %d rows" % (customer_id, result.deleted_count))

    rows = 0

    for src_row in src.db.Partners.find({"customerId": customer_id}):
        rows += 1
        dst.db.Partners.insert_one(src_row)

    print("Partners: %s: inserted %d rows" % (customer_id, rows))
    return


def copy_table(src, dst, table):
    if table in dst.db.list_collection_names():
        dst.db.drop_collection(table)
        print("%s: dropped" % table)

    dst.db.create_collection(table)
    print("%s: created" % table)

    # create indexes
    for name, index_info in src.db[table].index_information().items():
        keys = index_info['key']
        del(index_info['ns'])
        del(index_info['v'])
        del(index_info['key'])
        dst.db[table].create_index(keys, name=name, **index_info)
        print("%s: %s: index created" % (table, name))

    rows = 0
    batch = []
    fix_urls = False

    if table.endswith(".Location"):
        fix_urls = True

    for src_row in src.db[table].find():
        rows += 1

        if fix_urls:
            for i, img in enumerate(src_row.get("images", [])):
                for k in "href", "hrefAnnotated", "hrefOriginal", "hrefThumbnail":
                    if k in src_row["images"][i]:
                        src_row["images"][i][k].replace(
                            "https://s3.amazonaws.com/%s" % src.s3url.split("/")[2],
                            "https://s3.amazonaws.com/%s" % dst.s3url.split("/")[2]
                            )

        # if table is Location update image urls
        batch.append(src_row)

        if len(batch) >= 500:
            dst.db[table].insert_many(batch)
            batch = []

    if batch:
        dst.db[table].insert_many(batch)

    print("%s: inserted % d rows" % (table, rows))
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
    

def copy_images(src, dst, prefix, customer_name):
    src_s3 = src.session.client("s3")
    dst_s3 = dst.session.client("s3")

    src_url = "%s/%s/%s" % (src.s3url, prefix, customer_name)
    dst_url = "%s/%s/%s" % (dst.s3url, prefix, customer_name)

    pull = "aws --profile flowmsp-prod s3 sync --only-show-errors %s %s" % (src_url, prefix)
    push = "aws --profile flowmsp-dev  s3 sync --only-show-errors --delete %s %s" % (prefix, dst_url)

    print(pull)
    print(push)
    print()

    return

    src_keys = get_s3_file_list(src_url, src.session)
    dst_keys = get_s3_file_list(dst_url, dst.session)

    # count images
    print("%10d %s" % (len(src_keys), src_url))
    print("%10d %s" % (len(dst_keys), dst_url))

    # TODO have this sync images
    # 1. delete any in dst not in src
    # 2. copy   any in src not in dst
    files = 0
    dst_bkt = dst.s3url.split("/")[2]

    for obj in src_keys:
        local_file = "/tmp/%s" % os.path.basename(obj.key)
        src_s3.download_file(obj.bucket_name, obj.key, local_file)
        dst_s3.upload_file(local_file, dst_bkt, obj.key)
        os.remove(local_file)
        files += 1

    print("copied %d images from %s to %s" % (files, src.s3url, dst.s3url))
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="copy data from prod to test")
    parser.add_argument("customer_name", help="customer name, aka slug",
                        nargs='+')
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--limit", action="store_true")
    parser.add_argument("--src", default="flowmsp-prod")
    parser.add_argument("--dst", default="flowmsp-dev")

    # maybe copy partner data at some point
    # parser.add_argument("--include-partners", action="store_true")
    parser.add_argument("--skip-images", action="store_true")
    parser.add_argument("--src-url")
    parser.add_argument("--dst-url")

    args = parser.parse_args()

    src = EnvInfo(args.src, args.src_url)
    dst = EnvInfo(args.dst, args.dst_url)

    for c in args.customer_name:
        cust = copy_customer(src, dst, c)

        if cust is None:
            continue

        copy_password(src, dst, c)
        copy_partners(src, dst, cust["_id"])

        for collection in src.db.list_collection_names():
            if collection.startswith(c + "."):
                copy_table(src, dst, collection)

        if not args.skip_images:
            for prefix in "annotated", "originals", "processed":
                copy_images(src, dst, prefix, c)



    
