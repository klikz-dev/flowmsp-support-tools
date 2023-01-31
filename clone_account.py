#!/usr/bin/env python3

import os
import sys
import string
import uuid
import random
import datetime
import pprint
import argparse
from pymongo import MongoClient
import boto3
import botocore
import bcrypt


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


class EnvInfo:
    def __init__(self, profile_name):
        self.profile_name = profile_name
        self.session = boto3.session.Session(profile_name=profile_name)
        self.s3url = get_parm(self.session, "image_uri")
        self.mongo_url = get_parm(self.session, "mongo_uri")
        self.client = MongoClient(self.mongo_url)
        self.db = self.client.FlowMSP


def split_url(url):
    if url.startswith("s3://"):
        _, _, bucket, key = url.split("/", 3)
    else:
        bucket, key = url.split("/", 1)

    return bucket, key


def get_s3_file_list(url, sess):
    s3 = sess.resource("s3")
    bucket, key = split_url(url)

    print("bucket", bucket)
    print("key", key)

    keys = s3.Bucket(bucket).objects.filter(Prefix=key)

    return [k for k in keys]
    

def copy_images(env, prefix, oslug, nslug, id_map):
    s3 = env.session.client("s3")
    bucket = env.s3url.split("/", 3)
    url = "{base_url}/{prefix}/{slug}".format(
        base_url=env.s3url, prefix=prefix, slug=oslug)

    print("url", url)

    keys = get_s3_file_list(url, env.session)
    files = 0

    for k in keys:
        old_id = k.key.split("/")[2]
        new_id = id_map[old_id]

        old_loc = "/{slug}/{id}/".format(slug=oslug, id=old_id)
        new_loc = "/{slug}/{id}/".format(slug=nslug, id=new_id)

        new_key = k.key.replace(old_loc, new_loc)

        s3.copy_object(Bucket=k.bucket_name,
                       Key=new_key,
                       CopySource={'Bucket': k.bucket_name,
                                   'Key': k.key})
        files += 1

    print("copied %d images from %s to %s" % (files, oslug, nslug))

    return


def generate_slug(db):
    charset = string.ascii_lowercase + string.digits
    slug = "".join([random.choice(charset) for x in range(12)])
    row = db.Customer.find_one({"slug": slug})

    while row:
        slug = "".join([random.choice(charset) for x in range(12)])
        row = db.Customer.find_one({"slug": slug})

    return slug


def get_customer(env, slug):
    return env.db.Customer.find_one({"slug": slug})


def copy_customer(env, old, slug):
    now = datetime.datetime.now()
    then = now + datetime.timedelta(days=60)
    new = old.copy()

    new["_id"] = str(uuid.uuid4())
    new["license"]["creationTimestamp"] = now
    new["license"]["expirationTimestamp"] = then
    new["slug"] = slug
    new["toContains"] = "alerts+%s@flowmsp.com" % slug

    #pprint.pprint(new)
    result = env.db.Customer.insert_one(new)

    return new
    

def copy_partners(env, src_id, dst_id):
    rows = 0

    print("src", src_id)
    print("dst", dst_id)

    for row in env.db.Partners.find({"customerId": src_id}):
        rows += 1
        row["_id"] = str(uuid.uuid4())
        row["customerId"] = dst_id
        env.db.Partners.insert_one(row)

    print("Partners: inserted %d rows" % rows)

    return


def copy_table(env, table, oslug, nslug, replace_keys):
    otbl = "%s.%s" % (oslug, table)
    ntbl = "%s.%s" % (nslug, table)

    env.db.create_collection(ntbl)
    print("%s: created" % ntbl)

    # create indexes
    for name, index_info in env.db[otbl].index_information().items():
        keys = index_info['key']
        del(index_info['ns'])
        del(index_info['v'])
        del(index_info['key'])

        env.db[ntbl].create_index(keys, name=name, **index_info)
        print("%s: %s: index created" % (ntbl, name))

    rows = 0
    batch = []
    fix_urls = False

    if ntbl.endswith(".Location"):
        fix_urls = True

    id_map = {}

    for src_row in env.db[otbl].find():
        rows += 1

        old_id = src_row["_id"]
        src_row["_id"] = str(uuid.uuid4())
        id_map[old_id] = src_row["_id"]


        for k in replace_keys:
            if k in src_row:
                src_row[k] = replace_keys[k]

        if fix_urls:
            old_loc = "/{slug}/{id}/".format(slug=oslug, id=old_id)
            new_loc = "/{slug}/{id}/".format(slug=nslug, id=src_row["_id"])

            for i, img in enumerate(src_row["images"]):
                for k in img:
                    src_row["images"][i][k] = src_row["images"][i][k].replace(old_loc, new_loc)

        # if table is Location update image urls
        batch.append(src_row)

        if len(batch) >= 500:
            env.db[ntbl].insert_many(batch)
            batch = []

    if batch:
        env.db[ntbl].insert_many(batch)

    print("%s: inserted % d rows" % (ntbl, rows))

    if fix_urls:
        for p in "annotated", "originals", "processed":
            copy_images(env, p, oslug, nslug, id_map)

    return


def get_hashed_password(password):
    return bcrypt.hashpw(password.encode("utf-8"),
                         bcrypt.gensalt())


def check_password(plain_password, hashed_password):
    return bcrypt.checkpw(plain_password.encode("utf-8"),
                          hashed_password)


def check_email(db, email):
    return db.Password.find_one({"username": email})
    

def create_user(env, cust, email, password):
    encrypted_password = get_hashed_password(password).decode()
    encrypted_password = "$2a$10$mi7Jp5dEatNbCrJhfhoY1uuW1mzdXFvcwt7M1G0rutFCobW3HLsFi"

    row = {
        "_id": str(uuid.uuid4()),
        "customerId": cust["_id"],
        "customerSlug": cust["slug"],
        "password": encrypted_password,
        "username": email
    }

    result = env.db.Password.insert_one(row)

    return
    

def create_cuser(env, cust, email, first, last, role):
    table = "%s.User" % cust["slug"]

    env.db.create_collection(table)
    print("%s: created" % table)

    row = {
        "_id": str(uuid.uuid4()),
        "customerRef": {"customerId": cust["_id"],
                        "customerName": cust["name"],
                        "customerSlug": cust["slug"]
        },
        "email": email,
        "firstName": first,
        "lastName": last,
        "isOnDuty": False,
        "role": role,
        "uiConfig": {}
    }

    result = env.db[table].insert_one(row)
    return


def check_slug(db, slug):
    return db.Customer.find_one({"slug": slug})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="copy data from prod to test")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("slug")
    parser.add_argument("first")
    parser.add_argument("last")
    parser.add_argument("userid")
    parser.add_argument("password")
    parser.add_argument("--skip-images")
    parser.add_argument("--new-slug")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    image_uri = get_parm(session, "image_uri")
    client = MongoClient(mongo_uri)
    db = client.FlowMSP

    env = EnvInfo(args.profile)

    old = get_customer(env, args.slug)

    if not old:
        print("%s: source slug not found" % args.slug)
        raise SystemExit(1)

    if check_email(env.db, args.userid):
        print("%s: userid already exists" % args.userid)
        raise SystemExit(1)

    if args.new_slug and check_slug(env.db, args.new_slug):
        print("%s: target slug already exists" % args.new_slug)
        raise SystemExit(1)

    new_slug = args.new_slug or generate_slug(env.db)

    print("new_slug", new_slug)

    new = copy_customer(env, old, new_slug)
    print("id", new["_id"])
    create_user(env, new, args.userid, args.password)
    create_cuser(env, new, args.userid, args.first, args.last, "ADMIN")

    copy_partners(env, old["_id"], new["_id"])

    copy_table(env,
               "Hydrant",
               old["slug"],
               new["slug"],
               {"customerId": new["_id"], "customerSlug": new["slug"]})


    copy_table(env,
               "MsgReceiver",
               old["slug"],
               new["slug"],
               {"customerId": new["_id"], "customerSlug": new["slug"]})
               
    copy_table(env,
               "Location",
               old["slug"],
               new["slug"],
               {"customerId": new["_id"], "customerSlug": new["slug"]})

    raise SystemExit(0)

    if not args.skip_images:
        for prefix in "annotated", "originals", "processed":
            copy_images(env, prefix, c)
