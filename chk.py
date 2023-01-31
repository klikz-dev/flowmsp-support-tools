#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
from pymongo import MongoClient
import boto3
import botocore


# https://s3.amazonaws.com/flowmsp-images/originals/westmontfire/80ec1696-0458-4a70-94ab-09ba864f9940/IMG_3060_38e1f9b7_3e22_4ff3_b8a7_134fdde3de94.jpg
def chk_url(p, kurl):
    url = p[k]
    url2 = url.replace("https://s3.amazonaws.com/", "s3://")

    print(k, url)
    os.system("aws --profile flowmsp-prod s3 ls --recursive %s" % url2)

    return


def get_customers(db):
    return [c["slug"] for c in db.Customer.find()]


def split_image_url(url):
    if url.startswith("https://"):
        _, _, _, bkt, key = url.split("/", 4)
    else:
        bkt, key = url.split("/", 1)

    return bkt, key


def print_info(n, keys):
    for k in keys:
        print(k, n.get(k, "n/a"))

    return


# https://s3.amazonaws.com/flowmsp-images/processed/savoyfiredep/f0cbb998-311f-4247-936d-0dfaca3c3e7d/test_73_0c5143c5_f811_4cef_a47f_75f4ba7359a6-1024h.jpg
def check_image_links(image, s3_files):
    for href in "href", "hrefOriginal", "hrefThumbnail", "hrefAnnotated":
        if href in image:
            if image[href] not in s3_files:
                print("------------------------------------------------------------")

                print_info(p, ["_id", "href", "hrefOriginal",
                               "hrefThumbnail", "originalFileName",
                               "sanitizedFileName"])

                return "NOT OK"

    return "OK"


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

    files = []

    for k in keys:
        url = "https://s3.amazonaws.com/{b}/{k}".format(
            b=k.bucket_name,
            k=k.key)

        files.append(url)

    return files

    # return ["s3://{b}/{k}".format(b=bucket, k=k.key) for k in keys]
    # return [k for k in keys]


def load_s3_objects(customer_name, base_url, session):
    files = []

    for file_type in "annotated", "originals", "processed":
        url = "{bkt}/{file_type}/{slug}".format(
            bkt=base_url,
            file_type=file_type,
            slug=customer_name)

        files += get_s3_file_list(url, session)

    return files


def customer_name_ok(all_customers, customer):
    if customer not in all_customers:
        print("%s: not found" % customer, file=sys.stderr)

        for c in all_customers:
            if c.startswith(customer):
                print("maybe you meant %s?" % c, file=sys.stderr)

        return False
    else:
        return True


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("customer_name", help="customer name, aka slug",
                        nargs='*')
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--multi-line", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--address")
    parser.add_argument("--include-address", action="store_true")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    image_uri = get_parm(session, "image_uri")

    client = MongoClient(mongo_uri)

    db = client.FlowMSP

    all_customers = get_customers(db)
    customers = args.customer_name or all_customers

    if args.customer_name:
        errs = 0
        for c in args.customer_name:
            if not customer_name_ok(all_customers, c):
                errs += 1

        if errs:
            raise SystemExit(1)

    if args.verbose:
        m = max([len(c) for c in customers])

        print("%-*.*s %-10s %-10s %-10s %-10s %-10s" %
              (m, m, "Customer",
               "Locations", "Loc w/img", "Images", "Errors", "S3 Files"),
              file=sys.stderr)
        print("%s ---------- ---------- ---------- ---------- ----------" % ("-" * m),
              file=sys.stderr)

    for customer_name in customers:
        s3_files = load_s3_objects(customer_name, image_uri, session)

        locations = locationsi = images = errors = 0

        for rownum, row in enumerate(db["%s.Location" % customer_name].find()):
            locations += 1

            if args.address and not row["address"]["address1"].startswith(args.address):
                continue

            if "images" not in row or len(row["images"]) == 0:
                continue

            if args.debug:
                print(row.get("createdOn", "n/a"),
                      row.get("modifiedOn", "n/a"),
                      row["address"])

            locationsi += 1

            for i, image in enumerate(row["images"]):
                images += 1

                for attr in "annotationMetadata", "annotationSVG":
                    if attr in image and "https://test.flowmsp.com" in image[attr]:
                        if not args.multi_line:
                            print(customer_name,
                                  row["_id"],
                                  image["_id"],
                                  attr,
                                  "")
                        else:
                            print("------------------------------------------------------------")
                            print("slug", customer_name)
                            print("locationId", row["_id"])
                            print("address1", row["address"]["address1"])
                            print("imageCount", len(row["images"]))
                            print("imageId", image["_id"])
                            print("imageKey", attr)

                        errors += 1

                for href in "href", "hrefOriginal", "hrefThumbnail", "hrefAnnotated":
                    if href in image:
                        # pdfs don't have thumbnails
                        if image[href] == "NoImage" and "href" in image and image["href"].lower().endswith(".pdf"):
                            continue

                        if image[href] not in s3_files:
                            errors += 1

                            if args.debug:
                                pprint.pprint(image)

                            if not args.multi_line:
                                print(customer_name,
                                      row["_id"],
                                      image["_id"],
                                      href,
                                      image[href])
                            else:
                                print("------------------------------------------------------------")
                                print("slug", customer_name)
                                print("locationId", row["_id"])
                                print("address1", row["address"]["address1"])
                                print("imageCount", len(row["images"]))
                                print("imageId", image["_id"])
                                print("imageKey", href)
                                print("url", image[href])

        if args.verbose and errors > 0:
            print("%-*.*s %10d %10d %10d %10d %10d" %
                  (m, m, customer_name, locations, locationsi, images, errors, len(s3_files)),
                  file=sys.stderr)
