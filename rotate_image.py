#!/usr/bin/env python3

import sys
import os
import argparse
import pprint
from io import BytesIO
import boto3
from pymongo import MongoClient



from PIL import Image


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def fix_orientation(session, bucket, key, default_orientation, force):
    s3 = session.resource("s3")
    
    extension = os.path.splitext(key)[1].lower()

    formats = {".jpeg": "JPEG", ".jpg": "JPEG",
        ".png": "PNG",}

    fmt = formats.get(extension, "UNKNOWN")

    if fmt == "UNKNOWN":
        print("%s: unsupported file extension" % extension, file=sys.stderr)
        return

    # Grabs the source file
    obj = s3.Object(bucket_name=bucket, key=key)
    obj_body = obj.get()['Body'].read()
    
    img = Image.open(BytesIO(obj_body))
    w, h = map(float, img.size)
    print("image size: width=%s height=%s" % (w, h))

    if hasattr(img, '_getexif'):
        print("has exif data")
        exifdata = img._getexif()
        try:
            orientation = exifdata.get(274)
            print("orientation from exif", orientation)
        except:
            # There was no EXIF Orientation Data
            orientation = default_orientation
    else:
        orientation = default_orientation

    if force:
        orientation = default_orientation

    print("orientation", orientation)

    if orientation is 1:    # Horizontal (normal)
        pass
    elif orientation is 2:  # Mirrored horizontal
        img = img.transpose(Image.FLIP_LEFT_RIGHT)
    elif orientation is 3:  # Rotated 180
        img = img.rotate(180)
    elif orientation is 4:  # Mirrored vertical
        img = img.rotate(180).transpose(Image.FLIP_LEFT_RIGHT)
    elif orientation is 5:  # Mirrored horizontal then rotated 90 CCW
        img = img.rotate(-90).transpose(Image.FLIP_LEFT_RIGHT)
        
    # rotates 90 degress to right
    elif orientation is 6:  # Rotated 90 CCW
        img = img.rotate(-90)
    elif orientation is 7:  # Mirrored horizontal then rotated 90 CW
        img = img.rotate(90).transpose(Image.FLIP_LEFT_RIGHT)
        
    # rotates 90 degrees to left
    elif orientation is 8:  # Rotated 90 CW
        img = img.rotate(90)

    buffer = BytesIO()
    img.save(buffer, fmt)
    buffer.seek(0)

    # Uploading the image
    #bucket = "flowmsp-prod-doug"
    print("s3://%s/%s" % (bucket, key))

    obj = s3.Object(bucket_name=bucket, key=key)
    obj.put(Body=buffer)

    return


def split_url(url):
    if url.startswith("https://"):
        _, _, _, bucket, key = url.split("/", 4)
    elif url.startswith("s3"):
        _, _, bucket, key = url.split("/", 3)
    
    return bucket, key


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="rotate images")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("slug")
    parser.add_argument("location_id")
    parser.add_argument("--image-id")
    parser.add_argument("--not-image-id")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--orientation", type=int, default=1)
    parser.add_argument("--force", action="store_true")

    # 8 rotate 90 degrees to left
    # 6 rotate 90 degrees to right


    args = parser.parse_args()
    
    session = boto3.session.Session(profile_name=args.profile)
    mongo_uri = get_parm(session, "mongo_uri")
    image_uri = get_parm(session, "image_uri")
    client = MongoClient(mongo_uri)
    db = client.FlowMSP
    coll = "%s.Location" % args.slug

    q = {"_id": args.location_id}
    row = db[coll].find_one(q)
        
    if not row:
        print("location_id not found", file=sys.stderr)
        raise SystemExit(1)

    print("----------------------------------------")
    pprint.pprint(row["address"])
    print("----------------------------------------")
    #raise SystemExit(0)

    for img in row.get("images", []):
        # if provided, we want to skip all other ids
        if args.image_id and img["_id"] != args.image_id:
            continue
            
        # if provided, we want to skip this id
        if args.not_image_id and img["_id"] == args.not_image_id:
            continue

        pprint.pprint(img)

        for k in "href", "hrefOriginal", "hrefThumbnail":
            print("----------------------------------------")
            bkt, key = split_url(img[k])
            fix_orientation(session, bkt, key, args.orientation, args.force)

