#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
import datetime
from pymongo import MongoClient
import boto3
import botocore
import csv
import logging


logging.basicConfig()
logger = logging.getLogger("utils")


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


def get_db(session, url=None):
    if url is None:
        url = get_parm(session, "MONGO_URI")

    logger.debug(f"url={url}")

    client = MongoClient(url)
    db = client.FlowMSP
    
    return client, db
