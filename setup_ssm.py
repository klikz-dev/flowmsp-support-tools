#!/usr/bin/env python3

import os
import sys
import pprint
import argparse
import boto3
import botocore


def get_parm(session, name):
    ssm = session.client("ssm")

    r = ssm.get_parameter(Name=name, WithDecryption=True)

    return r["Parameter"]["Value"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check for broken links")
    parser.add_argument("profile", help="aws profile",
                        choices=["flowmsp-prod", "flowmsp-dev"])
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args()

    session = boto3.session.Session(profile_name=args.profile)

    ecs = session.client("ecs")
    ssm = session.client("ssm")

    r = ecs.describe_task_definition(taskDefinition="api-server")
    environment = r["taskDefinition"]["containerDefinitions"][0]["environment"]

    for e in environment:
        print(e["name"])

        ssm.put_parameter(
            Name=e["name"],
            Value=e["value"],
            Type="SecureString")





