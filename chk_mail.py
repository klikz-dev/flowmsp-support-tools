#!/usr/bin/env python3

import pprint
import requests

url = 'https://www.googleapis.com/oauth2/v4/token'

#base_url = "https://mail.google.com"
base_url = "https://www.googleapis.com/oauth2/v4/token"

def authorization():
    url = 'https://www.googleapis.com/oauth2/v4/token'
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "charset": "utf-8",
    }

    params = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token"
    }

    r = requests.post(url, headers=headers, params=params)

    pprint.pprint(r.json())
    return r


def watch(token, topic):
    url = "https://www.googleapis.com/gmail/v1/users/me/watch"
    #url = "https://pubsub.googleapis.com/v1/%s" % topic

    params = {
        "topicName": "projects/emailalertsdevproject/topics/devalerts",
        "labelIds": ["INBOX"]
    }
    #params = {"name": "INBOX"}

    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer %s" % token
    }

    r = requests.post(url, headers=headers, params=params)
    #r = requests.put(url, headers=headers, params=params)

    pprint.pprint(r)

    if r.ok:
        pprint.pprint(r.json())

    return r
