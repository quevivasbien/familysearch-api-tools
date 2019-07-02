# -*- coding: utf-8 -*-
"""
Created on Fri May 31 14:49:43 2019

@author: mckaydjensen
"""


import requests
import socket
import os
import json

local_dir = os.path.join(os.path.dirname(__file__))
# Valid app key is provided by FamilySearch
APP_KEY = os.path.join(local_dir, 'application_key.txt')
# The auth key file can be blank to start with. This will be
# looked up if the one provided is not valid.
AUTH_KEY = os.path.join(local_dir, 'authentication_key.txt')


def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('10.255.255.255', 1))
        ip = s.getsockname()[0]
    except:
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip


def get_new_auth_key():
    with open(APP_KEY, 'r') as fh:
        app_key = fh.read()
    data = {
        'client_id': app_key,
        'grant_type': 'unauthenticated_session',
        'ip_address': get_ip()
    }
    response = requests.post('https://ident.familysearch.org/cis-web/oauth2/v3/token',
                             data=data,
                             headers={'Content-Type': 'application/x-www-form-urlencoded'})
    if response.status_code != 200:
        print('Invalid request for access token!')
        return
    token = json.loads(response.content)['access_token']
    print('New authentication key:', token)
    with open(AUTH_KEY, 'w') as fh:
        fh.write(token)
    return token


def read_auth_key():
    """Gets auth key either from saved value or gets new key if
    old one is no longer valid
    """
    with open(AUTH_KEY, 'r') as fh:
        auth_key = fh.read()
    # Send a test request to check if you need a new key
    test = requests.get('http://api.familysearch.org/platform/tree/persons',
                        params={'pids': 'LHKL-JLF'},  # Just a random test ID
                        headers={'Authorization': 'Bearer {}'.format(auth_key),
                                 'Accept': 'application/json'})
    if test.status_code == 200:
        return auth_key
    # Get a new key if test request didn't work
    elif test.status_code == 401:  # Unauthorized error
        print('New authentication key needed')
        return get_new_auth_key()
    else:
        print('Unexpected error: HTTP response on test is', test.status_code)