# -*- coding: utf-8 -*-
"""
Created on Fri May 31 14:49:43 2019

@author: mckaydjensen
"""


from selenium import webdriver
import requests
import time
import os
import json

local_dir = os.path.join(os.path.dirname(__file__))
# Edit this to point to a selenium chromedriver file
CHROMEDRIVER = os.path.join(local_dir, 'chromedriver.exe')
# Location of json file containing list of authentication keys
AUTH_KEYS = os.path.join(local_dir, 'authentication_keys.json')

class Authenticator(object):

    def __init__(self, keys_file=AUTH_KEYS, num_of_keys=5):
        self.fs_username = None
        self.fs_password = None
        self.keys_file = keys_file
        self.num_of_keys = num_of_keys
        self.keys = self.load_keys(keys_file)

    def get_new_auth_key(self):
        '''Uses Selenium to query request new auth key from FamilySearch'''

        if self.fs_username is None or self.fs_password is None:
            # Ask for credentials from user
            self.fs_username = input('FamilySearch Username: ')
            self.fs_password = input('FamilySearch Password: ')

        # Start Selenium driver and navigate to FamilySearch platform page
        driver = webdriver.Chrome(CHROMEDRIVER)
        driver.get('https://www.familysearch.org/platform/')
        time.sleep(0.3)
        # Click on authenticate button
        driver.find_element_by_xpath('//*[text ()="Authenticate"]').click()
        time.sleep(0.3)
        # Input credentials and click Sign in
        try:
            driver.find_element_by_id('userName').send_keys(self.fs_username)
            driver.find_element_by_id('password').send_keys(self.fs_password)
            driver.find_element_by_xpath('//*[text ()="Sign In"]').click()
        # Try again if it didn't work the first time
        except:
            time.sleep(1)
            driver.find_element_by_xpath('//*[text()="Authenticate"]').click()
            time.sleep(0.3)
            driver.find_element_by_id('userName').send_keys(self.fs_username)
            driver.find_element_by_id('password').send_keys(self.fs_password)
            driver.find_element_by_xpath('//*[text()="Sign In"]').click()

        # Find auth key and save it
        auth_key = ''
        while not auth_key:
            try:
                auth_key = str(driver.find_element_by_xpath('//pre').text).strip()
            except:
                pass

        print('New authentication key:', auth_key)
        driver.quit()
        return auth_key

    def verify_auth_key(self, key):
        # Send a test request to check if you need a new key
        test = requests.get('http://api.familysearch.org/platform/tree/persons',
                            params={'pids':'LHKL-JLF'},
                            headers={'Authorization': 'Bearer {}'.format(key),
                                     'Accept': 'application/json'})
        if test.status_code == 200:
            return key
        # Get a new key if test request didn't work
        elif test.status_code == 401:  # Unauthorized error
            print('New authentication key needed')
            return self.get_new_auth_key()
        else:
            print('Unrecognized HTTP status:', test.status_code)

    def load_keys(self, save=True):
        with open(self.keys_file, 'r') as fh:
            keys = json.load(fh)
        # Pad keys to have length self.num_of_keys
        keys = keys[:self.num_of_keys]
        if len(keys) < self.num_of_keys:
            keys += ['NULL']*(self.num_of_keys - len(keys))
        # Make sure keys work and replace them if they don't
        keys = [self.verify_auth_key(key) for key in keys]
        if save:
            with open(self.keys_file, 'w') as fh:
                json.dump(keys, fh)
        return keys