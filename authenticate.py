# -*- coding: utf-8 -*-
"""
Created on Fri May 31 14:49:43 2019

@author: mckaydjensen
"""


from selenium import webdriver
import requests
import time

import os

local_dir = os.path.join(os.path.dirname(__file__))
# Edit this to point to a selenium chromedriver file
CHROMEDRIVER = os.path.join(local_dir, 'chromedriver.exe')
# Location of text file containing authentication key
AUTH_KEY = os.path.join(local_dir, 'authentication_key.txt')


def get_new_auth_key():
    '''Uses Selenium to query request new auth key from FamilySearch'''
    
    # Ask for credentials from user
    username = input('FamilySearch Username: ')
    password = input('FamilySearch Password: ')
    
    # Start Selenium driver and navegate to FamilySearch platform page
    driver = webdriver.Chrome(CHROMEDRIVER)
    driver.get('https://www.familysearch.org/platform/')
    time.sleep(0.3)
    # Click on authenticate button
    driver.find_element_by_xpath('//*[text ()="Authenticate"]').click()
    time.sleep(0.3)
    # Input credentials and click Sign in
    try:
        driver.find_element_by_id('userName').send_keys(username)
        driver.find_element_by_id('password').send_keys(password)
        driver.find_element_by_xpath('//*[text ()="Sign In"]').click()
    # Try again if it didn't work the first time
    except:
        time.sleep(1)
        driver.find_element_by_xpath('//*[text()="Authenticate"]').click()
        time.sleep(0.3)
        driver.find_element_by_id('userName').send_keys(username)
        driver.find_element_by_id('password').send_keys(password)
        driver.find_element_by_xpath('//*[text()="Sign In"]').click()
    
    # Find auth key and save it
    auth_key = ''
    while not auth_key:
        try:
            auth_key = str(driver.find_element_by_xpath('//pre').text).strip()
        except:
            pass
        
    print('New authentication key:', auth_key)
    with open(AUTH_KEY, 'w') as fh:
        fh.write(auth_key)
        
    driver.quit()
    return auth_key



def read_auth_key():
    '''Gets auth key either from saved value or gets new key if
    old one is no longer valid
    '''
    with open(AUTH_KEY, 'r') as fh:
        auth_key = fh.read()
    # Send a test request to check if you need a new key
    test = requests.get('http://api.familysearch.org/platform/tree/persons?'
                        + 'pids=LHKL-JLF',
                        headers={'Authorization': 'Bearer {}'.format(auth_key),
                                 'Accept': 'application/json'})
    if test.status_code == 200:
        return auth_key
    # Get a new key if test request didn't work
    elif test.status_code == 401: #Unauthorized error
        print('New authentication key needed')
        return get_new_auth_key()
    else:
        print(test.status_code)