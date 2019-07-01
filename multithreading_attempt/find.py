# -*- coding: utf-8 -*-
"""
Created on Sat Jun  1 09:30:10 2019

@author: mckaydjensen
"""

import sys
import requests
import pandas as pd
import numpy as np
import time
import json
import threading

import authenticate

COLUMN_MAP = 'column_map.json'
with open(COLUMN_MAP, 'r') as fh:
    column_map = json.load(fh)


class Thread(threading.Thread):

    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs={}, Verbose=None):
        threading.Thread.__init__(self, group, target, name, args, kwargs)
        self._return = None

    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args,
                                        **self._kwargs)
    def join(self):
        threading.Thread.join(self)
        return self._return


class FamilySearchFind(object):
    
    def __init__(self, threads=1):
        self.auth = authenticate.Authenticator(num_of_keys=threads)
        self.thread_count=threads

    @staticmethod
    def format_params(persondict):
        '''Takes dict of params and puts them in the format used in request URL
        '''
        params = []
        for k in persondict:
            value = persondict[k]
            chunks = value.split()
            if len(chunks) > 1:
                params.append('{}:"{}"'.format(k, '+'.join(chunks)))
            elif len(chunks) == 1:
                params.append('{}:{}'.format(k, chunks[0]))
        return '+'.join(params)

    @staticmethod
    def process_fs_entry(entries):
        dict_ = {
            'fsid1': entries[0]['id'],
            'score1': entries[0]['score']
        }
        if len(entries) > 1:
            dict_['fsid2'] = entries[1]['id']
            dict_['score2'] = entries[1]['score']
        if len(entries) > 2:
            dict_['fsid3'] = entries[2]['id']
            dict_['score3'] = entries[2]['score']
        return dict_

    def get_fsid(self, params, usekey=0):
        # Use matches rather than search.
        api_root = 'http://api.familysearch.org/platform/tree/matches?q='
        response = requests.get(api_root + self.format_params(params),
                                headers={'Authorization': 'Bearer {}'.format(self.auth.keys[usekey]),
                                         'Accept': 'application/json'})
        if response.status_code == 429:
            wait = float(response.headers['Retry-After'])*1.1
            print('Throttled on auth key {}, waiting {:.1f} seconds!'.format(usekey, wait))
            time.sleep(wait)
            return self.get_fsid(params, usekey)
        # 401 is permission error. Reauthenticate if this happens.
        elif response.status_code == 401:
            self.auth.keys[usekey] = self.auth.get_new_auth_key()
            return self.get_fsid(params, usekey)
        elif response.status_code == 204:
            print('No results for query {}'.format(params))
            return {}
        elif response.status_code != 200:
            print('Unsuccessful request: HTTP status code is {}'.format(response.status_code))
            return {}
        best_entries = response.json()['entries'][:3]  # Best options come first
        return self.process_fs_entry(best_entries)

    def get_fsids_for_df_(self, df, columndict=column_map, usekey=0):
        fsids = []
        for index, row in df.iterrows():
            persondict = {}
            for col in df.columns:
                if pd.notna(row[col]):
                    persondict[col] = row[col]
            fsids.append(self.get_fsid(persondict, usekey))
        return pd.DataFrame(fsids)

    def get_fsids_for_df(self, df, columndict=column_map):
        if type(df) is str: # If df is a str assume it is the filename of a csv
            df = pd.read_csv(df, index_col=0)
        # columndict should transform df's column names to match the parameter
        # names required by FamilySearch API
        if columndict:
            df = df[list(columndict.keys())].rename(columns=columndict)
        # Split into groups, one for each thread
        groups = df.groupby(np.arange(len(df)) * self.thread_count // len(df))
        threads = [Thread(target=self.get_fsids_for_df_,
                          args=(g[1], columndict, g[0],)) for g in groups]
        for thread in threads:
            thread.start()
        return pd.concat((thread.join() for thread in threads))


if __name__ == '__main__':
    input_filename = input('Type the file path of the input file: ')
    output_filename = input('Type the file path of the output file: ')
    fsf = FamilySearchFind()
    fsids = fsf.get_fsids_for_df(input_filename)
    fsids.to_csv(output_filename)