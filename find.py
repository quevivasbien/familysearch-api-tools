# -*- coding: utf-8 -*-
"""
Created on Sat Jun  1 09:30:10 2019

@author: mckaydjensen

Tools for finding FamilySearch PIDs given identifying info
"""

import requests
import pandas as pd
import time
import json

import authenticate

"""
COLUMN_MAP file should be a json file formatted like
{
 "column_name": "familysearch_search_param",
 ...
}
with the valid familysearch_search_param fields defined in the FamilySearch API documentation
(https://www.familysearch.org/developers/docs/api/tree/Tree_Person_Search_resource)

"""
COLUMN_MAP = 'column_map.json'
with open(COLUMN_MAP, 'r') as fh:
    column_map = json.load(fh)


class FamilySearchFind(object):
    """FamilySearchFind object is essentially a container for find-related functions with authentication integrated.
    """
    
    def __init__(self):
        self.key = authenticate.read_auth_key()

    @staticmethod
    def format_params(persondict):
        """Takes dict of params and puts them in the format used in request URL
        """
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
        """Takes a response from FamilySearch API search or find code and extracts most probable matches
        """
        dict_ = {
            'pid1': entries[0]['id'],
            'score1': entries[0]['score']
        }
        if len(entries) > 1:
            dict_['pid2'] = entries[1]['id']
            dict_['score2'] = entries[1]['score']
        if len(entries) > 2:
            dict_['pid3'] = entries[2]['id']
            dict_['score3'] = entries[2]['score']
        return dict_

    def get_fsid(self, persondict):
        """Takes a dict of params (like the ones built by get_fsids_for_df),
        queries familysearch match for matches, and returns the 3 best matches
        """
        params = self.format_params(persondict)
        # Use matches rather than search.
        api_root = 'http://api.familysearch.org/platform/tree/matches?q='
        response = requests.get(api_root + params,
                                headers={'Authorization': 'Bearer {}'.format(self.key),
                                         'Accept': 'application/json'})
        if response.status_code == 429:
            wait = float(response.headers['Retry-After'])*1.1
            print('Throttled, waiting {:.1f} seconds!'.format(wait))
            time.sleep(wait)
            return self.get_fsid(persondict)
        # 401 is permission error. Reauthenticate if this happens.
        elif response.status_code == 401:
            self.key = authenticate.read_auth_key()
            return self.get_fsid(persondict)
        elif response.status_code == 204:
            print('No results for query {}'.format(persondict))
            return {}
        elif response.status_code != 200:
            print('Unsuccessful request: HTTP status code is {}'.format(response.status_code))
            return {}
        best_entries = response.json()['entries'][:3]  # Best options come first
        return self.process_fs_entry(best_entries)

    def get_fsids_for_df(self, df, index_col=0, columndict=column_map, verbose=True):
        """Takes a pandas DataFrame and returns a dataframe of likely matches
        
        df: the dataframe to match for. the columns are identifier types; each row is a person
        index_col: the column of the dataframe that contains its index. set to None if there is no index
        columndict: a dict object to convert df's column names to FamilySearch identifiers.
            Defaults to the dict in the COLUMN_MAP file
        verbose: whether or not to print updates for each entry as the API is queried.
        """
        pids = []
        if type(df) is str:  # If df is a str assume it is the filename of a csv
            try:
                df = pd.read_csv(df, index_col=index_col)
            except UnicodeDecodeError:
                df = pd.read_csv(df, index_col=index_col, encoding='ansi')
        if columndict:
            df = df[columndict.keys()].rename(columns=columndict)
        for index, row in df.iterrows():
            if verbose:
                print(f'Working on {index}...')
            persondict = {}
            for col in df.columns:
                if pd.notna(row[col]):
                    persondict[col] = row[col]
            pids.append(self.get_fsid(persondict))
        return pd.DataFrame(pids, index=df.index)
    
if __name__ == '__main__':
    # This code will be run if the file is executed directly rather than imported.
    input_filename = input('Type the file path of the input file: ')
    output_filename = input('Type the file path of the output file: ')
    fsf = FamilySearchFind()
    fsids = fsf.get_fsids_for_df(input_filename)
    fsids.to_csv(output_filename)