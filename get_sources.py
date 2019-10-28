# -*- coding: utf-8 -*-
"""
Created on Fri Aug  2 13:10:29 2019

@author: mckaydjensen

Includes tools for getting source info via FamilySearch API, particularly for getting and processing census data
"""

import requests
import re
import pandas as pd
import numpy as np
import time
import warnings
import json
import os

import authenticate


CENSUS_PTTRN = r'[Cc]ensus'
DEATH_PTTRN = r'[Dd]eath'
CENSUS_COLUMNS = 'census_columns.json'
DEATH_RECORD_COLUMNS = 'death_record_columns.json'

ark_re = re.compile(r'[^:]{4}-[^:]{3}$')


def log_warning(message, origin=None, load=None, log_file='log.txt'):
    """Print a warning message and save warning info to a log file.

    message (str): the message to print
    origin (function): the function that caused the warning
    load: the identifier string or other argument that the origin function was dealing with
    log_file (str): the filename of the text file to save the warning to.
    """
    warnings.warn(message, category=RuntimeWarning)
    with open(log_file, 'a', encoding='utf-8') as fh:
        fh.write('{}; Origin: {}; Load: {}; Time: {}\n'.format(
            message, repr(origin), load, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        ))


def iterate(dictionary, mydict=None):
    """
    Iterate recursively through the full json and find the actual information.
    Parameters
    ----------
    dictionary - The actual json from the API.
    mydict     - An empty dictionary.
    Returns
    -------
    A dictionary with the variable names and values.
    <<Adapted from Tanner Eastmond's Familysearch.ScrapeRecord>>
    """
    if mydict is None:
        mydict = dict()
    # Figure out which person we are interested in.
    try:
        keep = dictionary['description'][4:]
        for count in range(len(dictionary['persons'])):
            if keep == dictionary['persons'][count]['id']:
                break
    except:
        count = 0

    # Initiate label and check.
    lab = ''
    check = False

    # Loop over each token and value from the json.
    for token, value in dictionary.items():
        # If the value is a dictionary, call the function again and continue.
        if isinstance(value, dict):
            iterate(value, mydict)
            continue

        # If the value is a list, call the function again for each nested dictionary.
        elif isinstance(value, list):
            for x in value:
                if isinstance(x, dict):
                    iterate(x, mydict)
                    continue
            continue

        # Get the variable name if the label is correct and mark check as true.
        if token == 'labelId':
            lab = value
            check = True

        # Get the variable value and update the dictionary if we don't already have that variable.
        if check == True and token == 'text':
            if lab.lower() not in mydict.keys():
                # Initialize the key if it doesn't exist yet.
                mydict.update({lab.lower() : re.sub(';',':',value)})
            else:
                # Append all of the data together.
                mydict[lab.lower()] = mydict[lab.lower()] + ';' + re.sub(';',':',value)
            # Reset check.
            check = False

    # Return the dictionary.
    return mydict, count


def create_df(response, arkid):
    """Takes a successful HTTP response from a request to the FamilySearch API for a record
    extracts the relevant fields, and puts them together as a Pandas DataFrame.

    response (requests.models.Response): A (status 200) response to a GET query to ~/platform/records/personas/{arkid}
    arkid (str): The ark ID of the requested resource
    """
    # Create dictionary based on JSON response
    response_dict = response.json()
    source_dict, c = iterate(response_dict)
    # Convert to Pandas DataFrame
    for k in source_dict.keys():
        source_dict[k] = source_dict[k].split(';')
    df = pd.DataFrame.from_dict(source_dict, orient='index').transpose()
    df['is_person'] = [int(i == c) for i in range(len(df))]
    try:
        arkids = [p['identifiers']['http://gedcomx.org/Persistent'][0] for p in response_dict['persons']]
        arkids = [ark_re.search(x).group() for x in arkids]
        assert len(arkids) == len(df)
    except (KeyError, AssertionError, AttributeError):
        arkids = [None]*len(df)
        try:
            arkids[c] = arkid
        except IndexError:
            pass
    df['ark_id'] = arkids
    return df


class FamilySearchSourcer:

    def __init__(self):
        self.authenticate()
        self.retries = 0

    def authenticate(self):
        """Get an access token and set the headers to be used for queries to the API"""
        self.key = authenticate.read_auth_key()
        self.headers = {
            'Authorization': 'Bearer {}'.format(self.key),
            'Accept': 'application/json'
        }

    def process_response(self, response, func, load, null, mutator):
        """Process the response to a GET request to the API, dealing with possible errors

        response (requests.models.Response): The response to a GET request to the API
        func (function): the function that created the request (needed so we can retry if it didn't work the first time)
        load: the identifier string or other argument that the origin function was dealing with (e.g. ark id or pid)
        null (uninitialized object): the object type to return in case of no results or failed requests.
            Note that this should be uninitialized; e.g. set null=list not null=list() or null=[]
        mutator (function): A function to run on the response before returning if HTTP status is 200 (successful)
        """
        if response.status_code == 200:
            to_return = mutator(response, load)
        elif response.status_code == 204:
            to_return = null()  # no results
        elif response.status_code == 401:
            # Reauthenticate and retry
            self.authenticate()
            to_return = func(load)
        elif response.status_code == 429:
            # Wait and retry
            wait = float(response.headers['Retry-After'])*1.1
            print('Throttled, waiting {:.1f} seconds!'.format(wait))
            time.sleep(wait)
            to_return = func(load)
        elif response.status_code >= 500:  # Server-side error
            if self.retries < 3:
                self.retries += 1
                print(f'Server-side error ({response.status_code}). Waiting 1 minute, then retrying...')
                time.sleep(60)
                return func(load)  # Don't send to to_return since we don't want to reset retries.
            else:
                log_warning(f'Retries maxed out. Last status was {response.status_code}.', func, load)
                to_return = null()
        else:
            log_warning(f'HTTP status code {response.status_code}', func, load)
            to_return = null()
        self.retries = 0
        return to_return

    def get_attached_sources(self, pid):
        """Takes a PID and returns a dict describing the sources attached to that person."""
        url = f'https://api.familysearch.org/platform/tree/persons/{pid}/sources'
        response = requests.get(url, headers=self.headers)
        return self.process_response(response, self.get_attached_sources, pid, list,
                                     lambda x, _: x.json()['sourceDescriptions'])

    def search_for_sources(self, pid):
        """Takes a PID and returns a dict describing possibly matching (but unattached) sources for that person."""
        url = (f'https://api.familysearch.org/platform/tree/persons/{pid}/matches?' +
                'collection=https://familysearch.org/platform/collections/records')
        response = requests.get(url, headers=self.headers)
        return self.process_response(response, self.search_for_sources, pid, list, lambda x, _: x.json()['entries'])

    def check_attached_sources(self, pid, lookfor):
        """Gets ark ids for records attached to a person that have a given word/pattern in their descriptions

        pid (str): the PID of the person to get records from
        lookfor (str): a regular expression to look for in record descriptions
        
        Returns pairs of arkids along with their confidence scores
        """
        arkids = []
        sources = self.get_attached_sources(pid)
        for source in sources:
            if re.search(lookfor, ' '.join(x['value'] for x in source['titles'])):
                try:
                    arkids.append(ark_re.search(source['about']).group())
                except (KeyError, AttributeError):
                    continue
        return list(zip(arkids, [1]*len(arkids)))  # Confidence assumed to be 1 for sources already attached

    def check_other_sources(self, pid, lookfor):
        """Like check_attached_sources, but searches for as-yet-unattached records instead of looking at attached ones

        pid (str): the PID of the person to get records from
        lookfor (str): a regular expression to look for in record descriptions
        
        Returns pairs of arkids along with their confidence scores
        """
        arkids = []
        scores = []
        sources = self.search_for_sources(pid)
        for source in sources:
            if re.search(lookfor, source['title']):
                arkid = ark_re.search(source['id'])
                if arkid is not None:
                    arkids.append(arkid.group())
                    scores.append(source['score'])
        return list(zip(arkids, scores))

    def check_all_sources(self, pid, lookfor):
        return self.check_attached_sources(pid, lookfor) + self.check_other_sources(pid, lookfor)

    def process_record(self, arkid, score):
        """Takes the ark ID for a record and creates a Pandas DataFrame of the data on the record."""
        url = f'https://api.familysearch.org/platform/records/personas/{arkid}'
        response = requests.get(url, headers=self.headers)
        df = self.process_response(response, self.process_record, arkid, pd.DataFrame, create_df)
        df['score'] = [score]*len(df)
        return df

    def get_records_for_pid(self, pid, lookfor):
        """Gets record data for a person with the record descriptions matching a given word/pattern

        pid (str): the person to look for records for
        lookfor (str): a regular expression to look for in record descriptions (e.g. r'[Cc]ensus' for census records)
        """
        print(f'Working on {pid}...')
        arkids = self.check_all_sources(pid, lookfor)
        if arkids:
            df = pd.concat((self.process_record(arkid, score) for arkid, score in arkids), sort=True)
            df['PID'] = [pid]*len(df)
            return df
        else:
            return pd.DataFrame()


def process_year(yr):
    if isinstance(yr, int):
        return yr
    elif isinstance(yr, str):
        return int(yr[-4:])
    else:
        return yr
    

def dedup(df):
    """Returns the rows in a df of record data that correspond to people in class reports.
    Also drops records with duplicate PID and year based on their confidence scores.
    If no record has a best confidence score, all will be dropped.
    """
    df_person = df[df['is_person']==1]
    duplicates = df_person[df_person.duplicated(['PID', 'year'], keep=False)]
    grouped = duplicates.groupby(['PID', 'year'])
    to_keep = []
    for group in grouped.groups:
        dups = grouped.get_group(group)
        max_score = dups.score.max()
        dups = dups[dups.score == max_score]
        if len(dups) == 1:
            to_keep.append(dups.index[0])
    to_drop = duplicates.index.difference(pd.Index(to_keep))
    return df_person.drop(index=to_drop)
#        for column in dups.columns:
#            notna = pd.notna(dups[column])
#            if notna.sum() > 1:
#                dups[column] = [np.nan]*len(dups)
#            else:
#                dups[column] = np.repeat(dups[column][notna].unique(), len(dups))
#        to_keep.append(dups.iloc[0])    


def condense_record(df_in, columns_file):
    """Takes a Pandas DataFrame of record data and merges/drops some of the columns to create a more compact dataset
    The column names to be retained and merged together are in the columns_file in JSON format.
    """
    with open(columns_file, 'r') as fh:
        columndict = json.load(fh)
    df_out = pd.DataFrame(columns=list(columndict.keys()))
    for k in columndict.keys():
        for x in columndict[k]:
            try:
                df_out[k] = df_out[k].combine_first(df_in[x])
            except KeyError:
                pass
    return df_out.drop_duplicates()


def condense_census(df_in):
    df_out = condense_record(df_in, CENSUS_COLUMNS)
    # Drop data from state censuses & where year is missing
    df_out.year = df_out.year.apply(process_year)
    df_out = df_out[pd.notna(df_out.year) & (df_out.year % 10 == 0)]
    return df_out


def condense_death_records(df_in):
    df_out = condense_record(df_in, DEATH_RECORD_COLUMNS)
    # Drop entries with no death date
    df_out = df_out[pd.notna(df_out['death_date'])]
    return df_out


def get_records_for_pids_in_csv(lookfor, filename, col_name='PID'):
    """Takes a CSV with a PID column and creates a Pandas DataFrame with all the record data for those PIDs.

    lookfor (str): the regex pattern used to identify record types from their descriptions, e.g. '[Cc]ensus' for census
    filename (str): the file name of the CSV to get the PIDs from
    col_name (str): the name of the column that contains the PIDs
    """
    df_in = pd.read_csv(filename)
    fss = FamilySearchSourcer()
    df_out = pd.concat((fss.get_records_for_pid(pid, lookfor) for pid in df_in[col_name])).reset_index(drop=True)
    return df_out


def condense_and_save(df, saveas=None, condense=None, save_uncondensed=True, append=True):
    """Saves DataFrame to CSV, with options to condense data first and save uncondensed version as well

    df (DataFrame): the data to save
    saveas (str, optional): a file name to save the outputted DataFrame at (should end with ".csv")
    condense (function, optional): if provided, the function to use to condense the data (e.g. condense_census)
    save_uncondensed (bool): if saveas isprovided and condense is True, determines whether to also save uncondensed data
    append (bool): If the saveas file already exists, whether to append to that file or just replace it.
    """
    if condense is not None:
        if (saveas is not None) and save_uncondensed:
            saveas_condensed = re.sub(r'\..{3,4}$', '_uncondensed.csv', saveas)
            if append and os.path.isfile(saveas_condensed):
                with open(saveas_condensed, 'a') as fh:
                    df.to_csv(fh, header=False, index=False)
            else:
                df.to_csv(saveas_condensed, index=False)
        df = condense(df)
    if saveas is not None:
        if append and os.path.isfile(saveas):
            with open(saveas, 'a') as fh:
                df.to_csv(fh, header=False, index=False)
        else:
            df.to_csv(saveas, index=False)
    return df


def get_census_for_pids_in_csv(filename, col_name='PID', saveas=None, condense=True, save_uncondensed=True):
    """Runs get_records_for_pids_in_csv, looking for census records. With options to condense results and save.

    saveas (str, optional): a file name to save the outputted DataFrame in CSV format
    condense (bool): whether or not to run condense_census on the data before outputting
    save_uncondensed (bool): if saveas isprovided and condense is True, determines whether to also save uncondensed data
    """
    df_out = get_records_for_pids_in_csv(CENSUS_PTTRN, filename, col_name)
    df_out = condense_and_save(df_out, saveas, condense_census if condense else None, save_uncondensed)
    return df_out


def get_deaths_for_pids_in_csv(filename, col_name='PID', saveas=None, condense=True, save_uncondensed=True):
    """Runs get_records_for_pids_in_csv, looking for death records. With options to condense results and save."""
    df_out = get_records_for_pids_in_csv(DEATH_PTTRN, filename, col_name)
    df_out = condense_and_save(df_out, saveas, condense_death_records if condense else None, save_uncondensed)
    return df_out
