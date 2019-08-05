import requests
import re
import pandas as pd

import authenticate


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
        pass

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


class FamilySearchSourcer:

    def __init__(self):
        self.key = authenticate.read_auth_key()
        self.headers = {
            'Authorization': 'Bearer {}'.format(self.key),
            'Accept': 'application/json'
        }

    def get_attached_sources(self, pid):
        url = f'https://api.familysearch.org/platform/tree/persons/{pid}/sources'
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            return response.json()['sourceDescriptions']
        elif response.status_code == 401:
            # Reauthenticate & retry
            self.__init__()
            return self.get_attached_sources(pid)
        elif response.status_code == 204:
            return []  # no results
        else:
            raise Exception(f'Error: HTTP status code {response.status_code}')

    def search_for_sources(self, pid):
        url = (f'https://api.familysearch.org/platform/tree/persons/{pid}/matches?' +
                'collection=https://familysearch.org/platform/collections/records')
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            return response.json()['entries']
        elif response.status_code == 401:
            # Reauthenticate & retry
            self.__init__()
            return self.search_for_sources(pid)
        elif response.status_code == 204:
            return []  # no results
        else:
            raise Exception(f'Error: HTTP status code {response.status_code}')

    def find_census(self, pid):
        arkids = []
        # Get Ark IDs for censuses already attached
        sources = self.get_attached_sources(pid)
        for source in sources:
            if re.search(r'[Cc]ensus', ' '.join(x['value'] for x in source['titles'])):
                arkids.append(re.search(r'[^:]+$', source['about']).group())
        # Search for more records
        sources = self.search_for_sources(pid)
        for source in sources:
            if re.search(r'[Cc]ensus', source['title']):
                arkids.append(re.search(r'[^:]+$', source['id']).group())
        return arkids

    def process_census(self, arkid):
        url = f'https://api.familysearch.org/platform/records/personas/{arkid}'
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            # Create dictionary based on JSON response
            source_dict, c = iterate(response.json())
            # Convert to Pandas DataFrame
            for k in source_dict.keys():
                source_dict[k] = source_dict[k].split(';')
            df = pd.DataFrame.from_dict(source_dict, orient='index').transpose()
            df['is_person'] = [int(i == c) for i in range(len(df))]
            df['ark_id'] = [arkid]*len(df)
            return df
        elif response.status_code == 401:
            # Reauthenticate & retry
            self.__init__()
            return self.process_census(arkid)
        elif response.status_code == 204:
            return dict()  # no results
        else:
            raise Exception(f'HTTP status code {response.status_code}. ArkID is {arkid}.')

    def get_census_for_pid(self, pid):
        arkids = self.find_census(pid)
        df = pd.concat((self.process_census(arkid) for arkid in arkids))
        return df


def get_census_for_pids_in_csv(filename, col_name='PID', saveas=None):
    fss = FamilySearchSourcer()
    df = pd.concat((fss.get_census_for_pid(pid) for pid in fss[col_name])).reset_index(drop=True)
    if saveas is not None:
        df.to_csv(saveas, index=False)
    return df
