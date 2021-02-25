#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import requests
import json
import pandas as pd

def pushshift_query(is_search, query_type, query):
    if is_search:
        url = 'https://api.pushshift.io/reddit/'+query_type+'/search/?'+query
    else:
        url = 'https://api.pushshift.io/reddit/'+query_type+query
    data = requests.get(url)
    data = json.loads(data.text)
    return pd.DataFrame(data['data'])

def fetch_data(query, cfg):
    ''' 
    Fetch submissions using Pushshift.

    Arguments:
    query: string, containing the querys used to fetch the submissions.
    cfg: dict, containing configuration parameters. Parameter details related to query can be found in Pushshift Documents.
        ['rm_dupe']: string, a field used to remove duplicate submissions. Default: '' (not remove duplicate).
        ['path_save']: a path to save all the resulting files. Default: current directory.
        ['save_suffix']: string containing the suffix of the saving name.
    '''
    # Default paratemers
    if not 'rm_dupe' in cfg.keys():
        cfg.update({'rm_dupe': ''})
    if not 'path_save' in cfg.keys():
        cfg.update({'path_save': os.getcwd()+'/'})
    
    try:
        df = pushshift_query(1, cfg['query_type'], query)
        if cfg['rm_dupe'] != '':
            df = df.drop_duplicates(subset=cfg['rm_dupe'], keep='first').reset_index()
        df = df[cfg['field'].split(',')]
        print('  - '+str(df.shape[0])+' '+cfg['query_type']+'(s) fetched')
        if not os.path.exists(cfg['path_save']):
            os.makedirs(cfg['path_save'])
        df.to_csv(cfg['path_save']+cfg['query_type']+'_'+cfg['save_suffix']+'.csv')
        return df
    except:
        print('  - 0 '+cfg['query_type']+'(s)'+' fetched')
        df = []
        return df

