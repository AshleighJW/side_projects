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

def fetch_data(query, cfg, verbose=1):
    ''' 
    Fetch submissions using Pushshift.

    Arguments:
    query: string, containing the querys used to fetch the submissions.
    cfg: dict, containing configuration parameters. Parameter details related to query can be found in Pushshift Documents.
        ['rm_dupe']: string, a field used to remove duplicate submissions. Default: '' (not remove duplicate).
        ['path_save']: a path to save all the resulting files. Default: current directory.
        ['save_suffix']: string containing the suffix of the saving name.
    verbose: Boolean. If verbose=1, print function process.
    '''
    # Default paratemers
    if not 'rm_dupe' in cfg.keys():
        cfg.update({'rm_dupe': ''})
    if not 'path_save' in cfg.keys():
        cfg.update({'path_save': os.getcwd()+'/'})
    cols = cfg['field'].split(',')
    
    try:
        df = pushshift_query(1, cfg['query_type'], query)
        if cfg['rm_dupe'] != '':
            df = df.drop_duplicates(subset=cfg['rm_dupe'], keep='first').reset_index()
        df = df[cfg['field'].split(',')]
        if verbose:
            print('  - '+str(df.shape[0])+' '+cfg['query_type']+'(s) fetched')
        if not os.path.exists(cfg['path_save']):
            os.makedirs(cfg['path_save'])
        df.to_csv(cfg['path_save']+cfg['query_type']+'_'+cfg['save_suffix']+'.csv')
    except:
        if verbose:
            print('  - 0 '+cfg['query_type']+'(s)'+' fetched')
        df = pd.DataFrame(columns=cols)
    return df

    
def fetch_cmts_of_a_subm(df_subm, query, cfg, verbose=1):
    ''' 
    Fetch comments of a specific submission.

    Arguments:
    df_subm: DataFrame of the submission whose comments to fetch from.
    query: string, containing the querys used to further filter the submissions.
    cfg: dict, containing configuration parameters. Parameter details related to query can be found in Pushshift Documents.
        ['rm_dupe']: string, a field used to remove duplicate submissions. Default: '' (not remove duplicate).
    verbose: Boolean. If verbose=1, print function process.
    '''
    # Default paratemers
    if not 'rm_dupe' in cfg.keys():
        cfg.update({'rm_dupe': ''})
    if not 'path_save' in cfg.keys():
        cfg.update({'path_save': os.getcwd()+'/'})
    cols = cfg['field'].split(',')
    
    # Retrive IDs of all comments of this submission
    id_cmts = pushshift_query(0, 'submission', '/comment_ids/'+df_subm['id'])
    if id_cmts.shape[0] == 0:
        return pd.DataFrame(columns=cols)
    str_id_cmt = ''
    for idx in range(id_cmts.shape[0]):
        str_id_cmt += id_cmts.iloc[idx][0] + ','
    str_id_cmt = str_id_cmt[:-1] # Remove the last comma 

    # Request link length control: if too long then do bactch retrive
    # cutoff_len is used to control the request line length (maximum: 8190) when retriving comments using comment ids.
    cutoff_len = 8189-len('https://api.pushshift.io/reddit/comment/search/?ids=filter='+cfg['field'])
    if len(str_id_cmt) <= cutoff_len:
        try:
            df = pushshift_query(1, 'comment', 'ids='+str_id_cmt+query)
        except:
            if verbose:
                print(' --Comment fetch bug; skipping...')
            return pd.DataFrame(columns=cols)
    else: # Fetch comments in batches
        df_cmts = []
        idx_batch = [i for i in range(0, len(str_id_cmt), cutoff_len)]
        for ii in range(0, len(idx_batch)-1):                    
            comma_loc = [pos for pos, char in enumerate(str_id_cmt[idx_batch[ii]:idx_batch[ii+1]]) if char == ',']
            id_batch = str_id_cmt[idx_batch[ii]:max(comma_loc)]
            try:
                df = pushshift_query(1, 'comment', 'ids='+str_id_cmt+query)
                df_cmts.append(df)
                del df
            except:
                print(' --Comment fetch bug; skipping...')
            del comma_loc, id_batch
        # Fetch comments from last batch separately due to unfixed lengtg
        comma_loc = [pos for pos, char in enumerate(str_id_cmt[idx_batch[-1]:]) if char == ',']
        id_batch = str_id_cmt[idx_batch[-1]:max(comma_loc)]
        try:
            df = pushshift_query(1, 'comment', 'ids='+str_id_cmt+query)         
            df_cmts.append(df)
            del df
        except:
            print(' --Comment fetch bug; skipping...')
        del comma_loc, id_batch
        
        # Merge comments fetched in different batches
        try:
            df = pd.concat(df_cmts)
        except:
            print(' --Comment fetch bug; skipping...')
            return pd.DataFrame(columns=cols)
        
    # Remove duplicate entries and save data
    try:
        if cfg['rm_dupe'] != '':
            df = df.drop_duplicates(subset=cfg['rm_dupe'], keep='first').reset_index()
        df[['url', 'subm_id']] = [df_subm['url'], df_subm['id']]
        if verbose:
            print('  - '+str(df.shape[0])+' comment(s) fetched')
    except:
        print(' --Comment fetch bug; skipping...')
        df = pd.DataFrame(columns=cols)
    return df

