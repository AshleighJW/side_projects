#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import requests
import json
import pandas as pd

def getPushshiftData(is_search, query, query_type, query_fields, sort_type='num_comments', query_num=100):
    if is_search:
        url = 'https://api.pushshift.io/reddit/'+query_type+'/search/?'+query
    else:
        url = 'https://api.pushshift.io/reddit/'+query_type+query
    if query_fields != '':
        url += '&filter='+query_fields
    if sort_type != '':
        url += '&sort_type='+sort_type+'&sort=desc'
    if (type(query_num) == int) and (query_num > 0):
        url += '&size='+str(query_num)
    data = requests.get(url)
    data = json.loads(data.text)
    return pd.DataFrame(data['data'])

def pushshift_fetch(query, cfg):
    ''' 
    Fetch submissions and their comments using Pushshift.

    Arguments:
    query: string, containing the querys used to fetch the submissions.
    cfg: dict, containing configuration parameters. Parameter details related to query can be found in Pushshift Documents.
        ['subm_field']: string, submission fields to fetch. Default: 'id,title,selftext'.
        ['cmt_field']: string, comment fields to fetch. Default: 'body'.
        ['subm_rm_dupe']: string, a field used to remove duplicate submissions. Default: '' (not remove duplicate).
        ['cmt_rm_dupe']: string, a field used to remove duplicate comments. Default: '' (not remove duplicate).
        ['num_fetch']: int, number of submission to fetch. Default: 500.
        ['path_save']: a path to save all the resulting files. Default: current directory.
        ['save_suffix']: string containing the suffix of the saving name.
    '''
    # Default paratemers
    if not 'subm_field' in cfg.keys():
        cfg.update({'subm_field': 'id,title,selftext'})
    if not 'cmt_field' in cfg.keys():
        cfg.update({'cmt_field': 'body'})
    if not 'subm_rm_dupe' in cfg.keys():
        cfg.update({'subm_rm_dupe': ''})
    if not 'cmt_rm_dupe' in cfg.keys():
        cfg.update({'cmt_rm_dupe': ''})
    if not 'num_fetch' in cfg.keys():
        cfg.update({'num_fetch': 100})
    if not 'path_save' in cfg.keys():
        cfg.update({'path_save': os.getcwd()+'/'})
    
    
    # cutoff_len is used to control the request line length (maximum: 8190) when retriving comments using comment ids.
    cutoff_len = 8189-len('https://api.pushshift.io/reddit/comment/search/?ids=filter='+cfg['cmt_field'])
    
    df_subm = getPushshiftData(1, query, 'submission', cfg['subm_field'])
    if cfg['subm_rm_dupe'] != '':
        df_subm = df_subm.drop_duplicates(subset=cfg['subm_rm_dupe'], keep='first').reset_index()

    print(' - Fetching comments')
    query_cmt = []
    if df_subm.shape[0] > 0:
        for n in range(0, df_subm.shape[0]):
            if df_subm['num_comments'][n] == 0:
                continue
            else:
                id_cmts = getPushshiftData(0, '/comment_ids/'+df_subm['id'][n], 'submission', '', '', '')[0]
                str_id_cmt = ''
                for id_cmt in id_cmts:
                    str_id_cmt += str(id_cmt) + ','
                str_id_cmt = str_id_cmt[:-1] # Remove the last comma 

                # Request link length control: if too long then do bactch retrive
                if len(str_id_cmt) <= cutoff_len:
                    try:
                        df_cmt = getPushshiftData(1, 'ids='+str_id_cmt, 'comment', cfg['cmt_field'], '', '')
                        if cfg['cmt_rm_dupe'] != '':
                            df_cmt = df_cmt.drop_duplicates(subset=cfg['cmt_rm_dupe'], keep='first').reset_index()
                        query_cmt.append(df_cmt)
                        del df_cmt
                    except:
                        print(' --Comment fetch bug; skipping... Comment index: '+str(n)+'/'+str(df_subm.shape[0]))
                else:
                    idx_batch = [i for i in range(0, len(str_id_cmt), cutoff_len)]
                    for ii in range(0, len(idx_batch)-1):                    
                        idx_temp = [pos for pos, char in enumerate(str_id_cmt[idx_batch[ii]:idx_batch[ii+1]]) if char == ',']
                        id_batch = str_id_cmt[idx_batch[ii]:max(idx_temp)]
                        try:
                            df_cmt = getPushshiftData(1, 'ids='+id_batch, 'comment', cfg['cmt_field'], '', '')
                            if cfg['cmt_rm_dupe'] != '':
                                df_cmt = df_cmt.drop_duplicates(subset=cfg['cmt_rm_dupe'], keep='first').reset_index()
                            query_cmt.append(df_cmt)
                            del df_cmt
                        except:
                            print(' --Comment fetch bug; skipping... Comment index: '+str(n)+'/'+str(df_subm.shape[0]))
                        del idx_temp, id_batch
                    idx_temp = [pos for pos, char in enumerate(str_id_cmt[idx_batch[-1]:]) if char == ',']
                    id_batch = str_id_cmt[idx_batch[-1]:max(idx_temp)]
                    try:
                        df_cmt = getPushshiftData(1, 'ids='+id_batch, 'comment', cfg['cmt_field'], '', '')
                        if cfg['cmt_rm_dupe'] != '':
                            df_cmt = df_cmt.drop_duplicates(subset=cfg['cmt_rm_dupe'], keep='first').reset_index()
                        query_cmt.append(df_cmt)
                        del df_cmt
                    except:
                        print(' --Comment fetch bug; skipping... Comment index: '+str(n)+'/'+str(df_subm.shape[0]))
                    del idx_batch, idx_temp, id_batch
                del id_cmts, str_id_cmt, id_cmt

    print(' - Saving data')
    if not os.path.exists(cfg['path_save']):
        os.makedirs(cfg['path_save'])
    df_subm.to_csv(cfg['path_save']+'subm_'+cfg['save_suffix']+'.csv')
    df_cmt = pd.concat(query_cmt).reset_index()
    df_cmt.to_csv(cfg['path_save']+'cmt_'+cfg['save_suffix']+'.csv')
    del df_subm, df_cmt, query_cmt

    print(' - Fetching data has finished!')