# -*- coding: utf-8 -*-
"""
Collection of functions for data cleaning and analysis

@author: santiag0m
"""

import numpy as np
import pandas as pd
import scipy.stats as scs

def join_tables(dfs, set_index=None):
    dfs = list(dfs)
    cum_vars = set()
    if set_index is not None:
        if isinstance(set_index, str):
            set_index = [set_index]*len(dfs)
    for i, df in enumerate(dfs):
        index = ''
        if set_index is not None:
            index = set_index[i]
            df = df.drop_duplicates(index, keep=False)
            df = df.set_index(index)
            df_vars = list(df.columns)
        for v in df_vars:
            if v!= index:
                if v in cum_vars:
                    df = df.drop(v, axis=1)
        cum_vars.update(df_vars)
        if i==0:
            res = df
        else:
            res_idx = res.index
            df_idx = df.index
            t_idx = res_idx.intersection(df_idx)
            res = pd.concat([res.loc[t_idx,:], df.loc[t_idx,:]], 
                            axis=1, sort=True)
    return res

def chi2(df_col1, df_col2):
    freq1 = df_col1.astype('object').value_counts()
    freq2 = df_col2.astype('object').value_counts()
    
    idxs1 = list(freq1.index)
    idxs2 = list(freq2.index)
    
    idxs = [x for x in idxs1 if x not in idxs2]
    
    for idx in idxs:
        temp = pd.Series([0],index=[idx])
#        temp.index = temp.index.astype('object')
        freq2 = freq2.append(temp)
        
    idxs = [x for x in idxs2 if x not in idxs1]
    for idx in idxs:
        temp = pd.Series([0],index=[idx])
#        temp.index = temp.index.astype('object')
        freq1 = freq1.append(temp)
    
    freq1.sort_index()
    freq2.sort_index()
    
    return scs.chi2_contingency([freq1, freq2])[1]

def t_test(df_col1, df_col2):
    return scs.ttest_ind(df_col1, df_col2)[1]

def check_boolean(srs, null_val = False):
    
    def obj2bool(srs, null_val):
        # Is supposed to be a string
        srs = srs.str.lower()
        vc = srs.value_counts().values
        
        bool_eq = {'y':True, 'yes':True, 
                   's':True, 'si':True, 
                   't':True, 'true':True,
                   'n':False, 'no':False,
                   'f':False, 'false':False,
                   '1':True, '0':False}
        
        if (len(vc)==2):
            # Only two posible values -> boolean variable
            try:
                srs.loc[~srs.isna()] = srs.loc[~srs.isna()].apply(lambda x: bool_eq[x])
                srs = srs.fillna(null_val)
                return srs
            except:
                return None
        else:
            return None
        
    def num2bool(srs, null_val):
        vc = srs.value_counts().values
        
        if (len(vc)==2):
            # Only two posible values -> boolean variable
            try:
                srs.loc[~srs.isna()] = srs.loc[~srs.isna()].apply(lambda x: True if x!=0 else False)
                srs = srs.fillna(null_val)
                return srs
            except(KeyError):
                print('%s has two possible values but could not be converted' % srs.name)
                return None
        else:
            return None
    
    if (srs.dtype == np.object):
        return obj2bool(srs, null_val)
    elif (srs.dtype == np.int64 or srs.dtype == np.float64):
        return num2bool(srs, null_val)
    else:
        return None
    

def check_datetime(srs):
    
    try:
        srs = pd.to_datetime(srs, infer_datetime_format=True)
        return srs
    except:
        return None
    
def check_numeric(srs, tol=0.05):
    
    if(srs.dtype == np.float64 or srs.dtype == np.int64 or srs.dtype == np.uint8):
        return srs
    if(srs.dtype != np.object):
        return None
    
    srs = srs.str.replace(',','.')
    try:
        srs = pd.to_numeric(srs, errors='coerce')
        inv = np.sum(srs.isna().values)
        inv = inv/len(srs)
        if(inv==0):
            return srs
        elif (inv < tol):
            print('ORDERLY WARNING: {} : {:.2f}% of data was assigned NaN'.format(srs.name, inv*100))
            return srs
        else:
            if (inv < 1):
                print('ORDERLY WARNING: {} : Not numeric due to too much NaN values {:.2f}%'.format(srs.name, inv*100))
            return None
    except:
        return None

def boolean2int(df):
    dfn = df.copy()
    dts = dfn.dtypes
    bool_vars = list(dts[dts=='bool'].index)
    
    for var in bool_vars:
        dfn[var] = dfn[var].apply(lambda x: 1 if x else 0)
        dfn[var] = dfn[var].astype('uint8')
    
    return dfn
    
    

def assign_types(df, verbose=True):
    
    dfn = df.copy()
    
    unassigned = []
    
    try:
        cols = list(dfn.columns)
        
        for c in cols:
            srs = dfn.loc[:,c].copy()
            
            assigned = False
            btemp = check_boolean(srs)
            ntemp = None
            dtemp = None
            
            if(btemp is not None):
                dfn.loc[:,c] = btemp.astype('bool')
                if verbose:
                    print('%s was converted to boolean' % c)
                assigned = True
            else:
                ntemp = check_numeric(srs)
                
            if(ntemp is not None):
                dfn.loc[:,c] = ntemp
                if verbose:
                    print('%s was converted to numeric' % c)
                assigned = True
            else:
                dtemp = check_datetime(srs)
            
            if(dtemp is not None):
                dfn.loc[:,c] = dtemp
                if verbose:
                    print('%s was converted to datetime' % c)
                assigned = True
            
            if(not assigned):
                unassigned.append(c)
                
        
        if len(unassigned)>0:
            if verbose:
                    print('\n')
                    print('--------------------------------------------')
                    print('UNASSIGNED VARIABLES:')
                    print('--------------------------------------------')
                    print('\n')
                    for c in unassigned:
                        print(c)
        
        return dfn
    except (AttributeError):
        # Working directly with series
        btemp = check_boolean(dfn)
        ntemp = None
        dtemp = None    
        
        if(btemp is not None):
            if verbose:
                    print('%s was converted to boolean' % dfn.name)
            return btemp.astype('bool')
        else:
            ntemp = check_numeric(dfn)
            
        if(ntemp is not None):
            if verbose:
                    print('%s was converted to numeric' % c)
            return ntemp
        else:
            dtemp = check_datetime(dfn)
        
        if(dtemp is not None):
            if verbose:
                    print('%s was converted to datetime' % c)
            return dtemp
        
                
        
        
        
        
        