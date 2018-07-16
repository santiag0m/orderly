# -*- coding: utf-8 -*-
"""
Collection of functions for data cleaning

@author: santiag0m
"""

import numpy as np
import pandas as pd

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
                srs.loc[~srs.isnull()] = srs.loc[~srs.isnull()].apply(lambda x: bool_eq[x])
                srs.loc[srs.isnull()]=null_val
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
                srs.loc[~srs.isnull()] = srs.loc[~srs.isnull()].apply(lambda x: True if x!=0 else False)
                srs.loc[srs.isnull()]=null_val
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
    
def check_numeric(srs):
    
    if(srs.dtype == np.float64 or srs.dtype == np.int64):
        return srs
    if(srs.dtype != np.object):
        return None
    
    srs = srs.str.replace(',','.')
    try:
        srs = pd.to_numeric(srs)
        return srs
    except:
        return None


def assign_types(df):
    
    dfn = df.copy()
    
    unassigned = []
    
    try:
        cols = list(dfn.columns)
        
        for c in cols:
            srs = dfn[c]
            
            assigned = False
            btemp = check_boolean(srs)
            ntemp = None
            dtemp = None
            
            if(btemp is not None):
                dfn.loc[:,c] = btemp.astype('bool')
                print('%s was converted to boolean' % c)
                assigned = True
            else:
                ntemp = check_numeric(srs)
                
            if(ntemp is not None):
                dfn.loc[:,c] = ntemp
                print('%s was converted to numeric' % c)
                assigned = True
            else:
                dtemp = check_datetime(srs)
            
            if(dtemp is not None):
                dfn.loc[:,c] = dtemp
                print('%s was converted to datetime' % c)
                assigned = True
            
            if(not assigned):
                unassigned.append(c)
                
        
        if len(unassigned)>0:
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
            print('%s was converted to boolean' % dfn.name)
            return btemp.astype('bool')
        else:
            ntemp = check_numeric(dfn)
            
        if(ntemp is not None):
            print('%s was converted to numeric' % c)
            return ntemp
        else:
            dtemp = check_datetime(dfn)
        
        if(dtemp is not None):
            print('%s was converted to datetime' % c)
            return dtemp
        
                
        
        
        
        
        