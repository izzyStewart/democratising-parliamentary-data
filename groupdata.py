# Importing modules.
import csv
import numpy as np
import time
import datetime
from datetime import timedelta
import os
import pandas as pd 
from multiprocessing import Pool
from functools import reduce


def read_csv(filename):
    """Function to read csv file, remove un-needed fields and return as a dataframe. 
    """
    df = pd.read_csv("DATA/votes-all/"+filename, sep=',',error_bad_lines=False, dtype='unicode')
    df = df.filter(regex='^(?!division.*)')
    df = df.filter(regex='^(?!uri.*)')
    df = df.groupby('uin').apply(lambda x: x.ffill().bfill()).drop_duplicates()
    return df


def read_all_files():
    """Function uses multiprocessing to read all csv files and merge to one dataframe.  
    """
    # Setting up pool with 8 processes.
    pool = Pool(processes=8) 

    # Get the list of file names.
    path = "DATA/votes-all/"
    files = os.listdir(path)
    file_list = [filename for filename in files if filename.split('.')[1]=='csv']

    # Using the pool to map the file names to dataframes.
    df_list = pool.map(read_csv, file_list)
    pool.close() 
    pool.join()
    
    # Reducing list of dataframes to single dataframe.
    df_final = reduce(lambda left,right: pd.merge(left,right,on=['uin','title','date'],how='outer'), df_list)
    return df_final 

def set_column_types(df, col, col_type):
    """Function sets the column to correct dtype for analyses.  
    """
    df[col] = df[[col]].apply(lambda x: x.astype(col_type))
    return df


class CreateVoteDf(object):
    """Classes uses above functions to create single dataframe of all mp votes. 
    """
    
    def __init__(self, start_date, end_date):
        """Variables needed for class stored here.
        """
        self.start_date = start_date
        self.end_date = end_date

    def build_vote_df(self, start_date, end_date):
        """Function builds dataframe.
        """
        df_merged = read_all_files()
        print("Multiprocessing finished. Now adjusting dataframe...")
        
        # Sets column types
        df_merged['date'] = pd.to_datetime(df_merged['date'])
        set_column_types(df_merged,'title','str')
        set_column_types(df_merged,'uin','str')
        df_merged = df_merged.set_index('uin')
        # Remove dates of votes outside the selected dates.
        df_merged = df_merged[(df_merged['date'] > start_date) & (df_merged['date'] < end_date)]
        # Change NAN values to 'did not vote'
        df_merged = df_merged.replace(np.nan, 'did not vote', regex=True)
        # Print to show results have been collected return completed dataframe.
        print("Completed. All files now read in and combined in one dataframe.")
        return df_merged
    

class TransformVoteDf(object):
    """Class to transform vote dataframe using pivot and changing the votes values to numeric. 
    """
    
    def __init__(self, mp_df):
        """Variables needed for class stored here.
        """
        self.mp_df = mp_df

    def pivot_df(self, votes_df):
        """Function to pivot dataframe (needed for later analyses).
        """
        df = votes_df
        df = df.drop('title', 1)
        df = df.drop('date', 1)
        df.rename(columns={'uin':'index'}, inplace=True)

        df.set_index('index',inplace=True)
        # Using transpose to pivot values changing rows to columns.
        df = df.transpose().reset_index().rename(columns={'index':'member_id'})
        return df
    
    def votes_to_int(self, pivot_df):
        """Function to change string values to numeric (needed for later analyses).
        """
        df = pivot_df
        # Votes to numeric: no is -1, yes is 1 and non-votes are 0.
        df = df.replace(['^no$'], [-1], regex=True)
        df = df.replace(['did not vote'], [0], regex=True)
        df = df.replace(['aye'], [1], regex=True)
        set_column_types(df,'member_id','int')        
        return df

    def merge_mp_id(self, df_int):
        """Function merges vote dataframe with mp id dataframe (needed for later analyses).
        """        
        mp = self.mp_df
        mp = mp.reset_index()
        merge = df_int.merge(mp, on=['member_id'], how='outer')
        merge.set_index('index',inplace=True)
        merge = merge.reset_index(drop=True)
        return merge
