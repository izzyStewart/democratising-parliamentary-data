# Importing modules. 
import io
import requests
import pandas as pd
import csv
import mnis
import datetime
from datetime import timedelta
import numpy as np
import multiprocessing
import sys


def request_url(url):
    """Function to read csv from data.parliament site. If the request fails, the error messages below are printed. 
    """
    try:
        # Read csv to pandas, ignoring lines with errors.
        df = pd.read_csv(url,error_bad_lines=False)
        return df
    except IOError as e: 
        # To handle IOError.
        print('I/O error({0}): {1}'.format(e.errno, e.strerror))
    except: 
        # To handle other exceptions such as attribute errors.
        print('Unexpected error:', sys.exc_info()[0])

        
def get_mp_votes(mp_id,vote):
    """Function to retrieve all votes from a single member of parliament. 
    """
    # Using the request_url function with the API query. 
    mp_df = request_url('http://lda.data.parliament.uk/commonsdivisions/'+vote+'.csv?mnisId='+str(mp_id))
    # A for loop pull data from multiple pages, until all votes from that MP are collected.
    for i in range (1, 20):  
        mp_df2 = request_url('http://lda.data.parliament.uk/commonsdivisions/'+vote+'.csv?_page='+str(i)
                          +'&mnisId='+str(mp_id))
        mp_df = mp_df.append(mp_df2,ignore_index=True)
        mp_df[mp_id] = vote
    return mp_df


def mp_vote_type(mp_id):
    """Function to yes and no votes using the above function and group the results. 
    """
    no_votes = get_mp_votes(mp_id,'no')
    yes_votes = get_mp_votes(mp_id,'aye')
    frames = [no_votes,yes_votes]
    all_votes = pd.concat(frames)
    return all_votes


def get_all_votes(mps_ids):
    """Function to get all votes from all mps using the above functions. 
    """
    df = pd.DataFrame(columns=['uin', 'title','date'])
    # Loops through the list of mps in parliament on the selected dates.
    for i in range(len(mps_ids)):
        df1 = mp_vote_type(mps_ids[i])
        if df1.empty == False:
            df = df.merge(df1, on=['uin','title','date'], how='outer')
        else:
            df[mps_ids[i]] = np.NaN
    # Saves the results to csv files in a folder.
    with open('DATA/votes-segment/'+str(mps_ids[i])+'.csv','a') as f:
        df.to_csv(f,header=True,index=False)

        
def chunks(l, num):
    """Function defines chunks for retreiving votes, as the process is quite slow. 
    """
    return [l[i:i+num] for i in range(0, len(l), num)]


def list_split(mp_list, num):
    """Function splits the chunks into arrays for multiprocessing. 
    """
    split = chunks(mp_list,num) #split list into parts
    chunk = split[1]
    array = np.array_split(chunk, 6)
    return array


class MakeVoteFiles(object):
    """Class creates csv files containing collections of votes from the members of parliament. 
    """
    
    def __init__(self, mps_ids, number):
        """Variables needed for class stored here.
        """
        self.mps_ids = mps_ids
        self.number = number

    def build_vote_files(self):
        """Function builds the vote csv files using multiprocessing across two processes.
        """
        mp_list = list_split(self.mps_ids, self.number)
   
        pool = multiprocessing.Pool(processes=2)
        pool.map(get_all_votes, mp_list)
        pool.close() 
        pool.join()
        # Print statement to show user the votes have been collected.
        print("Multiprocessing finished. Votes from "+str(self.number)+" mps collected. Check folder DATA/votes-segment for results.")
        