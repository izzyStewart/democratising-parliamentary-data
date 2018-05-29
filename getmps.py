# Importing modules.
import io
import requests
import pandas as pd
import csv
import mnis
import datetime
from datetime import timedelta

def get_mps(start,end):
    """Function to get data from members in parliament between the start and end dates set. Uses 'mnis' module, 
    that pulls data from the 'Members Names Information Service.' 
    """
    members = mnis.getCommonsMembersBetween(start, end)
    sd = mnis.getSummaryDataForMembers(members, end)
    df = pd.DataFrame(sd)
    # Removes mps not serving at the end date. This insures mps are active in parliament between the start and end dates.
    df = df[df.party.str.contains("Not serving") == False]
    return df

def set_column_types(df, col, col_type):
    """Function to set columns to correct dtypes for analyses. 
    """
    df[col] = df[[col]].apply(lambda x: x.astype(col_type))
    return df


class MakeMpDf(object):
    """Class creates dataframe from the data pulled in from 'Members Names Information Service.'
    """
    
    def __init__(self, start_date, end_date):
        """Variables for start and end dates stored here.
        """
        self.start_date = start_date
        self.end_date = end_date

    def create_df(self):
        """Functiion creates dataframe using functions listed above.
        """
        # Calling the 'get_mp' function to get mp details from dates selected
        mp_df = get_mps(self.start_date,self.end_date)
        
        # Changing df columns to correct 'dtypes' needed for project
        set_column_types = (mp_df, 'constituency', 'str')
        set_column_types = (mp_df, 'days_service', 'float')
        set_column_types = (mp_df, 'gender', 'str')
        set_column_types = (mp_df, 'list_name', 'str')
        set_column_types = (mp_df, 'member_id', 'int')
        set_column_types = (mp_df, 'party', 'str')
        
        # Set index and return dataframe.
        mp_df = mp_df.set_index('member_id')
        return mp_df