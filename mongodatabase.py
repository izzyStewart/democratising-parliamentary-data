# Importing modules. 
import pymongo
import pandas as pd
import numpy as np
import csv
import json

def load_data(csv):
    """Function to load csv to pandas.
    """
    df = pd.read_csv(csv, sep=',', error_bad_lines=False)
    return df

def clean_df(df):
    """Function to remove 'unnamed' columns.
    """
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    return df

def df_to_json(df):
    """Function to convert dataframe to json.
    """
    json_data = json.loads(df.to_json(orient='records'))
    return json_data

def insert_data(col_name, json_data):
    """Function to insert data into collection.
    """
    col_name.insert_many(json_data)
    return col_name

def party_sort(old_col, new_col, party):
    """Function to create new collections sorted by party.
    """
    party_votes = old_col.find({"party":party})
    new_col.insert_many(party_votes)
    return new_col
    

class CreateCollections(object):
    """Class created mongodb collections from imported data. 
    """
    
    def __init__(self, database):
        """Mongo database is defined here.
        """
        self.database = database

    def connect_mongo(self):
        """Function to connect to mongodb.
        """
        # Creating connection.
        client = pymongo.MongoClient()
        # Clearing database (if database exists).
        client.drop_database(self.database)
        # Assigning database to variable.
        db = client[self.database]
        return db
    
    def insert_all_data(self, db):
        """Function to insert all csv data into new database.
        """
        # Assign variables to collections
        mps = db.mps
        votes_int = db.votes_int
        votes = db.votes
        votes_id = db.votes_id
        # Load csv data
        mps_df = load_data('DATA/MPS-ID-2001-5.csv')
        votes_int_df = load_data('DATA/VOTES-INT.csv')
        votes_df = load_data('DATA/VOTING-2001-5.csv')
        votes_id_df = load_data('DATA/VOTES-INT-ID.csv')
        # Clean data
        votes_int_df = clean_df(votes_int_df)
        votes_df = clean_df(votes_df)
        votes_id_df = clean_df(votes_id_df)
        # Data to json
        mps_json = df_to_json(mps_df)
        votes_int_json = df_to_json(votes_int_df)
        votes_json = df_to_json(votes_df)
        votes_id_json = df_to_json(votes_id_df)
        # Insert json into newly created collections
        insert_data(mps, mps_json)
        insert_data(votes_int, votes_int_json)
        insert_data(votes, votes_json)
        insert_data(votes_id, votes_id_json)

    
class PerformAggregations(object):
    """Class performs aggregations, creating new collections from analyses. 
    """
    
    def __init__(self):
        """Initialising class.
        """
    
    def embed_votes(self, db):
        """Function to embed vote collection into mp id collection.
        """
        # Assign variables to collections
        mps = db.mps
        votes_int = db.votes_int
        mps_votes = db.mps_votes
        # Pipeline aggregation.
        pipeline = [{'$lookup': 
             {'from' : 'votes_int',
              'localField' : 'member_id',
              'foreignField' : 'member_id',
              'as' : 'votes'}},
            { '$out' : 'mps_votes' }
             ]
        output = mps.aggregate(pipeline)

    def clean_array(self, db):
        """Function to unwind and embedded array and remove unneeded fields.
        """
        # Assign variables to collections
        mps_votes = db.mps_votes
        # Pipeline aggregation two
        pipeline = [{'$unwind': '$votes'},{'$out': 'mps_votes'}]
        output = mps_votes.aggregate(pipeline)
        # Remove un-needed fields
        mps_votes.update({}, {'$unset': {'votes._id':1}}, multi=True)
        mps_votes.update({}, {'$unset': {'votes.member_id':1}}, multi=True)
        
    def party_votes_sort(self, db):
        """Function to sort voting data by the three main parties.
        """
        # Assign variables to collections
        mps_votes = db.mps_votes
        votes_id = db.votes_id
        lab = db.mp_lab
        con = db.mp_con
        lib_dem = db.mp_lib_dem
        ld_votes_int = db.ld_votes_int
        con_votes_int = db.con_votes_int
        lab_votes_int = db.lab_votes_int
        
        # Sort mps_votes by party
        party_sort(mps_votes, lab, "Labour")
        party_sort(mps_votes, con, "Conservative")
        party_sort(mps_votes, lib_dem, "Liberal Democrat")

        party_sort(votes_id, lab_votes_int, "Labour")
        party_sort(votes_id, con_votes_int, "Conservative")
        party_sort(votes_id, ld_votes_int, "Liberal Democrat")
        
        
    
