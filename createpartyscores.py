# Importing modules.
import pymongo
import pandas as pd
import numpy as np
import csv
import json

def replace_empty_val(coll):
    """Function to update all empty values to '0.' 
    """
    coll.update({'value.1':{'$exists': False}},{'$set': {'value.1': 0}},multi=True)
    coll.update({'value.0':{'$exists': False}},{'$set': {'value.0': 0}},multi=True)
    coll.update({'value.-1':{'$exists': False}},{'$set': {'value.-1': 0}},multi=True)
    return coll


def party_scores(coll, out_coll, party_total):
    """Function to scores for each vote for each party. 
    """
    # Creating aggregation pipeline to calculate score.
    pipeline = [{'$project': 
             {'item': 1, 'score':
              {'$divide': [{'$subtract':['$value.1', '$value.-1']},party_total]}}},
            { '$out' : out_coll }]
    output = coll.aggregate(pipeline)
    return output

# Create collections defining the majority outcome for each vote for each party
def majority_scores(vote_results, majority_score):
    """Function to give majority scores for each vote for each party. 
    """
    # Collecting yes majorities
    yes = vote_results.find({ '$expr': { '$gt': [ '$value.1' , '$value.-1' ] }})
    majority_score.insert_many(yes)
    majority_score.update({}, {'$set': {'score': 1}}, multi=True)
    # Collecting cases where the votes were equal or no mp in that party voted
    equal = vote_results.find({ '$expr': { '$eq': [ '$value.-1' , '$value.1' ] }})
    majority_score.insert_many(equal)
    majority_score.update({'score':{'$exists': False}},{'$set': {'score': 0}},multi=True)
    # Collecting no majorities
    no = vote_results.find({ '$expr': { '$gt': [ '$value.-1' , '$value.1' ] }})
    majority_score.insert_many(no)
    majority_score.update({'score':{'$exists': False}},{'$set': {'score': -1}},multi=True)
    majority_score.update({'value': {'$exists': True}}, {'$unset': {'value': True}}, multi = True)


class GetPartyScores(object):
    """Class to get party scores for each vote. 
    """
    
    def __init__(self):
        """Initialising class.
        """
    
    def replace_empty_votes(self, db):
        """Function to fill empty values.
        """
        # Assign variables to collections
        lab_vote_results = db.lab_vote_results
        con_vote_results = db.con_vote_results
        ld_vote_results = db.ld_vote_results
        # Calling replace_empty_val function
        replace_empty_val(lab_vote_results)
        replace_empty_val(con_vote_results)
        replace_empty_val(ld_vote_results)
        
    def create_party_scores(self, db):
        """Function to create party scores
        """
        # Assign variables to collections
        lab_vote_results = db.lab_vote_results
        con_vote_results = db.con_vote_results
        ld_vote_results = db.ld_vote_results
        lab_score = db.lab_score
        con_score = db.con_score
        ld_score = db.ld_score
        # Calling create_scores function
        party_scores(lab_vote_results, 'lab_score', 413)
        party_scores(ld_vote_results, 'ld_score', 55)
        party_scores(con_vote_results, 'con_score', 160)
        
    def create_majority_scores(self, db):
        """Function to create majority scores
        """
        # Assign variables to collections
        lab_vote_results = db.lab_vote_results
        con_vote_results = db.con_vote_results
        ld_vote_results = db.ld_vote_results
        lab_maj_score = db.lab_maj_score
        con_maj_score = db.con_maj_score
        ld_maj_score = db.ld_maj_score
        # Calling create_scores function
        majority_scores(lab_vote_results, lab_maj_score)
        majority_scores(con_vote_results, con_maj_score)
        majority_scores(ld_vote_results, ld_maj_score)
        



