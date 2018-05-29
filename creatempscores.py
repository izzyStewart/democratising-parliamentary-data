# Importing modules
import pandas as pd
import numpy as np
import csv
import io
import mnis
import datetime
from datetime import timedelta
import sys
import json
from functools import reduce

def read_json(file):
    """Function to read json
    """
    df = pd.read_json(file, lines=True)
    return df

def read_parties(file):
    """Function to read in party json files and set to correct format for analyses.
    """
    df = read_json(file)
    df['value'] = df['value'].astype(str)
    df['value'] = df['value'].str.split("':").str[1]
    df['value'] = df['value'].str.split('}').str[0]
    df['value'] = df['value'].astype(float)
    df.rename(columns={'_id':'index'}, inplace=True)
    df.set_index('index',inplace=True)
    df = df.transpose().reset_index()
    df.set_index('index',inplace=True)
    return df

def read_mps(file):
    """Function to read in mp json files and set to correct format for analyses.
    """
    df = read_json(file)
    df.drop(['_id', 'constituency','date_of_birth','days_service',
             'first_start_date','gender','list_name','party'], axis=1, inplace=True)
    df.rename(columns={'member_id':'index'}, inplace=True)
    df.set_index('index',inplace=True)
    return df   

def get_scores(all_scores, col):
    """Function to calculate loyalty scores for each mp.
    """
    score_total = all_scores.sum(axis=1) 
    score_total = score_total.to_frame(name=None)
    score_total = score_total.reset_index()
    non_zeros = all_scores.astype(bool).sum(axis=1)
    non_zeros = non_zeros.to_frame(name=None)
    non_zeros = non_zeros.reset_index()
    df_count = score_total.merge(non_zeros, on=['index'], how='outer')
    df_count[col] = np.where(df_count['0_y'] < 1, df_count['0_y'], df_count['0_x']/df_count['0_y'])
    df_count.drop(['0_x', '0_y'], axis=1, inplace=True)
    return df_count

def mp_vote_scores(mp_df, votes_df):
    """Function to calculate loyalty scores for each vote.
    """
    df = pd.DataFrame(mp_df.values*votes_df.values, columns=mp_df.columns, index=mp_df.index)
    return df

def party_loyalty_vote(mp_scores, column):
    """Function to create party loyalty dataframe.
    """
    pivot_df = mp_scores.transpose()
    vote_total_scores = get_scores(pivot_df, column)
    return vote_total_scores

def cols_to_norm(df, col):
    """Function to normalise scores for analyses.
    """
    df[[col]] = df[[col]].apply(lambda x: (x - x.min()) / (x.max() - x.min()))
    return df

    
# Read in party mp vote data
con_mps = read_mps('DATA/mongo-db/con_votes_int.json')
lab_mps = read_mps('DATA/mongo-db/lab_votes_int.json')
ld_mps = read_mps('DATA/mongo-db/ld_votes_int.json')

# Read in party vote score data
con_score = read_parties('DATA/mongo-db/con_score.json')
lab_score = read_parties('DATA/mongo-db/lab_score.json')
ld_score = read_parties('DATA/mongo-db/ld_score.json')
# Read in party vote majority score data
con_maj_score = read_parties('DATA/mongo-db/con_maj_score.json')
lab_maj_score = read_parties('DATA/mongo-db/lab_maj_score.json')
ld_maj_score = read_parties('DATA/mongo-db/ld_maj_score.json')


class AnalyseVoteLoyalty(object):
    """Class to create dataframes to analyse vote loyalty. 
    """
    
    def __init__(self):
        """Variables needed for class stored here. 
        """
        self.con_mps = con_mps
        self.lab_mps = lab_mps
        self.ld_mps = ld_mps
        self.con_maj_score = con_maj_score
        self.lab_maj_score = lab_maj_score
        self.ld_maj_score = ld_maj_score
        self.con_score = con_score
        self.lab_score = lab_score
        self.ld_score = ld_score

    def get_vote_party_loyalty(self, votes_df):
        """Function to create loyalty scores for each party per each commons division vote. 
        """
        con_mp_scores_maj = mp_vote_scores(self.con_mps, self.con_maj_score)
        lab_mp_scores_maj = mp_vote_scores(self.lab_mps, self.lab_maj_score)
        ld_mp_scores_maj = mp_vote_scores(self.ld_mps, self.ld_maj_score)
        
        vote_lab_score = party_loyalty_vote(lab_mp_scores_maj, 'lab_score')
        vote_con_score = party_loyalty_vote(con_mp_scores_maj, 'con_score')
        vote_ld_score = party_loyalty_vote(ld_mp_scores_maj, 'ld_score')

        vote_frames = [vote_lab_score, vote_con_score, vote_ld_score]
        all_vote_score = reduce(lambda left,right: pd.merge(left,right,on=['index'],how='outer'), vote_frames)

        all_vote_score['total'] = (all_vote_score['lab_score'] + all_vote_score['con_score'] + all_vote_score['ld_score'])/3
        all_vote_score.rename(columns={'index':'uin'}, inplace=True)

        votes = votes_df.filter(['uin','date','title'], axis=1)
        all_vote_score_id = all_vote_score.merge(votes, on=['uin'], how='outer')
        all_vote_score_id.dropna(how='any')  
        return all_vote_score_id
    
    def get_mp_party_loyalty(self, mp_df):
        """Function to create overall loyalty scores for each member of parliament.
        """
        con_mp_scores = mp_vote_scores(self.con_mps, self.con_score)
        lab_mp_scores = mp_vote_scores(self.lab_mps, self.lab_score)
        ld_mp_scores = mp_vote_scores(self.ld_mps, self.ld_score)
        
        con_mp_scores_maj = mp_vote_scores(self.con_mps, self.con_maj_score)
        lab_mp_scores_maj = mp_vote_scores(self.lab_mps, self.lab_maj_score)
        ld_mp_scores_maj = mp_vote_scores(self.ld_mps, self.ld_maj_score)
        
        ld_total_scores = get_scores(ld_mp_scores, 'score')
        ld_total_maj_scores = get_scores(ld_mp_scores_maj, 'maj_score')

        lab_total_scores = get_scores(lab_mp_scores, 'score')
        lab_total_maj_scores = get_scores(lab_mp_scores_maj, 'maj_score')

        con_total_scores = get_scores(con_mp_scores,'score')
        con_total_maj_scores = get_scores(con_mp_scores_maj, 'maj_score')

        frames = [con_total_scores, lab_total_scores, ld_total_scores]
        frames2 = [con_total_maj_scores, lab_total_maj_scores, ld_total_maj_scores]

        all_scores = pd.concat(frames)
        all_scores_maj = pd.concat(frames2)

        all_scores.rename(columns={'index':'member_id'}, inplace=True)
        all_scores_maj.rename(columns={'index':'member_id'}, inplace=True)

        mp_id_scores = all_scores.merge(mp_df, on=['member_id'], how='outer')
        mp_id_scores = mp_id_scores.loc[mp_id_scores['party'].isin(['Conservative','Labour','Liberal Democrat'])]
        mp_id_scores = mp_id_scores.merge(all_scores_maj, on=['member_id'], how='outer')

        cols_to_norm(mp_id_scores, 'score')
        cols_to_norm(mp_id_scores, 'maj_score')
        return mp_id_scores
    
    def smallest_party_value_total(self, loyalty_df, number):
        """Function to display the votes with the lowest loyalty from the mps.
        """
        df = loyalty_df[(loyalty_df != 0).all(1)]
        df = df.nsmallest(number, 'total')
        return df

    def smallest_mp_value(self, loyalty_df, number):
        """Function to display the mps with the lowest loyalty.
        """
        loyalty_df.drop(loyalty_df[loyalty_df.maj_score == 0].index, inplace=True)
        df = loyalty_df.nsmallest(number, 'maj_score')
        return df
    
    def smallest_lab_value(self, loyalty_df, number):
        """Function to display the votes with the lowest loyalty score from the Labour mps.
        """
        loyalty_df.drop(loyalty_df[loyalty_df.lab_score == 0].index, inplace=True)
        df = loyalty_df.nsmallest(number, 'lab_score')
        return df
    
    def smallest_con_value(self, loyalty_df, number):
        """Function to display the votes with the lowest loyalty score from the Conservative mps.
        """
        loyalty_df.drop(loyalty_df[loyalty_df.con_score == 0].index, inplace=True)
        df = loyalty_df.nsmallest(number, 'con_score')
        return df
    
    def smallest_ld_value(self, loyalty_df, number):
        """Function to display the votes with the lowest loyalty score from the Liberal Democrat mps.
        """
        loyalty_df.drop(loyalty_df[loyalty_df.ld_score == 0].index, inplace=True)
        df = loyalty_df.nsmallest(number, 'ld_score')
        return df











