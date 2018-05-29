# Importing modules.
import pandas as pd
from time import time
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from  matplotlib import pyplot
import matplotlib.patches as mpatches
import sklearn
from sklearn import metrics
from sklearn.cluster import KMeans
from sklearn.datasets import load_digits
from sklearn.decomposition import PCA
from sklearn.preprocessing import scale
import pylab as pl
import seaborn as sns

def set_graph_size(w,h):
    """Function to reset the size of the graph displayed in the notebook.
    """
    fig_size = plt.rcParams["figure.figsize"]
    # Sets the width
    fig_size[0] = w
    # Sets the height
    fig_size[1] = h
    plt.rcParams["figure.figsize"] = fig_size

class PlotPCA(object):
    """Class to create PCA values from mps voting behaviour. 
    """
    
    def __init__(self):
        """Initialising class.
        """
    
    def votes_to_pca(self, df_int, mp_df):
        """Function to transform all vote outcomes from each mp to PCA components. 
        """
        df_int.set_index('member_id', inplace=True)
        df_int.columns.names = ['votes']

        pca = PCA(n_components=2)
        pca.fit(df_int)

        tr = pca.transform(df_int)

        df_2d = pd.DataFrame(tr)
        df_2d.index = df_int.index
        df_2d.columns = ['PC1','PC2']

        df_2d = df_2d.reset_index()

        df_merge = df_2d.merge(mp_df, on=['member_id'], how='outer')
        return df_merge


    def cluster_parties(self, mps_pca):
        """Function to plot PCA values for each of the three main parties. 
        """
        set_graph_size(10,10)
        fig, ax = plt.subplots()
        colors = {'Labour':'#ff9999', 'Conservative':'#66b3ff', 'Liberal Democrat':'#ffcc99', 
                  'Sinn Féin':'#d8b99e', 'Independent':'#d8b99e', 
                  'Ulster Unionist Party':'#d8b99e', 'Democratic Unionist Party':'#d8b99e', 
                  'Scottish National Party':'#d8b99e', 'Independent Conservative':'#d8b99e',
                  'Respect':'#d8b99e', 'Social Democratic & Labour Party':'#d8b99e',
                  'Plaid Cymru':'#d8b99e', 'Speaker':'#d8b99e'
                 }

        ax.scatter(mps_pca['PC2'], mps_pca['PC1'], s=30, c=mps_pca['party'].apply(lambda x: colors[x]))

        plt.title('PCA - Party Voting 2001-2005', fontsize=19)
        plt.xlabel('PC2', fontsize=15)
        plt.ylabel('PC1', fontsize=15)

        red_patch = mpatches.Patch(color='#ff9999', label='Labour')
        blue_patch = mpatches.Patch(color='#66b3ff', label='Conservative')
        orange_patch = mpatches.Patch(color='#ffcc99', label='Liberal Democrat')
        brown_patch = mpatches.Patch(color='#d8b99e', label='Other')

        plt.legend(handles=[red_patch, blue_patch, orange_patch, brown_patch], fontsize=15)
        graph = plt.show()

        return graph

    def cluster_mps(self, mps_pca):
        """Function plot PCA values for Labour MPs. 
        """
        df_lab = mps_pca.loc[mps_pca['party'] == 'Labour']

        set_graph_size(17,17)

        fig, ax = plt.subplots()

        party = df_lab['party']

        ax.scatter(df_lab['PC2'], df_lab['PC1'], s=10, c='#ff9999')

        for i, mp in enumerate(df_lab.list_name):
            ax.annotate(mp, (df_lab.iloc[i].PC2, df_lab.iloc[i].PC1), ha='right', va='bottom',
                        bbox=dict(boxstyle='round,pad=0.5', fc='#ff9999', alpha=0.5))

        plt.title('PCA - MP Voting 2001-2005', fontsize=26)
        plt.xlabel('PC2', fontsize=20)
        plt.ylabel('PC1', fontsize=20)
        graph = plt.show()
        return graph

class PlotPCA_05_07(object):
    """Class to repeat the analyses of the class above using the 2005-2007 mp data.
    """
    
    def __init__(self):
        """Initilising function.
        """
    
    def votes_to_pca(self, df_int, mp_df):
        df_int.set_index('member_id', inplace=True)
        df_int.columns.names = ['votes']

        pca = PCA(n_components=2)
        pca.fit(df_int)

        tr = pca.transform(df_int)

        df_2d = pd.DataFrame(tr)
        df_2d.index = df_int.index
        df_2d.columns = ['PC1','PC2']

        df_2d = df_2d.reset_index()

        df_merge = df_2d.merge(mp_df, on=['member_id'], how='outer')
        return df_merge


    def cluster_parties(self, mps_pca):
        set_graph_size(10,10)
        fig, ax = plt.subplots()
        colors = {'Labour':'#ff9999', 'Conservative':'#66b3ff', 'Liberal Democrat':'#ffcc99', 
                  'Sinn Féin':'#d8b99e', 'Independent':'#d8b99e', 
                  'Ulster Unionist Party':'#d8b99e', 'Democratic Unionist Party':'#d8b99e', 
                  'Scottish National Party':'#d8b99e', 'Independent Conservative':'#d8b99e',
                  'Respect':'#d8b99e', 'Social Democratic & Labour Party':'#d8b99e',
                  'Plaid Cymru':'#d8b99e', 'Speaker':'#d8b99e','Independent Labour':'#d8b99e'
                 }

        ax.scatter(mps_pca['PC2'], mps_pca['PC1'], s=30, c=mps_pca['party'].apply(lambda x: colors[x]))

        plt.title('PCA - Party Voting 2005-2007', fontsize=19)
        plt.xlabel('PC2', fontsize=15)
        plt.ylabel('PC1', fontsize=15)

        red_patch = mpatches.Patch(color='#ff9999', label='Labour')
        blue_patch = mpatches.Patch(color='#66b3ff', label='Conservative')
        orange_patch = mpatches.Patch(color='#ffcc99', label='Liberal Democrat')
        brown_patch = mpatches.Patch(color='#d8b99e', label='Other')

        plt.legend(handles=[red_patch, blue_patch, orange_patch, brown_patch], fontsize=15)
        graph = plt.show()

        return graph

    def cluster_mps(self, mps_pca):
        df_lab = mps_pca.loc[mps_pca['party'] == 'Labour']

        set_graph_size(17,17)

        fig, ax = plt.subplots()

        party = df_lab['party']

        ax.scatter(df_lab['PC2'], df_lab['PC1'], s=10, c='#ff9999')

        for i, mp in enumerate(df_lab.list_name):
            ax.annotate(mp, (df_lab.iloc[i].PC2, df_lab.iloc[i].PC1), ha='right', va='bottom',
                        bbox=dict(boxstyle='round,pad=0.5', fc='#ff9999', alpha=0.5))

        plt.title('PCA - MP Voting 2005-2007', fontsize=26)
        plt.xlabel('PC2', fontsize=20)
        plt.ylabel('PC1', fontsize=20)
        graph = plt.show()
        return graph