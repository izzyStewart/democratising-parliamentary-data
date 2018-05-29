<h1> democratising-parliamentary-data</h1>


This project aims to use data scraped from online sources to increase usability and analysis for a greater segment of society.
The public currently have access to 'TheyWorkForYou' website, listing the voting records for UK MPs, however this website has limited comparison and analysis capabilities. The program I have designed allows for cross party insight, especially focusing on the influences of the three-line whip.

The data in this project is taken from data.parliament, an open source website allowing developers to build API calls to retrieve raw data from Parliament. Most data is taken from the official parliamentary record, The Hansard. In order to retrieve this data in a accurate and efficient manner, I used multiprocessing techniques to request the data and format it for analysis.

The data I have scraped is taken from parliamentary sessions under two different Prime ministers’; Tony Blair during the period of 2001-2005 and Gordon Brown in the period of 2005-2007. I have primarily looked at the 2001-5 data using the 2005-7 data to compare results.

On embarking on this project, I wanted to look at what effects the loyalty of the MPs to their party, in particular whether they vote with, or against the three-line whip. An elected representative from each party will issue a three-line whip this is to maximise the number of MPs that vote and ensure that the vote is in line with the party agenda. To examine this, I have used MongodDB's aggregation pipelines and map-reduce functions to create loyalty scores. It is interesting to look at the effect on loyalty when the party is under an elected leader - Tony Blair as opposed to an unelected leader - Gorden Brown.

To test various hypothesis such as the effect of days service on a MPs voting and if the relationship between the party’s loyalty, I used linear regression from MLLIB in Spark. I also used PCA and cluster analysis to examine the relationship of MPs and party voting behaviour before and after an election period.

In this project I have endeavoured to better utilise the data found on data.parliament to allow for greater usability as a tool for democratic engagement. While loyalty to a party is not always the main concern when voters choose their MP, divisions within parties are becoming increasingly relevant. Though the data used here is just a snapshot of UK Parliament between 2001-2007, this approach could be used across a multitude of circumstances, from local government, to international organisations such as the UN. The tool has the possibility to be adapted according to the needs of the user.

<h2>Intructions</h2>

<b>Project details:</b>
Python version 3

<b>Python code</b>
- main.ipynb (notebook report)
- Spark-linear-regression.ipynb (spark code to be run after main)

<b>Python modules</b>
- creatempscores.py
- createpartyscores.py
- getmps.py
- getvotes.py
- groupdata.py
- kmeans.py
- mongodatabase.py

<b>Javascript modules</b>
- map-reduce-count.js
- map-reduce-pivot.js

Code has been run. Find outputs in notebook cells, including the datasets and visualiations. Find the data in /DATA

Data taken from data.parliament: http://www.data.parliament.uk/

<b>Packages used</b>
- io
- requests
- pandas
- csv
- mnis
- time
- datetime
- sys
- json
- functools
- pymongo
- numpy
- multiprocessing
- os
- matplotlib
- sklearn
- pylab
- seaborn

<b>Additional setup</b>
To run the program you need to have spark and mongodb installed locally.

