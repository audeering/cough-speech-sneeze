import pandas as pd
import numpy as np
import os
import shutil 

#loading csv file containing reevaluated labels for cough and sneeze
path_csv = '/home/audeering.local/jthybo/Documents/sneeze_cough_data/20210412-102437-cough-sneeze/'
df = pd.read_csv(path_csv +'20210412-102437_cough-and-sneeze_annotations-cough_sneeze.csv')
df = df[['Media File','Answer 1','Bad File?']]


#Removing files with bad sound quality or other sounds than cough and sneeze
df = df[df['Bad File?'] == False ]  
df_cough = df[df['Answer 1'] == 'coughing']
df_sneeze = df[df['Answer 1'] == 'sneezing']

#Creating new column that matches the file name in the original csv file
df_cough['file'] = 'coughing/' + df_cough['Media File'].astype(str)
df_sneeze['file'] = 'sneezing/' + df_sneeze['Media File'].astype(str)

#loading the original csv file for the data base

path_old_csv = '/home/audeering.local/jthybo/audb/cough-speech-sneeze/1.0.0/d3b62a9b/'
df_old = pd.read_csv(path_old_csv +'db.files.csv')

#seperating csv file into sub catagories
df_old_cough = df_old[df_old['category'] == 'coughing']
df_old_sneeze = df_old[df_old['category'] == 'sneezing']
df_old_silence = df_old[df_old['category'] == 'silence']
df_old_speech = df_old[df_old['category'] == 'speech']


v=0
#Removing cough and sneeze files with were not accepted in 2nd scoring round
for x in df_old_cough['file'].copy():
    if df_cough['file'].str.contains(x).any():
     v=v+1
    else:
        df_old_cough = df_old_cough[~df_old_cough['file'].str.contains(x)]

for x in df_old_sneeze['file'].copy():
    if df_sneeze['file'].str.contains(x).any():
     v=v+1
    else:
        df_old_sneeze = df_old_sneeze[~df_old_sneeze['file'].str.contains(x)]


#Create foldes for version 2.0.0
dirName = 'audb/cough-speech-sneeze/2.0.0/d3b62a9b/'
eventName = ['coughing','silence','sneezing','speech']

for name in eventName:
    os.makedirs(dirName+name)

#Moving accepted files from old database to the new
for file in df_old_cough['file']:
    source = path_old_csv + 'coughing/' + file[9:]
    destination = dirName + 'coughing/'
    shutil.move(source, destination)

for file in df_old_sneeze['file']:
    source = path_old_csv + 'sneezing/' + file[9:]
    destination = dirName + 'sneezing/'
    shutil.move(source, destination)

for file in df_old_silence['file']:
    source = path_old_csv + 'silence/' + file[8:]
    destination = dirName + 'silence/'
    shutil.move(source, destination)

for file in df_old_speech['file']:
    source = path_old_csv + 'speech/' + file[6:]
    destination = dirName + 'speech/'
    shutil.move(source, destination)

#Saving update csv file
final_csv_file = pd.concat([df_old_cough,df_old_speech,df_old_silence,df_old_sneeze], ignore_index=True)
final_csv_file.to_csv('audb/cough-speech-sneeze/2.0.0/d3b62a9b/db.files.csv')

