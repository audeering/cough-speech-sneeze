import pandas as pd
import audeer
import audb
import shutil

name = 'cough-speech-sneeze'
previous_version = '1.0.0'
version = '2.0.0'
build_dir = '../build'
repository = audb.Repository(
    name='data-public-local',
    host='https://artifactory.audeering.com/artifactory',
    backend='artifactory',
)

build_dir = audeer.mkdir(build_dir)

db = audb.load_to(
    build_dir,
    name,
    version=previous_version,
    num_workers=8,
)

#loading csv file containing reevaluated labels for cough and sneeze
path_csv_reevaluation = '../20210412-102437-cough-sneeze/20210412-102437_cough-and-sneeze_annotations-cough_sneeze.csv'
df = pd.read_csv(path_csv_reevaluation)
df = df[['Media File','Answer 1','Bad File?']]

#Removing files with bad sound quality or other sounds than cough and sneeze
df = df[df['Bad File?'] == False ]  
df_cough = df[df['Answer 1'] == 'coughing']
df_sneeze = df[df['Answer 1'] == 'sneezing']

#Creating new column that matches the file name in the original csv file
df_cough['file'] = 'coughing/' + df_cough['Media File'].astype(str)
df_sneeze['file'] = 'sneezing/' + df_sneeze['Media File'].astype(str)

#loading the original csv file for the data base
df_old = pd.read_csv(build_dir +'/db.files.csv')

#seperating csv file into sub catagories
df_old_cough = df_old[df_old['category'] == 'coughing']
df_old_sneeze = df_old[df_old['category'] == 'sneezing']


#Removing cough and sneeze files with were not accepted in 2nd scoring round
for x in df_old_cough['file'].copy():
    if not df_cough['file'].str.contains(x).any():
        db.drop_files(x)
        
for x in df_old_sneeze['file'].copy():
    if not df_sneeze['file'].str.contains(x).any():
        db.drop_files(x)


#Create folders for publishing
publish_dir = '../publish/'
eventName = ['coughing','silence','sneezing','speech']

for name in eventName:
    audeer.mkdir(publish_dir+name)


#Move relevant files to new folder
for wav_file in db.files:
    source = build_dir + '/' + wav_file
    destination = publish_dir + wav_file
    shutil.move(source, destination)


# Update header metadata
db.license = 'Unknown'

db.description = (
    'Cough-speech-sneeze: a data set of human sounds This dataset was collected by' 
    'Dr. Shahin Amiriparian. It contains samples of human speech, coughing, and' 
    'sneezing collected from YouTube, as well as silence clips. The original' 
    'publication of this (possibly then extended) dataset is the following:' 
    'Amiriparian, S., Pugachevskiy, S., Cummins, N., Hantke, S., Pohjalainen, J.,' 
    'Keren, G., Schuller, B., 2017. CAST a database: Rapid targeted large-scale big' 
    'data acquisition via small-world modelling of social media platforms, in: 2017' 
    'Seventh International Conference on Affective Computing and Intelligent' 
    'Interaction (ACII). IEEE, pp. 340–345. https://doi.org/10.1109/ACII.2017.8273622'
)
db.save(publish_dir)

#Update db csv file
db_files_updated = pd.read_csv(publish_dir +'db.files.csv')
db_csv = pd.read_csv(build_dir +'/db.csv')
db_csv = db_csv.rename(columns={"Unnamed: 0": "file"})

for x in db_csv['file'].copy():
    if not db_files_updated['file'].str.contains(x).any():
        db_csv= db_csv[~db_csv['file'].str.contains(x)]

db_csv.to_csv(publish_dir + 'db.csv')

audb.publish(
    publish_dir,
    version,
    repository,
    previous_version=previous_version,
)

