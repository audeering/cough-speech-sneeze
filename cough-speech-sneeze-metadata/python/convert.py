import os

import pandas as pd

import audiofile as af

import audata.define as define
import audata.utils as utils
from audata import (
    AudioInfo,
    Column,
    Database,
    Scheme,
    Table
)


CATEGORY = [
    'coughing',
    'silence',
    'sneezing',
    'speech'
]


def convert(
        description: str,
        data_root: str,
        annotation_root: str = None
) -> Database:
    r"""Conversion function called by Gradle plugin."""

    ##########
    # Header #
    ##########
    db = Database(
        name='cough-speech-sneeze',
        source='Dataset based on the publication of Shahin Amiriparian: "Amiriparian, S., Pugachevskiy, S., Cummins, N., Hantke, S., Pohjalainen, J., Keren, G., Schuller, B., 2017. CAST a database: Rapid targeted large-scale big data acquisition via small-world modelling of social media platforms, in: 2017 Seventh International Conference on Affective Computing and Intelligent Interaction (ACII). IEEE, pp. 340–345. https://doi.org/10.1109/ACII.2017.8273622"',
        usage=define.Usage.COMMERCIAL, # CC-4
        description=description,
    )

    #########
    # Media #
    #########
    # used `soxi <file name> to sample some (~10) audio files from all categories and checked their metadata
    db.media['microphone'] = AudioInfo(
        sampling_rate=None, # all categories got 44100 kHz, but 'silent' only got 16000 kHz
        channels=1,
        format='wav',
    )

    ###########
    # Schemes #
    ###########
    db.schemes['category'] = Scheme(labels=CATEGORY)
    db.schemes['duration'] = Scheme(dtype=define.DataType.TIME)

    ##########
    # Tables #
    ##########
    files = list(utils.scan_files(
        root=data_root,
        recursive=True,
        pattern='*.wav',
    ))
    category_files = [get_category(f) for f in files]
    durations = utils.run_worker_threads(
        num_workers=12,
        task_fun=lambda x: pd.to_timedelta(af.duration(
            os.path.join(data_root, x)), unit='s'),
        params=files,
        progress_bar=True,
        task_description='Parse duration',
    )
    # files
    db['files'] = Table(files=files)
    db['files']['category'] = Column(scheme_id='category')
    db['files']['category'].set(category_files, files=files)
    db['files']['duration'] = Column(scheme_id='duration')
    db['files']['duration'].set(durations, files=files)

    return db


def get_category(filename):
    # The category-folder is always the 2nd last part of the string (e.g. 'coughing/ipo39x2bv9c_12.7253-13.7358.wav')
    return filename.split('/')[-2]
