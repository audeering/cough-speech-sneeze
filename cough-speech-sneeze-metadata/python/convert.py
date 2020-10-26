import os

import pandas as pd

import audeer
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


CATEGORY ={
    'coughing': 'coughing',
    'silence': 'silence',
    'sneezing': 'sneezing',
    'speech': 'speech'
}


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
        source='', #? NAS?
        usage=define.Usage.COMMERCIAL, #?
        description=description,
    )

    #########
    # Media #
    #########
    # used `soxi <file name> to sample some (~10) audio files from all categories and checked their metadata
    # All
    db.media['microphone'] = AudioInfo(
        sampling_rate=44100, # all categories got 44100 kHz, but 'silent' only got 16000 kHz
        channels=1,
        format='wav',
    )

    ###########
    # Schemes #
    ###########
    db.schemes['category'] = Scheme(labels=list(CATEGORY.values()))
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
    category_id = audeer.basename_wo_ext(filename).split('-')[2]
    return CATEGORY[category_id]


def get_intensity(filename):
    intensity_id = audeer.basename_wo_ext(filename).split('-')[3]
    return EMOTIONAL_INTENSITY[intensity_id]


def get_speaker(filename):
    return audeer.basename_wo_ext(filename).split('-')[6]


def get_statement(filename):
    statement_id = audeer.basename_wo_ext(filename).split('-')[4]
    return STATEMENT[statement_id]


def get_channel(filename):
    channel_id = audeer.basename_wo_ext(filename).split('-')[1]
    return VOCAL_CHANNEL[channel_id]
