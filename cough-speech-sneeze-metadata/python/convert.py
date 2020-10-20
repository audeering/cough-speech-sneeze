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
    Rater,
    Scheme,
    Table,
)


# Filename identifier mappings, see https://doi.org/10.5281/zenodo.1188975
MODALITY = {
    '01': 'audio-video',
    '02': 'video',
    '03': 'audio',
}
VOCAL_CHANNEL = {
    '01': 'speech',
    '02': 'song',
}
EMOTION = {
    '01': 'neutral',
    '02': 'calm',
    '03': 'happy',
    '04': 'sad',
    '05': 'angry',
    '06': 'fearful',
    '07': 'disgust',
    '08': 'suprised',
}
EMOTIONAL_INTENSITY = {
    '01': 'normal',
    '02': 'strong',
}
STATEMENT = {
    '01': 'Kids are talking by the door',
    '02': 'Dogs are sitting by the door',
}
REPETITION = {
    '01': '1st repetition',
    '02': '2nd repetition',
}
lang = utils.str_to_language('en').name
ACTORS = {
    '01': {'gender': define.Gender.MALE, 'language': lang},
    '02': {'gender': define.Gender.FEMALE, 'language': lang},
    '03': {'gender': define.Gender.MALE, 'language': lang},
    '04': {'gender': define.Gender.FEMALE, 'language': lang},
    '05': {'gender': define.Gender.MALE, 'language': lang},
    '06': {'gender': define.Gender.FEMALE, 'language': lang},
    '07': {'gender': define.Gender.MALE, 'language': lang},
    '08': {'gender': define.Gender.FEMALE, 'language': lang},
    '09': {'gender': define.Gender.MALE, 'language': lang},
    '10': {'gender': define.Gender.FEMALE, 'language': lang},
    '11': {'gender': define.Gender.MALE, 'language': lang},
    '12': {'gender': define.Gender.FEMALE, 'language': lang},
    '13': {'gender': define.Gender.MALE, 'language': lang},
    '14': {'gender': define.Gender.FEMALE, 'language': lang},
    '15': {'gender': define.Gender.MALE, 'language': lang},
    '16': {'gender': define.Gender.FEMALE, 'language': lang},
    '17': {'gender': define.Gender.MALE, 'language': lang},
    '18': {'gender': define.Gender.FEMALE, 'language': lang},
    '19': {'gender': define.Gender.MALE, 'language': lang},
    '20': {'gender': define.Gender.FEMALE, 'language': lang},
    '21': {'gender': define.Gender.MALE, 'language': lang},
    '22': {'gender': define.Gender.FEMALE, 'language': lang},
    '23': {'gender': define.Gender.MALE, 'language': lang},
    '24': {'gender': define.Gender.FEMALE, 'language': lang},
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
        name='ravdess',
        source='https://doi.org/10.5281/zenodo.1188975',
        usage=define.Usage.RESEARCH,
        languages=[utils.str_to_language('en')],
        description=description,
    )

    #########
    # Media #
    #########
    db.media['microphone'] = AudioInfo(
        sampling_rate=48000,
        channels=1,
        format='wav',
    )

    ##########
    # Raters #
    ##########
    db.raters['gold'] = Rater()

    ###########
    # Schemes #
    ###########
    db.schemes['emotion'] = Scheme(labels=list(EMOTION.values()))
    db.schemes['emotional intensity'] = Scheme(
        labels=list(EMOTIONAL_INTENSITY.values())
    )
    db.schemes['vocal channel'] = Scheme(labels=list(VOCAL_CHANNEL.values()))
    db.schemes['speaker'] = Scheme(labels=ACTORS)
    db.schemes['transcription'] = Scheme(
        labels=list(STATEMENT.values()),
        description='Sentence produced by actor.',
    )
    db.schemes['duration'] = Scheme(dtype=define.DataType.TIME)

    ##########
    # Tables #
    ##########
    files = list(utils.scan_files(
        data_root,
        recursive=True,
        pattern='*.wav',
    ))
    speaker = [get_speaker(f) for f in files]
    emotion = [get_emotion(f) for f in files]
    intensity = [get_intensity(f) for f in files]
    statement = [get_statement(f) for f in files]
    channel = [get_channel(f) for f in files]
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
    db['files']['speaker'] = Column(scheme_id='speaker')
    db['files']['speaker'].set(speaker, files=files)
    db['files']['duration'] = Column(scheme_id='duration')
    db['files']['duration'].set(durations, files=files)
    db['files']['transcription'] = Column(scheme_id='transcription')
    db['files']['transcription'].set(statement, files=files)
    db['files']['vocal channel'] = Column(scheme_id='vocal channel')
    db['files']['vocal channel'].set(channel)
    # emotion
    db['emotion'] = Table(files=files)
    db['emotion']['emotion'] = Column(scheme_id='emotion', rater_id='gold')
    db['emotion']['emotion'].set(emotion)
    db['emotion']['emotional intensity'] = Column(
        scheme_id='emotional intensity',
        rater_id='gold',
    )
    db['emotion']['emotional intensity'].set(intensity)

    return db


def get_emotion(filename):
    emotion_id = audeer.basename_wo_ext(filename).split('-')[2]
    return EMOTION[emotion_id]


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
