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


data_root = 'build/unpacked/'


CATEGORY ={
    'coughing': 'coughing',
    'silence': 'silence',
    'sneezing': 'sneezing',
    'speech': 'speech'
}


files = list(utils.scan_files(
        root=data_root,
        recursive=True,
        pattern='*.wav',
    ))


def get_category(filename):
    category_id = audeer.basename_wo_ext(filename).split('-')[2]
    return CATEGORY[category_id]


category_files = [get_category(f) for f in files]
