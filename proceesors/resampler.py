'''
Tools used to resample data from tick level to lower resolution data.
'''

import numpy as np
import pandas as pd

class FXTickDataResampler():
    '''
    Basic toolbox for resampling tick data.
    '''

    def __init__(self, data: pd.DataFrame):
        self.data = data
