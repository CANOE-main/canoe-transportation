import pandas as pd
import os

cp_name = 'ON-2016TTS_no-we_2018_v4_2023-batteries'

dir_path = os.path.dirname(os.path.abspath(__file__)) + '/'
cp_file = dir_path + 'ramp_mobility/results/' + cp_name + '.csv'

cp = pd.read_csv(cp_file, index_col=0)
cp.index = pd.to_datetime(cp.index, utc=True)
cp = cp.set_index(cp.index.tz_convert('America/Toronto'))
cp = cp[cp.index.year == 2018]
cp = cp.resample('H').mean()
cp = cp.loc['2018-01-02':'2018-12-30']

cp.to_csv('ldv_charging (to clustering).csv')