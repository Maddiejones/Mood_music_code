import pandas as pd
import os 
import numpy as np
from tqdm import tqdm 
from filepaths import FULL_FILEPATH_IN, FULL_FILEPATH_OUT

def main(ema_folder, out_folder):
    files = os.listdir(ema_folder)
    daily_files = [s for s in files if "Daily" in s]

    for file in tqdm(daily_files):
        process_ema_file(file, ema_folder, out_folder)

def tidy_ema(data):
    empty_row = pd.DataFrame([[np.nan] * len(data.columns)], columns=data.columns)
    extra_row = pd.concat([empty_row, data],ignore_index=True)
    duplicate_rows = pd.concat([data, extra_row], axis=1)
    full_df = duplicate_rows.iloc[1::3, :]
    full_df = full_df.set_axis(['Null1', 'Null2', 
                                'time_entry', 'time_cheerful', 'time_worried', 'time_relaxed', 'time_sad', 'time_frustrated', 
                                'start_time', 'end_time', 'q_entry', 'q_cheerful','q_worried','q_relaxed','q_sad','q_frustrated'], 
                                 axis=1)
    full_df.drop(columns = ['Null1', 'Null2'], inplace=True)
    return(full_df)

def full_day(day:pd.DataFrame, evening:pd.DataFrame) -> pd.DataFrame:
    """"take data from day and evening and joins them into a single dataframe"""
    full_day = pd.concat([day, evening], axis=0)
    #reorders the columns so the question scores are at the beginning
    cols= ['start_time','end_time',
       'q_entry', 'q_cheerful','q_worried','q_relaxed','q_sad','q_frustrated',
       'time_entry', 'time_cheerful', 'time_worried', 'time_relaxed', 'time_sad', 'time_frustrated']
    full_day = full_day[cols]
    return(full_day)


def process_ema_file(filename, ema_folder, out_folder):
    raw_daily = pd.read_csv(os.path.join(ema_folder,filename),skiprows=4)
    raw_evening = pd.read_csv(os.path.join(ema_folder, filename.replace("Daily", "Evening")), skiprows=4)
    tidy_daily = tidy_ema(raw_daily)
    tidy_evening = tidy_ema(raw_evening)
    tidy_day = full_day(tidy_daily, tidy_evening)

    #inserts column from filename containing the unique participant ID 
    tidy_day.insert(0, 'personID', filename[0:4])
    #save out single tidy file for participant 
    out_filename = filename[0:4]+'_ema.tsv'
    tidy_day.to_csv(os.path.join(out_folder, out_filename), sep='\t', index=False)

if __name__ == "__main__":
    main(FULL_FILEPATH_IN, FULL_FILEPATH_OUT)
    

