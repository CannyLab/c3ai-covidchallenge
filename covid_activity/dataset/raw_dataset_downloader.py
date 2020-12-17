from covid_activity.utils.c3api import c3aidatalake
import csv
import os
import pandas as pd
import subprocess
from tqdm import tqdm
import xlrd

from covid_activity.references import DATASET_DIR


##### Census Buerau activity data #############
def _run_bash(bash_command):
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE) 
    output, error = process.communicate()

def download_industry_sector_statistics(root_dir=DATASET_DIR):
    os.chdir(root_dir)
    _run_bash("wget https://www2.census.gov/programs-surveys/nonemployer-statistics/tables/2018/combine18_xslx.zip")
    _run_bash("unzip combine18_xslx.zip")
    # extract excel file
    wb = xlrd.open_workbook('combine18.xlsx', True)
    print(wb.sheet_names())
    for name in wb.sheet_names():
        with open(f'{root_dir}/{name}_data.csv', 'w') as csv_file: 
            sh = wb.sheet_by_name(name)
            wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
            for rownum in range(sh.nrows):
                wr.writerow(sh.row_values(rownum))
    #_run_bash(f"mv combine18_xslx.zip Data_data.csv Definitions_data.csv Notes_data.csv {DATASET_DIR}")

#######################################################

### c3api ######################################
def download_counties():
    '''download_counties 
       NOTE: the returned populationCDS from the c3aidatalake are incorrect.
    '''
    counties = c3aidatalake.fetch(
    "outbreaklocation",
    {
        "spec" : {
            "filter" : "contains(id, 'UnitedStates') && locationType == 'county'"
        }
    },
    get_all = True
    )
    return counties

def download_county_case_count(counties_path: str = os.path.join(DATASET_DIR, 'counties.csv'), source_dataset="JHU_ConfirmedCases"):
    counties = pd.read_csv(counties_path)
    today = pd.Timestamp.now().strftime("%Y-%m-%d")
    all_counties_time = []
    ids = list(counties['id'].values)
    for i in tqdm(range(counties.shape[0] // 10), total=counties.shape[0] // 10):
        counties_time = c3aidatalake.evalmetrics(
            "outbreaklocation",
            {
                "spec" : {
                    "ids": ids[i * 10: i * 10 + 10],
                    "expressions" : [source_dataset],
                    "start" : "2020-01-01",
                    "end" : today,
                    "interval" : "DAY",
                }
            }
        )
        all_counties_time +=[counties_time]
    all_counties_cases = pd.concat(all_counties_time, axis=1)
    return all_counties_cases

def download_deaths(counties_path: str = os.path.join(DATASET_DIR, 'counties_deaths.csv'), source_dataset="JHU_ConfirmedDeaths"):
    counties = pd.read_csv(counties_path)
    today = pd.Timestamp.now().strftime("%Y-%m-%d")
    all_counties_time = []
    ids = list(counties['id'].values)
    for i in tqdm(range(counties.shape[0] // 10), total=counties.shape[0] // 10):
        counties_time = c3aidatalake.evalmetrics(
            "outbreaklocation",
            {
                "spec" : {
                    "ids": ids[i * 10: i * 10 + 10],
                    "expressions" : [source_dataset],
                    "start" : "2020-01-01",
                    "end" : today,
                    "interval" : "DAY",
                }
            }
        )
        all_counties_time +=[counties_time]
    all_counties_cases = pd.concat(all_counties_time, axis=1)
    return all_counties_cases

def download_county_policy_data(root_dir=DATASET_DIR):
    '''download_policy_data 
    downloads policy data from healthdata
    https://healthdata.gov/sites/default/files/state_policy_updates_20201212_1920.csv
    '''
    _run_bash(f"cd {root_dir} && wget https://healthdata.gov/sites/default/files/state_policy_updates_20201212_1920.csv")
    return pd.read_csv(os.path.join(root_dir,'state_policy_updates_20201212_1920.csv' ))

### helpers ###
def saver(save_path, func):
    '''saver: saves output of a function to dataframe
    Args:
        save_path (string): a string where the dataframe will be saved
        func (callable): A function that returns a pd.DataFrame
    returns function
    '''
    def f(*args, **kwargs):
        if not os.path.exists(save_path):
            df = func(*args, **kwargs)
            print(f'Saving df to {save_path}')
            df.to_csv(save_path)
        else:
            df = pd.read_csv(save_path)
        return df
    return f

download_counties = saver('../data/counties.csv', download_counties)
download_county_case_count = saver('../data/county_case_counts.csv', download_county_case_count )
