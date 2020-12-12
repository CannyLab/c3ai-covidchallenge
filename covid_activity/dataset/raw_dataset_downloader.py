from covid_activity.utils.c3api import c3aidatalake
import csv
import pandas as pd
import subprocess
from tqdm import tqdm
import xlrd



##### Census Buerau activity data #############
def _run_bash(bash_command):
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE) 
    output, error = process.communicate()

def download_industry_sector_statistics(root_dir=""):
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

#######################################################

### c3api ######################################
def download_counties():
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

def download_county_case_count(counties: pd.DataFrame, source_dataset="JHU_ConfirmedCases"):
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
