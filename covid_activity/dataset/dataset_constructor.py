import covid_activity
from covid_activity.utils.c3api import c3aidatalake
from tqdm import tqdm
import pandas as pd

### ------ Creating Raw Sector Activity Distribution per county -------  #### 
def get_rid_of_dashes(v):
  if '-' not in v:
    return v
  else:
    return v[:2] + '.0'

def get_rid_of_N_value(v):
  if v == 'N' or v == 'S':
    return 0
  else:
    return float(v)

def clean_values(industries, 
                 keys=['2017 NAICS Code', 'CBP Establishments',  'NES Establishments']):
    for key in keys:
      assert key in industries.columns
    industries['2017 NAICS Code'] = industries['2017 NAICS Code'].apply(get_rid_of_dashes)
    industries['NES Establishments'] = industries['NES Establishments'].apply(get_rid_of_N_value)
    industries['CBP Establishments'] = industries['CBP Establishments'].apply(get_rid_of_N_value)
    return industries

# change these helper functions to select the industries 

def find_sector_counts(industries, sector_range):
  sector = industries[(industries['2017 NAICS Code'].apply(lambda v: float(v)) < sector_range[1]) & (industries['2017 NAICS Code'].apply(lambda v: float(v)) >= sector_range[0])]
  sector['Total CBP and NES Establishments'] = sector['CBP Establishments'] + sector['NES Establishments']
  sector_counts = sector.groupby(by='2017 NAICS Description').agg(sum) #.drop('Total')
  return sector_counts
  
def activities_by_county(data, county_name=None, sector_ranges=[[100, 999], [7000, 7999], [8000, 8999]]):
  if county_name:
    data = data[county_name == data['County']]

  industries = data[['2017 NAICS Code', '2017 NAICS Description', 'CBP Establishments',  'NES Establishments']]
  industries = clean_values(industries)

  sector_counts = []
  for code_range in sector_ranges:
    sector_counts.append(find_sector_counts(industries, code_range))
  #return sector_counts
  return pd.concat(sector_counts, axis=0)
    
    
def create_sector_activities_by_county(path="../data/Data_data.csv"):
    data = pd.read_csv(path)
    columns = data.iloc[5]
    data = data.iloc[6:]
    data.columns = columns
    return activities_by_county(data)

##### covid case count dataset from C3ai DataLake API #######
from covid_activity.dataset.raw_dataset_downloader import (download_counties,
                                                           download_county_case_count)                                                         
def data_in_index(row, col_name='index'):
    return 'data' in row[col_name]

def create_county_case_counts():
    all_counties_cases = download_county_case_count(download_counties())
    indicies = all_counties_cases.transpose().reset_index().apply(data_in_index, axis=1)
    all_counties_cases = all_counties_cases.transpose()[indicies.values.reshape(-1)].transpose()  
