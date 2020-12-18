import covid_activity
from covid_activity.utils.c3api import c3aidatalake
import numpy as np
import os
import pandas as pd
import re
from tqdm import tqdm
tqdm.pandas()
import warnings
# for the time being assumed that all the data has been downloaded and exists in data
from covid_activity.dataset.raw_dataset_downloader import (download_counties,
                                                           download_county_case_count)   
import datetime
from covid_activity.references import DATASET_DIR

### ------ Creating Raw Sector Activity Distribution per county -------  #### 
class CountyActivity:
  def __init__(self, path=  'Data_data.csv'):
      self.path = os.path.join(DATASET_DIR, path)
      self.county_activites = None
      self._process_data()
  
  def _process_data(self):
    self.data = pd.read_csv(self.path)
    columns = self.data.iloc[5]
    self.data = self.data.iloc[6:]
    self.data.columns = columns
    self.data['county'] = np.asarray([string.replace(' ', '') for string in ((self.data['County'] + '_' + self.data['State'] + '_UnitedStates').values)])
    return self.data
  
  def get_activities(self, county_name=None, sector_ranges=[[100, 999], [7000, 7999], [8000, 8999]]):
    self.activities = activities_by_county(self.data, county_name, sector_ranges)
    return self.activities
  
  def _get_county_fips(self, use_cached=True):
    if use_cached and 'fips' in self.data:
      return self.data[['County FIPS','State FIPS', 'State', 'county', 'fips']].drop_duplicates().reset_index(drop=True)
    fips_to_state = self.data[['County FIPS','State FIPS', 'State', 'county']]
    leading_zero = leading_zero_creator(n=2)
    fips_to_state['fips'] = (fips_to_state['State FIPS'].apply(leading_zero))
    leading_zero = leading_zero_creator(n=3)
    fips_to_state['fips'] = fips_to_state['fips']  + fips_to_state['County FIPS'].apply(leading_zero)
    self.data['fips'] = fips_to_state['fips']
    return self.data[['County FIPS','State FIPS', 'State', 'county', 'fips']].drop_duplicates().reset_index(drop=True)

def remove_period(string):
    try:
        string = str(int(string))
        return string.replace('.', '')
    except:
        return ""
def leading_zero_creator(n):
  def func(v):
    try:
        string = remove_period(v)
        if len(string) < n:
            return '0' * (n-len(string)) + string
        return string
    except:
        return ""
  return func
    

def _get_rid_of_dashes(v):
  if '-' not in v:
    return v
  else:
    return v[:2] + '.0'

def _get_rid_of_N_value(v):
  if v == 'N' or v == 'S':
    return 0
  else:
    return float(v)

def _clean_values(industries, 
                 keys=['2017 NAICS Code', 'CBP Establishments',  'NES Establishments']):
    for key in keys:
      assert key in industries.columns
    industries['2017 NAICS Code'] = industries['2017 NAICS Code'].apply(_get_rid_of_dashes)
    industries['NES Establishments'] = industries['NES Establishments'].apply(_get_rid_of_N_value)
    industries['CBP Establishments'] = industries['CBP Establishments'].apply(_get_rid_of_N_value)
    return industries

def _find_sector_counts(industries, sector_range):
  with warnings.catch_warnings(record=True):  
    sector = industries[(industries['2017 NAICS Code'].apply(lambda v: float(v)) < sector_range[1]) & (industries['2017 NAICS Code'].apply(lambda v: float(v)) >= sector_range[0])]
    sector['Total CBP and NES Establishments'] = sector.loc[:, 'CBP Establishments'] + sector.loc[:, 'NES Establishments']
    sector_counts = sector.groupby(by='2017 NAICS Description').agg(sum) #.drop('Total')
    return sector_counts
  
def activities_by_county(data, county_name=None, sector_ranges=[[100, 999], [7000, 7999], [8000, 8999]]):
  if county_name: 
    data = data[county_name == data['county']]

  industries = data[['2017 NAICS Code', '2017 NAICS Description', 'CBP Establishments',  'NES Establishments']]
  industries = _clean_values(industries)

  sector_counts = []
  for code_range in sector_ranges:
    sector_counts.append(_find_sector_counts(industries, code_range))
  #return sector_counts
  return pd.concat(sector_counts, axis=0)
    
  
###############################################################

##### covid case count dataset from C3ai DataLake API #######

class C3aiDataLake:
  def __init__(self,
               county_path = 'counties.csv',
               case_count_path = 'county_case_counts.csv',
               merged_path = 'county_case_counts_pop.csv',
               death_count_path = 'death_counts.csv',
               ):
    self.county_path = os.path.join(DATASET_DIR, county_path)
    self.case_count_path = os.path.join(DATASET_DIR, case_count_path)
    self.death_count_path = os.path.join(DATASET_DIR, death_count_path)
    self.merged_path = os.path.join(DATASET_DIR, merged_path)
    
    assert os.path.exists(self.county_path), f'{self.county_path} Does not exist. Download data first.'
    assert os.path.exists(self.case_count_path), f'{self.case_count_path} Does not exist. Download data first.'
    
    self.counties = None
    self.case_counts = None
    self.death_counts = None
    self.population = None
    self.merged = None
    
    self._process_data()

    
  def _process_data(self):
    self.get_counties()
    self.get_county_case_counts()
    self.get_county_population()
    self.get_county_case_counts_pop()
    self.get_county_death_counts()

  def _county_names(self):
    return self.merged['county'].values
    
  def get_counties(self, use_cached=True):
    if use_cached and self.counties is not None:
      return self.counties
    self.counties = pd.read_csv(self.county_path)
    self.counties['county'] = self.counties['id']
    return self.counties
  
  def get_county_case_counts(self, use_cached=True):
    if use_cached and self.case_counts is not None:
      return self.case_counts
    self.case_counts = pd.read_csv(self.case_count_path)
    indicies =self.case_counts.transpose().reset_index().apply(data_in_index, axis=1)
    self.case_counts = self.case_counts.transpose()[indicies.values.reshape(-1)].reset_index()
    self.case_counts['county'] = self.case_counts['index'].apply(lambda string: re.sub('\.JHU_ConfirmedCases\.data', "", string))
    self.case_counts = self.case_counts.drop(['index'], axis=1)
    self.case_counts = self.case_counts.loc[:, ['county', *self.case_counts.columns[:-1]]]
    return self.case_counts
  
  def get_county_death_counts(self, use_cached=True):
    if use_cached and self.death_counts is not None:
      return self.death_counts
    self.death_counts = pd.read_csv(self.death_count_path)
    indicies =self.death_counts.transpose().reset_index().apply(data_in_index, axis=1)
    self.death_counts = self.death_counts.transpose()[indicies.values.reshape(-1)].reset_index()
    self.death_counts['county'] = self.death_counts['index'].apply(lambda string: re.sub('\.JHU_ConfirmedDeaths\.data', "", string))
    self.death_counts = self.death_counts.drop(['index'], axis=1)
    self.death_counts = self.death_counts.loc[:, ['county', *self.death_counts.columns[:-1]]]
    return self.death_counts
  
  def get_county_population(self, use_cached=True):
    if use_cached and self.population is not None:
      return self.population
    self.population = self.counties[['id', 'populationCDS']]
    self.population.columns=['county', 'population']
    return self.population
  
  def get_county_case_counts_pop(self, use_cached=True):
    if use_cached and self.merged is not None:
      return self.merged
    
    if use_cached and os.path.exists(self.merged_path):
      #print(self.merged_path)
      self.merged = pd.read_csv(self.merged_path)
      return self.merged
    print('merging counties, population, and case counts')
    self.merged = pd.DataFrame(list(zip(
                    self.counties['county'].values,
                    self.population['population'].values, 
                    self.case_counts.drop('county', axis=1).values,
                    )))
    self.merged = self.merged.explode(2) # can only explode a single column not multiple
    self.merged[3] = pd.DataFrame(list(zip(
                      self.counties['county'].values,
                      self.population['population'].values, 
                      self.death_counts.drop('county', axis=1).values,
                    ))).explode(2)[2][:self.merged.shape[0]].values
    self.merged.columns = ['county', 'population', 'cases', 'day', 'deaths']
    #self.merged['death_counts'] = self.death_counts.values[:self.merged.shape[0]]
    #self.merged.to_csv(self.merged_path)
    return self.merged

                                                      
def data_in_index(row, col_name='index'):
  return 'data' in row[col_name]


###############################################################


######## covid land area #######################################
class LandArea():
  def __init__(self, land_area_path = "county_landarea.csv"):
    self.path = os.path.join(DATASET_DIR, land_area_path)
    self.land_area = None
  def get_landarea(self, use_cached=True):
    if use_cached and self.land_area is not None:
      return self.land_area
    self.land_area = pd.read_csv(self.path)
    self.land_area['land_area'] = self.land_area['LND010190D']
    self.land_area = self.land_area.drop(['LND010190D'], axis=1)
    return self.land_area
  
  
###############################################################

######################Policy Data###############################
class PolicyData():
  def __init__(self, policy_path = "state_policy_updates.csv", state_names_path = "state_id_to_name.csv"):
    self.policy_path = os.path.join(DATASET_DIR, policy_path)
    self.state_names_path = os.path.join(DATASET_DIR, state_names_path)
    self.policy_data = None

    self.selected_policies_for_dataset = [
      'State',
      'Election', 
      'Gatherings', 
      'Public Gatherings',
      
      'Health Risk Status', 
      
      'Mask Requirement',
      'Mandate Face Mask Use By All Individuals In Public Facing Businesses',
      'Mandate Face Mask Use By All Individuals In Public Spaces',
      
      
      'Medical', 
      
      'Non-Essential Businesses',
      
      'Quarantine',
      'Shelter in Place',
      
        'Travel', 
        'travel',
        'traveler from out of state',
      
      'Updated Guidelines',
      
      'start_stop',
      'date'
    ]
    
    # essential buissnesses according to https://www.cbia.com/resources/coronavirus/coronavirus-state-federal-updates/connecticut-designated-essential-businesses/
    # essential sectors
    self.essential_sectors = [
      "Chemical", 
      "Commercial Facilities", 
      "Communications", 
      "Critical Manufacturing", 
      "Dams", 
      "Defense Industrial Base", 
      "Emergency Services", 
      "Financial Services", 
      "Food and Agriculture", 
      "Government Facilities", 
      "Healthcare and Public Health", 
      "Information Technology", 
      "Nuclear", 
      "Transportation", 
      "Water"
    ]
    
    self.essential_activities = [
      'Accommodation',
      'Accommodation and Food Services',
      'Administrative and Support Services',
      'Administrative and Support and Waste Management and Remediation Services',
      'Agriculture, Forestry, Fishing and Hunting',
      'Ambulatory Health Care Services',
          

      'Automotive Repair and Maintenance',


      'Commercial and Industrial Machinery and Equipment (except Automotive and Electronic) Repair and Maintenance',

      'Construction of Buildings',
      
      'Death Care Services',
      'Drycleaning and Laundry Services',

      'Fabricated Metal Product Manufacturing',

      'Fishing, Hunting and Trapping',
      'Food Manufacturing',


      'Forestry and Logging',


      'Gasoline Stations',

      'Health Care and Social Assistance',
      'Health and Personal Care Stores',
      'Heavy and Civil Engineering Construction',


      'Information',

      'Machinery Manufacturing',

      'Manufacturing',
      'Merchant Wholesalers, Durable Goods',
      'Merchant Wholesalers, Nondurable Goods',
      'Mining, Quarrying, and Oil and Gas Extraction',


      'Motor Vehicle and Parts Dealers',
      'Nonmetallic Mineral Product Manufacturing',

      'Nursing and Residential Care Facilities',
      'Other Information Services',





      'Personal and Household Goods Repair and Maintenance',
      'Personal and Laundry Services',
      'Plastics and Rubber Products Manufacturing',
      'Primary Metal Manufacturing',
      'Professional, Scientific, and Technical Services',
      'Publishing Industries (except Internet)',
      'Rental and Leasing Services',
      'Repair and Maintenance',

      'Social Assistance',
      'Special Food Services',

      'Support Activities for Agriculture and Forestry',
      'Support Activities for Transportation',
      'Telecommunications',
      'Transit and Ground Passenger Transportation',
      'Transportation and Warehousing',

      'Truck Transportation',
      'Utilities',
    ]
    self.essential_activities = set(self.essential_activities)
    
  def get_policy_data(self, use_cached=True):
    if self.policy_data and use_cached:
      return self.policy_data
    self.policy_data = pd.read_csv(self.policy_path)
    state_id_name = pd.read_csv(self.state_names_path)
    state_id_name.append(['PR', 'Puerto Rico', 'Puerto Rico'])
    # map state ids to state names
    self.policy_data = pd.merge(self.policy_data, 
                                state_id_name, 
                                left_on='state_id', 
                                right_on='Code', 
                                how='left').drop(['state_id', 'Code', 'Abbrev'], axis=1)
    # rearrange columns so state's in the first column
    self.policy_data = self.policy_data.loc[:, [self.policy_data.columns[-1], *self.policy_data.columns[0:self.policy_data.shape[1]-1].values]]
    self.policy_data['State'] = self.policy_data['State'].apply(lambda v: ''.join(str(v).split(' ')))
    self.policy_data['county'] = self.policy_data['county'].apply(lambda v: ''.join(str(v).split(' ')))

    # create one-hot encodings for the policy type which describes which industry/activity was shutdown
    self.policy_data = pd.concat([self.policy_data, pd.get_dummies(self.policy_data['policy_type'])], axis=1)

    # only pick the selected policies (handpicked beforehand)
    self.policy_data = self.policy_data.loc[:, self.selected_policies_for_dataset]
    self.policy_data = group_policies([ 'Travel', 
                 'travel',
                 'traveler from out of state'], self.policy_data)
    self.policy_data = group_policies([
        'Mask Requirement',
        'Mandate Face Mask Use By All Individuals In Public Facing Businesses',
        'Mandate Face Mask Use By All Individuals In Public Spaces',
                
    ], self.policy_data)

    self.policy_data = group_policies([
        'Gatherings', 
        'Public Gatherings'
    ], self.policy_data)
    
    # change date representation
    self.policy_data['day'] =  self.policy_data['date'].apply(convert_to_int)
    self.policy_data[ self.policy_data['day'] < 0]['day'] = 0
    self.policy_data = self.policy_data.drop(['date'], axis=1)
    self.policy_data.drop_duplicates().fillna(0)
    return self.policy_data
  

def group_policies(col_names, df, new_col=None):
    '''
      combines different policy types into a single industry (since theres a lot of variance in the names)
    '''
    new_col = new_col if not new_col else col_names[0]
    group = df[col_names[0]]
    for name in col_names[1:]:
        group += df[name]
    df[new_col] = group.apply(lambda v: min(1, v))
    df = df.drop(col_names[1:], axis=1)
    return df
  
d = datetime.date(2020, 1, 10) - datetime.date(2020, 1, 1)
d.total_seconds() / 60 / 60 / 24
def convert_to_int(date_str):
    date = datetime.date(*[int(s) for s in date_str.split('-')])
    delta = date - datetime.date(2020, 1, 1)
    return int(delta.total_seconds() / 60 / 60 / 24)
  
def square_wave_interpolate(signal):
    wave = np.zeros_like(signal)
    mag = 0
    for i, (t, t_p_1) in enumerate(zip(signal[:-1], signal[1:])):
        if (t_p_1 - t) > 0:
            mag = 1
        elif (t_p_1 - t) < 0:
            mag = 0
        wave[i + 1] = mag
    wave[-1] = wave[-2]
    return wave

####################Combining the entire dataset############################################
class CountyDataLake:
  def __init__(self, path = os.path.join(DATASET_DIR, 'cccpaap_masked.csv'), cccpa_path = os.path.join(DATASET_DIR, 'cccpa.csv')):
    self.activities = CountyActivity()
    self.county_case_counts_pop =  C3aiDataLake()
    self.cccpa_path = cccpa_path
    self.county_case_counts_pop_activity=None

    self.policy = PolicyData()
    self.land_area = LandArea()
    self.path = path
    self.cccpaap = None
    self._process()
    
  def _process(self):
    self.get_county_case_counts_pop_activity_landarea_policy()
  
  def _county_names(self):
    return self.county_case_counts_pop._county_names()
    
  def county_activities(self):
    return self._get_county_activities()
  
  def county_case_counts(self):
    return self.county_case_counts_pop.get_county_case_counts()
  
  def county_population(self):
    return self.county_case_counts_pop.get_county_population()
  
  def county_landarea(self):
    return self.land_area.get_landarea()
  
  def county_policy(self):
    return self.policy.get_policy_data()
  
  def _get_county_activities(self, sector_ranges=[[100, 999], [7000, 7999], [8000, 8999]], use_cached=True):
    if use_cached and self.county_case_counts_pop_activity is not None:
      return self.county_case_counts_pop_activity
    if os.path.exists(self.cccpa_path):
      self.county_case_counts_pop_activity = pd.read_csv(self.cccpa_path).drop_duplicates().fillna(0)
      return self.county_case_counts_pop_activity
    
    county_names = np.unique(self._county_names())
    series = self.activities.get_activities(county_name=county_names[0], sector_ranges=sector_ranges)['Total CBP and NES Establishments']
    county_case_counts_pop_activity = pd.DataFrame(series).transpose()
    print("Merging county_case_counts with activities")
    with warnings.catch_warnings(record=True):
      start=1
      for county in tqdm(county_names[start:], total=len(county_names[start:])):
        series = self.activities.get_activities(county_name=county, sector_ranges=sector_ranges)['Total CBP and NES Establishments']
        if series.shape[0] > 0:
          county_case_counts_pop_activity = county_case_counts_pop_activity.append(series.to_dict(), ignore_index=True)
        else:
          print(f"skipping {county} with {series.shape[0]} activity entries")
        print(county_case_counts_pop_activity.head())
    
    self.county_case_counts_pop_activity = county_case_counts_pop_activity.drop_duplicates().fillna(0)
    self.county_case_counts_pop_activity.to_csv(self.cccpa_path)
    return self.county_case_counts_pop_activity
  
  def _mask_non_essential_activities(self, cccpaap):
    other_cols = set(list(cccpaap.columns.values[:4]) + list(cccpaap.columns.values[90:]))

    def mask(row):
      if row['Non-Essential Businesses'] >=.5:
          if not row['start_stop']:
              for col, value in row.items():
                  if col not in self.policy.essential_activities and col not in other_cols:
                      row[col] = 0
      return row
    return cccpaap.progress_apply(mask, axis=1)
  def get_county_case_counts_pop_activity_landarea_policy(self, use_cached=True):
    if use_cached and self.cccpaap is not None: 
      return self.cccpaap
    if os.path.exists(self.path):
      self.cccpaap = pd.read_csv(self.path)
      return  self.cccpaap
    
    # create the county case counts population activity dataset
    cccpa = self._get_county_activities()
    # create the land areas dataset and merge with cccpa
    county_areas = self.county_landarea()
    cccpaa= pd.merge(cccpa, county_areas, left_on='county', right_on='county', how='inner')
    cccpaa = cccpaa[cccpaa['land_area'] > 0]
    cccpaa['population_density'] = cccpaa.apply(lambda row: row['population'] / row['land_area'], axis=1)
    
    # create the policy dataset and merge with cccpaa
      # cccpaap should now contain an entry with each row listing a county, the activities, case counts, land area, and policy start and stop date
    cccpaap = _interpolate_policy(
      self._county_names(),
      self.policy.get_policy_data(),
      cccpaa
    )
    
    # using the interpolated policy mask out nonessential buissness that were shutdown at the time of the incident
    cccpaap = self._mask_non_essential_activities(cccpaap)
    self.cccpaap = cccpaap
    self.cccpaap.to_csv(self.path)
    return self.cccpaap

def _interpolate_policy(counties, policy, cccpaa):
  cccpaap_raw =  pd.merge(cccpaa, 
                      policy, 
                      left_on=['state', 'day'], 
                      right_on=['State', 'day'], how='left')
  
  policy_names = policy.drop(['date', 'day', 'State', 'start_stop', None], axis=1).columns.values
  cccpaap = pd.DataFrame(columns=cccpaap_raw.columns)
  with warnings.catch_warnings(record=True):
    for county_name in tqdm(counties):
      county = (cccpaap_raw[cccpaap_raw['county'] == county_name])
      county.loc[:, policy_names].interpolate(axis=1, inplace=True)
      county.loc[:, policy_names].fillna(0, inplace=True)
      county['start_stop'] = square_wave_interpolate(county['start_stop'].apply(lambda string: 0 if string =='stop' else 1))
      cccpaap = cccpaap.append(county)
  cccpaap = cccpaap.drop_duplicates().iloc[:, :-1].fillna(0)
  return cccpaap

def _drop_unlisted_activities(self):
   self.cccpaap =  self.cccpaap[self.cccpaap.drop(['day', 'county', 'cases', 'population'], axis=1).apply(has_listed_activities, axis=1)]


def has_listed_activities(row):
  for k,v in row.items():
    if v:
      return True
  return False


def compute_diffs(dataframe, Y_value='cases'):
    APPROX_NEG_INFTY=-1e2
    APPROX_POS_INFTY=1e2
    # create deltas
    # can't do this b/c we're computing spurious deltas b/w counties
    dataframe['daily_change'] = np.zeros(dataframe.shape[0])
    dataframe['daily_change'].iloc[1:] = dataframe[Y_value].values[1:] - dataframe[Y_value].values[:-1]
    dataframe['daily_change'] = dataframe['daily_change'].fillna(0)
    dataframe['daily_growth_rate'] = np.zeros(dataframe.shape[0])
    dataframe['daily_growth_rate'].iloc[1:] = (dataframe[Y_value].apply(lambda v: 1e-3 if v == 0.0 else v).values[1:] / dataframe[Y_value].apply(lambda v: 1e-3 if v == 0.0 else v).values[:-1])
    dataframe['daily_growth_rate'] = dataframe['daily_growth_rate'].apply(lambda v: 1e-3 if v == 0.0 else v).apply(np.log)
    return dataframe

