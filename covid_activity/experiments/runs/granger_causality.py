from covid_activity.dataset.dataset_constructor import CountyDataLake, compute_diffs
from covid_activity.experiments.causality import GrangerCausalityTest
from covid_activity.references import DATASET_DIR
import os
from tqdm import tqdm
import json

if __name__ == '__main__':
    dlake = CountyDataLake()
    cccpaap_masked = dlake.get_county_case_counts_pop_activity_landarea_policy()
    cols = [
       'Accommodation',
       'Accommodation and Food Services',
       'Administrative and Support Services',
       'Administrative and Support and Waste Management and Remediation Services',
       'Agriculture, Forestry, Fishing and Hunting',
       'Ambulatory Health Care Services',
       'Amusement, Gambling, and Recreation Industries',
       'Arts, Entertainment, and Recreation',
       'Automotive Repair and Maintenance',
       'Building Material and Garden Equipment and Supplies Dealers',
       'Clothing and Clothing Accessories Stores',
       'Commercial and Industrial Machinery and Equipment (except Automotive and Electronic) Repair and Maintenance',
       'Construction', 'Construction of Buildings',
       'Credit Intermediation and Related Activities',
       'Death Care Services', 'Drycleaning and Laundry Services',
       'Educational Services', 'Fabricated Metal Product Manufacturing',
       'Finance and Insurance', 'Fishing, Hunting and Trapping',
       'Food Manufacturing', 'Food Services and Drinking Places',
       'Food and Beverage Stores', 'Forestry and Logging',
       'Furniture and Home Furnishings Stores',
       'Furniture and Related Product Manufacturing', 'Gasoline Stations',
       'General Merchandise Stores', 'Health Care and Social Assistance',
       'Health and Personal Care Stores',
       'Heavy and Civil Engineering Construction',
       'Independent Artists, Writers, and Performers',
       'Industries not classified', 'Information',
       'Insurance Carriers and Related Activities',
       'Machinery Manufacturing',
       'Management of Companies and Enterprises', 'Manufacturing',
       'Merchant Wholesalers, Durable Goods',
       'Merchant Wholesalers, Nondurable Goods',
       'Mining, Quarrying, and Oil and Gas Extraction',
       'Miscellaneous Manufacturing', 'Miscellaneous Store Retailers',
       'Motor Vehicle and Parts Dealers',
       'Nonmetallic Mineral Product Manufacturing', 'Nonstore Retailers',
       'Nursing and Residential Care Facilities',
       'Other Information Services', 'Other Personal Services',
       'Other Services (except Public Administration)',
       'Performing Arts Companies',
       'Performing Arts, Spectator Sports, and Related Industries',
       'Personal Care Services',
       'Personal and Household Goods Repair and Maintenance',
       'Personal and Laundry Services',
       'Plastics and Rubber Products Manufacturing',
       'Primary Metal Manufacturing',
       'Printing and Related Support Activities',
       'Professional, Scientific, and Technical Services',
       'Publishing Industries (except Internet)', 'Real Estate',
       'Real Estate and Rental and Leasing', 'Religious Organizations',
       'Religious, Grantmaking, Civic, Professional, and Similar Organizations',
       'Rental and Leasing Services', 'Repair and Maintenance',
       'Restaurants and Other Eating Places', 'Retail Trade',
       'Securities, Commodity Contracts, and Other Financial Investments and Related Activities',
       'Social Assistance', 'Special Food Services',
       'Specialty Trade Contractors', 'Spectator Sports',
       'Sporting Goods, Hobby, Musical Instrument, and Book Stores',
       'Support Activities for Agriculture and Forestry',
       'Support Activities for Transportation', 'Telecommunications',
       'Transit and Ground Passenger Transportation',
       'Transportation and Warehousing', 'Traveler Accommodation',
       'Truck Transportation', 'Utilities',
       'Wholesale Electronic Markets and Agents and Brokers',
       'Wholesale Trade', 'Wood Product Manufacturing', 'population_density',
        ]

    cccpaap_masked = compute_diffs(cccpaap_masked)
    # to combine these values could do a clustering by population density
    panel_exp = {}
    for county_name in tqdm(set(cccpaap_masked['county'].values)):
        county = cccpaap_masked[cccpaap_masked['county'] == county_name]
        gct = GrangerCausalityTest(
            X = cccpaap_masked[cols],
            Y = cccpaap_masked[['daily_growth_rate']],
            x_lag = 1,
            y_lag = 5,
            dummy_variables = 0
        )
        
        gct.fit()
        p_value = gct.F_test()
        #print(county['population'].iloc[0])
        panel_exp[county_name] = {
            'p_value': p_value,
            'full_model': gct.full_model,
            'reduced_model': gct.reduced_model,
            'population': county['population'].iloc[0],
            'population_density': county['population_density'].iloc[0]
        }
    with open(os.path.join(DATASET_DIR, 'full_policy_model.json'), 'w', encoding='utf-8') as f:
        json.dump(panel_exp, f)
    
    
    