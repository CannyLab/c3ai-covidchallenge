from covid_activity.dataset.dataset_constructor import CountyDataLake, compute_diffs
from covid_activity.experiments.causality import GrangerCausalityTest
from covid_activity.references import DATASET_DIR, cols
import os
from tqdm import tqdm
import json

def growth_rate_experiment(Y_value='cases'):
    dlake = CountyDataLake()
    cccpaap_masked = dlake.get_county_case_counts_pop_activity_landarea_policy()

    cccpaap_masked = compute_diffs(cccpaap_masked, Y_value=Y_value)
    # to combine these values could do a clustering by population density
    panel_exp = {}
    for county_name in tqdm(set(cccpaap_masked['county'].values)):
        county = cccpaap_masked[cccpaap_masked['county'] == county_name]
        gct = GrangerCausalityTest(
            X = county[cols],
            Y = county[['daily_growth_rate']],
            x_lag = 1,
            y_lag = 5,
            dummy_variables = 0
        )
        
        gct.fit()
        p_value = gct.F_test()
        #print(county['population'].iloc[0])
        panel_exp[county_name] = {
            'p_value': p_value,
            'full_model': list(gct.full_model.reshape(-1)),
            'reduced_model': list(gct.reduced_model.reshape(-1)),
            'population': county['population'].iloc[0],
            'population_density': county['population_density'].iloc[0]
        }
        
    with open(os.path.join(DATASET_DIR, f'{Y_value}_full_policy_model.json'), 'w', encoding='utf-8') as f:
        json.dump(panel_exp, f, indent=4)
        
    #combined_panel_experiment(panel_exp, Y_value)
 
#  def combined_panel_experiment(panel_exp, Y_value):
#     # compute the country model weighted by population density
#     county, result = next(granger_casuality_county_results.items())
#     full_model = np.zeros_like(result['full_model'])
#     reduced_model = np.zeros_like(result['reduced_model'])
#     weights = []
#     for county, result in granger_casuality_county_results.items():
#         population_density = result['population_density']
#         full_model += np.asarray(result['full_model']) * population_density
#         reduced_model += np.asarray(result['reduced_model'])  * population_density
#         weights.append(population_density)

#     full_model /= np.sum(weights)
#     reduced_model /= np.sum(weights)
    
#     full_model = full_model.reshape(-1, 1)
#     reduced_model = reduced_model.reshape(-1,1)
    
    
#     cccpaap_masked = dlake.get_county_case_counts_pop_activity_landarea_policy()
#     cccpaap_masked = compute_diffs(cccpaap_masked, Y_value='cases')
#     print("Starting Test")
#     gct = GrangerCausalityTest(
#                 X = cccpaap_masked[cols],
#                 Y = cccpaap_masked[['daily_growth_rate']],
#                 x_lag = 1,
#                 y_lag = 5,
#                 dummy_variables = 0
#             )
#     gct.full_model = full_model
#     gct.reduced_model = reduced_model
#     p_value = gct.F_test()
#     results = {
#             'p_value': p_value,
#             'full_model': list(gct.full_model.reshape(-1)),
#             'reduced_model': list(gct.reduced_model.reshape(-1)),
#             'population': county['population'].iloc[0],
#             'population_density': county['population_density'].iloc[0]
#         }
#         with open(os.path.join(DATASET_DIR, f'{Y_value}_full_policy_model.json'), 'w', encoding='utf-8') as f:
#         json.dump(panel_exp, f, indent=4)
    

if __name__ == '__main__':
   #growth_rate_experiment('cases')
    growth_rate_experiment('deaths')
    
    
    