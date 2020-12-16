from covid_activity.dataset.dataset_constructor import CountyDataLake, compute_diffs
from covid_activity.experiments.causality import GrangerCausalityTest
from covid_activity.references import DATASET_DIR, cols
import os
from tqdm import tqdm
import json

if __name__ == '__main__':
    dlake = CountyDataLake()
    cccpaap_masked = dlake.get_county_case_counts_pop_activity_landarea_policy()

    cccpaap_masked = compute_diffs(cccpaap_masked)
    # to combine these values could do a clustering by population density
    panel_exp = {}
    i = 0
    for county_name in tqdm(set(cccpaap_masked['county'].values)):
        if i == 2: break;
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
        i += 1
        
    with open(os.path.join(DATASET_DIR, 'full_policy_model.json'), 'w', encoding='utf-8') as f:
        json.dump(panel_exp, f, indent=4)
    
    
    