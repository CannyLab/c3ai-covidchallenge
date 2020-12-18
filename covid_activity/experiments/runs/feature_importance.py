from covid_activity.dataset.dataset_constructor import CountyDataLake, compute_diffs
from covid_activity.experiments.causality import FeatureImportance
from covid_activity.references import DATASET_DIR, cols
import os
from sklearn.neural_network import MLPClassifier
from sklearn.preprocessing import StandardScaler as Scalar
import pickle
import json
from tqdm import tqdm 

if __name__ == '__main__':
    dlake = CountyDataLake()
    cccpaap_masked = dlake.get_county_case_counts_pop_activity_landarea_policy()
    cccpaap_masked = compute_diffs(cccpaap_masked)
    
  

    panel_exp = {}
    models = {}
    for county_name in tqdm(set(cccpaap_masked['county'].values)):
        county = cccpaap_masked[cccpaap_masked['county'] == county_name]
        X = county[
        cols + [
            'start_stop',
            ]
        ]
        Y = county['daily_growth_rate']
        
        model = MLPClassifier(hidden_layer_sizes=(100, 100), max_iter=200)
        FI_test = FeatureImportance(
            model = model,
            X = X, 
            Y = Y,
            scalar = None 
        )
    
        #print(f"fitting reg {model}")
        FI_test.fit()
        # with open(os.path.join(DATASET_DIR, 'feature_importance_model.pickle'), 'wb') as f:
        #     s = pickle.dump(model, f)
        print(f"Running feature_importance test for {county_name}")
        fi_train = FI_test.feature_importance_test(test=False)
        fi_test = FI_test.feature_importance_test(test=True)
        panel_exp[county_name] = {
                'train': fi_train, 
                'test': fi_test, 
            }
        models[county_name] = {
            pickle.dumps(model)
        }
    
        with open(os.path.join(DATASET_DIR, 'feature_importance_panel_exp.json'), 'w', encoding='utf-8') as f:
            json.dump(panel_exp, f, indent=4)
        
        with open(os.path.join(DATASET_DIR, 'feature_importance_test.json'), 'wb') as f:
            json.dump(models, f)
    

