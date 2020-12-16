from covid_activity.dataset.dataset_constructor import CountyDataLake, compute_diffs
from covid_activity.experiments.causality import FeatureImportance
from covid_activity.references import DATASET_DIR, cols
import os
from sklearn.neural_network import MLPRegressor
from sklearn.preprocessing import StandardScaler as Scalar
import pickle
import json

if __name__ == '__main__':
    dlake = CountyDataLake()
    cccpaap_masked = dlake.get_county_case_counts_pop_activity_landarea_policy()
    cccpaap_masked = compute_diffs(cccpaap_masked)
    
  
    clf = MLPRegressor(hidden_layer_sizes=(100, 100, 100))
    scalar = Scalar()
    
    X = cccpaap_masked[cols]
    Y = cccpaap_masked['daily_growth_rate']
    
    FI_test = FeatureImportance(
        clf = clf,
        X = X, 
        Y = Y,
        scalar = scalar 
    )
    
    print(f"fitting clf {clf}")
    FI_test.fit()
    print("Running feature_importance test")
    fi_train = FI_test.feature_importance_test(test=False)
    fi_test = FI_test.feature_importance_test(test=True)
    with open(os.path.join(DATASET_DIR, 'feature_importance_train.json'), 'w', encoding='utf-8') as f:
        json.dump(fi_train, f, indent=4)
    
    with open(os.path.join(DATASET_DIR, 'feature_importance_test.json'), 'w', encoding='utf-8') as f:
        json.dump(fi_test, f, indent=4)

    with open(os.path.join(DATASET_DIR, 'feature_importance_model.pickle'), 'w', encoding='utf-8') as f:
        s = pickle.dump(clf, f)
    

