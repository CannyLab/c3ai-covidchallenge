from scipy.stats import f as F
import numpy as np
from tqdm import tqdm 
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
# class Causality:
#     def __init__(self, 
#                  X,
#                  Y,
#                  ):
#         self.X = X
#         self.Y = Y
# class AutoRegressiveModel:
#     def __init__(self, coefficents)

def F_score(r_sum, f_sum, r_dof, f_dof):
    return ((r_sum - f_sum) / (r_dof - f_dof)) / (f_sum / f_dof)

def normal_equations(X, Y):
    #print((X.T @ X).shape, (X.T@Y).shape)
    return np.linalg.pinv((X.T @ X)) @ (X.T @ Y)# returns theta

class GrangerCausalityTest:
    def __init__(self,
                 X, 
                 Y, 
                 x_lag = 1, 
                 y_lag = 5,
                 dummy_variables = 0
                 ):
        '''Training follows three steps for each model
           0. Creation of the dataset with the number of features representing the lag
           1. OLS regression on these values (can add dummy variables to include other effects) for both models
           2. Then apply the Granger Causality test using an F-metric 
                2.1 Compute residuals
                2.2 input the residuals squared as well as the degrees of freedom into F-distribution to determine p
        '''
        assert X.shape[0] == Y.shape[0], 'X and Y number of datapoints dont match'
        
        self.X = X
        self.Y = Y
        self.x_lag = x_lag
        self.y_lag = y_lag
        
        self.full_model = None # this is our Y only autoregressive model
        # null hypothesis = all weights for additionally x values are close to zero and not helpful
        self.reduced_model = None # this is our X and Y autoregressive modle
        
        # print(self.Y.values)

        # self.sscaler = MinMaxScaler()
        # self.sscaler.fit(Y * 1000.) 
        # self.Y = pd.DataFrame(self.sscaler.transform(Y) / 1000.)
        # print(self.Y.values)
        
        
        N = self.X.shape[0]
        self.full_dof = N - x_lag - y_lag - 1 - dummy_variables
        self.red_dof = N - y_lag - 1
        self.X_big_boi = None
        self.Y_big_boi = None
        
        # create the matrix we'll be using for computing the model parameters for each t
        X_dfs= []
        for lag in range(self.x_lag):
            X_dfs.append(self.X.shift(lag))
            
        
        Y_dfs = []
        for lag in range(self.y_lag):
            X_dfs.append(self.Y.shift(lag))
            Y_dfs.append(self.Y.shift(lag))
        
        
        
        
        self.X_big_boi = pd.concat(X_dfs, axis=1)
        for dv in range(dummy_variables):
           self.X_big_boi[f'dv_{dv}'] =  np.ones(self.X_big_boi.shape[0])
        self.Y_big_boi = pd.concat(Y_dfs, axis=1)
        
    
        
        self.X_big_boi = self.X_big_boi[max(self.x_lag, self.y_lag):-1].to_numpy()
        self.Y_big_boi = self.Y_big_boi[max(self.x_lag, self.y_lag):-1].to_numpy()
        
        self.Y_targets = self.Y[max(self.y_lag, x_lag) + 1:].to_numpy()
            
        # print(self.X_big_boi.shape)
        # print(self.Y_targets.shape)

        
    def F_test(self):
        
        red_preds_error= ((self.Y_targets - self.Y_big_boi @ self.reduced_model) ** 2).sum()
        full_residual_error = ((self.Y_targets - self.X_big_boi @ self.full_model) ** 2).sum()
        
       # print(red_preds_error, full_residual_error)
        
        f_score = F_score(red_preds_error, full_residual_error, self.red_dof, self.full_dof)
       # print(f_score)
        
        F_dist = F(self.red_dof - self.full_dof, self.full_dof)
        p_value = 1 - F_dist.cdf(f_score)
        
        return p_value
        
    
    def fit(self):
        self.reduced_model = normal_equations(self.Y_big_boi, self.Y_targets)
        self.full_model = normal_equations(self.X_big_boi, self.Y_targets)
   


class FeatureImportance:
    def __init__(self, 
                 model,
                 X: pd.DataFrame, 
                 Y: pd.DataFrame,
                 scalar = None
                 ):
        np.random.seed(555)
        self.model = model
        self.X_train,self.X_test, self.Y_train, self.Y_test  = train_test_split(X.to_numpy(),Y.to_numpy())
        if scalar:
            self.scalar = scalar
            self.X_train = scalar.fit_transform(self.X_train)
            self.X_test = scalar.transform(self.X_test)
        self.num_features = self.X_train.shape[1]
        
    def fit(self):
        self.model.fit(self.X_train,self.Y_train)
    
    def permuted_accuracy(self, i, X, Y):
        X_perm = X.copy()
        feature = X_perm[:, i]
        feature = np.random.choice(feature, size=len(feature))
        X_perm[:, i] = feature
        Y_perm_pred = self.model.predict(X_perm)
        accuracy = np.count_nonzero(Y_perm_pred == Y) / len(Y) 
        return accuracy

    def _feature_importance(self, ith_feature,accuracy, test=True):
        X, Y = (self.X_test, self.Y_test) if test else (self.X_train, self.Y_train)
        accuracy_perm = self.permuted_accuracy(ith_feature, X, Y)
        feature_importance = (1 - accuracy_perm + 1e-8) / (1- accuracy + 1e-8)
        return {f"FI_{ith_feature}":feature_importance, 
                "accuracy_perm": accuracy_perm
                }

    def feature_importance_test(self, test=True):
        X, Y = (self.X_test, self.Y_test) if test else (self.X_train, self.Y_train)
        accuracy = np.count_nonzero(np.self.model.predict(X) == Y)
        fi_tests = {'accuracy':accuracy}
        for i in range(self.num_features):
            fi_tests[i] = self._feature_importance(i,accuracy, test=test)
        return fi_tests
        

#class SIR_model
def derivative_test():
    pass

