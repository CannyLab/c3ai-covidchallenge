from scipy.stats import f as F
import numpy as np
from tqdm import tqdm 
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

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
   


def feature_importance_test():
    pass

def derivative_test():
    pass

