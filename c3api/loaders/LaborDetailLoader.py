import c3api.c3aidatalake as datalake
from c3api.loaders.generalData_loader import GeneralDataLoader
import pandas as pd


class LaborDetailLoader(GeneralDataLoader):
    
    def __init__(self):
        super(LaborDetailLoader, self).__init__()
        
    
    def fetch(self,
              parent,
              year : int = None, 
              month : int = None,
              laborForce : int = None,
              employedPopulation : int = None,
              unemployedPopulation : int = None,
              unemployentRate : float = None,
              origin : str = ""
 
            ) -> pd.DataFrame:

        field_names = [ 'year', 
                        'month', 
                        'laborForce', 
                        'employedPopulation', 
                        'unemployedPopulation', 
                        'unemployentRate', 
                        'origin']

        fields = [id_ for id_ in 
                            [year,
                            month, 
                            laborForce,
                            employedPopulation,
                            unemployedPopulation,
                            unemployentRate,
                            origin]
                         ] 
        
        clause = '&&'.join([f" {name} == '{field}'" for name, field in zip(field_names, fields) if field])
        

        assert parent, "location must be specified"
        print(parent)
        print(clause)
        spec = {
            "filter" : f"contains(parent, '{parent}')" + " && " + clause,
        }
        
        
        if limit: 
            spec['limit'] = limit
        
        print(spec)
        return datalake.fetch(
            "labordetail", 
            {
                "spec":spec
            }, 
            get_all=True
        )
        

def test():
   
    
    # labor = datalake.fetch(
    #     "labordetail",
    #     {
    #         "spec" : {
    #             "filter" : "contains(location, 'Oklahoma')"
    #         }
    #     },
    #     get_all = True
    # )
    from c3api.loaders.LaborDetailLoader import LaborDetailLoaderLoader 
    hospdl = HospitalDataLoader()     
    return hospdl.fetch("Oklahoma", year = 2020)   
