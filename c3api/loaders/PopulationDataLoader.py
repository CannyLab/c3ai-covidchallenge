import c3api.c3aidatalake as datalake
from c3api.loaders.generalData_loader import GeneralDataLoader
import pandas as pd


class PopulationDataLoader(GeneralDataLoader):
    
    def __init__(self):
        super(PopulationDataLoader, self).__init__()
        
    
    def fetch(self,
              parent,
              year : int = None,
              populationAge: str = "",
              gender: str = "", 
              race: str = "",
              ethnicity: str = "",
              estimate: bool = None,
              median: bool = None,
              value: float = None,
              minAge: int = None,
              maxAge: int = None,
              origin: str = ""


            ) -> pd.DataFrame:

        field_names = [ 'year', 
                        'populationAge', 
                        'gender', 
                        'race', 
                        'ethnicity', 
                        'estimate', 
                        'median', 
                        'value', 
                        'minAge', 
                        'maxAge', 
                        'origin',]
        fields = [id_ for id_ in 
                            [year,
                            populationAge,
                            gender,
                            race, 
                            ethnicity,
                            estimate,
                            median,
                            value,
                            minAge,
                            maxAge,
                            origin]
                         ] 
        
        clause = '&&'.join([f" {name} == '{field}'" for name, field in zip(field_names, fields) if field])
        
        # super(PopulationDataLoader, self).fetch(
        #     parent, 
        #     include_fields = include_fields, 
        #     limit = limit
        # )
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
            "populationdata", 
            {
                "spec":spec
            }, 
            get_all=True
        )
        

def test():
   
    
    # population = datalake.fetch(
    #     "populationdata",
    #     {
    #         "spec" : {
    #             "filter" : "contains(parent, 'White alone')"
    #         }
    #     },
    #     get_all = True
    # )
    from c3api.loaders.PopulationDataLoader import PopulationDataLoader
    popdl = PopulationDataLoader()     
    return popdl.fetch("Oklahoma", race="White alone")   