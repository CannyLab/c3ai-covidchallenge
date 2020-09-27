import c3api.c3aidatalake as datalake
from c3api.loaders.generalData_loader import GeneralDataLoader
import pandas as pd


class HospitalDataLoader(GeneralDataLoader):
    
    def __init__(self):
        super(HospitalDataLoader, self).__init__()
        
    
    def fetch(self,
              location : str,
              name : str = "", 
              hospitalType : str = "",
              address : str = "", 
              lat : float = None, 
              lon : float = None,
              liscensedBeds : int = None, 
              staffedBeds : int = None, 
              icuBeds : int = None,
              icuBedsAdult : int = None,
              icuBedsPedi : int = None, 
              icuBedsPotential : int = None,
              ventilatorUsage : int = None, 
              bedUtilization : float = None, 
              limit : int = None
            ) -> pd.DataFrame:
        '''fetch following the c3ai. api https://c3.ai/covid-19-api-documentation/#tag/Hospital/paths/~1api~11~1hospital~1fetch/post
           gets information about hospitals from c3ai's datalake

        Args:
            name (str): Hosptial name
            location (OutbreakLocation): C3.ai Type OutbreakLocation associated with hospital.
            hospitalType (str): Hospital type. Short Term Acute Care Hospital, Critical Access Hospital, VA Hospital, etc.
            address (str): Hospital address
            lat (float): Hospital's lat
            lon (float): Hospital's lon
            liscensedBeds (int): Maximum number of beds the hospital holds the license to operate
            staffedBeds (int): Number of adult, pediatric, birthing room, and ICU beds maintained in patient care areas of the hospital.
            icuBeds (int): Number of intensive care unit (ICU) beds in the hospital
            icuBedsAdult (int): Number of ICU beds for adults in the hospital
            icuBedsPedi (int): Number of pediatric ICU beds in the hospital
            icuBedsPotential (int): Potential increase in bed capacity in the hospital, computed as the number of licensed beds minus the number of staffed beds
            ventilatorUsage (int): Hospital's average number of ventilators in use
            bedUtilization (float): Hospital's average bed utilization rate, computed based on the Medicare Cost Report as the total number of patient days (excluding nursery days) divided by the available bed days.
        '''
        field_names = [ 'name', 
                        'hospitalType', 
                        'address', 
                        'lat', 
                        'lon', 
                        'liscensedBeds', 
                        'staffedBeds', 
                        'icuBeds', 
                        'icuBedsAdult', 
                        'icuBedsPedi', 
                        'icuBedsPotential', 
                        'ventilatorUsage', 
                        'bedUtilization']
        fields = [id_ for id_ in 
                            [name, 
                            hospitalType, 
                            address, 
                            lat, 
                            lon, 
                            liscensedBeds, 
                            staffedBeds, 
                            icuBeds, 
                            icuBedsAdult, 
                            icuBedsPedi, 
                            icuBedsPotential, 
                            ventilatorUsage, 
                            bedUtilization]
                         ] 
        
        clause = '&&'.join([f" {name} == '{field}'" for name, field in zip(field_names, fields) if field])
        
        # super(HospitalDataLoader, self).fetch(
        #     location, 
        #     include_fields = include_fields, 
        #     limit = limit
        # )
        assert location, "location must be specified"
        print(location)
        print(clause)
        spec = {
            "filter" : f"contains(location, '{location}')" + " && " + clause,
        }
        
        
        if limit: 
            spec['limit'] = limit
        
        print(spec)
        return datalake.fetch(
            "hospital", 
            {
                "spec":spec
            }, 
            get_all=True
        )
        

def test():
   
    
    # population = datalake.fetch(
    #     "hospital",
    #     {
    #         "spec" : {
    #             "filter" : "contains(location, 'Oklahoma')"
    #         }
    #     },
    #     get_all = True
    # )
    from c3api.loaders.HospitalDataLoader import HospitalDataLoader 
    hospdl = HospitalDataLoader()     
    return hospdl.fetch("Oklahoma", address="1401 W Locust St,Stilwell,74960,OK,USA")   
