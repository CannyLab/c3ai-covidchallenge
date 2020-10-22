import c3api.c3aidatalake as datalake
from c3api.loaders.generalData_loader import GeneralDataLoader
import pandas as pd

class OutbreakLocationLoader(GeneralDataLoader):
    
    def __init__(self):
        super(LaborDetailLoader, self).__init__()
        
    #returns 2000 entries max
    def fetch(self,
              parent,
              id : str = None, # The location ID of the country, state or province, and county to fetch for the COVID-19 outbeak. Should be used with the key filter, e.g., "filter": 'id == "Afghanistan"'.
        	  name : str = None, #	Actual name of the country, state or province, and county corresponding to the location ID.
			  fips : str = None, #	FIPS code for the country, and for the county and county-equivalents in the United States.
'locationType', 'populationCDS', 'id', 'name', 'typeIdent',
       'location.value.id', 'location.timestamp', 'hospitalIcuBeds',
       'hospitalStaffedBeds', 'hospitalLicensedBeds', 'latestTotalPopulation',
       'populationOfAllChildren', 'latestLaborForce',
       'latestEmployedPopulation', 'latestUnemployedPopulation',
       'latestUnemploymentRate', 'laborForceOfAllChildren', 'fips.id',
       'hospitalPrediction.timestamp', 'countryArea', 'countryCode',
       'population2018', 'population2019', 'latestKindergartenPopulation',
       'elementarySchoolCount', 'kindergartenCount', 'universityCount',
       'nursingHomeCount', 'elderlyPopulationRatio', 'elderlyAloneRatio'
            ) -> pd.DataFrame:
# id 	string 	The location ID of the country, state or province, and county to fetch for the COVID-19 outbeak. Should be used with the key filter, e.g., "filter": 'id == "Afghanistan"'.
# name 	string 	Actual name of the country, state or province, and county corresponding to the location ID.
# fips 	string 	FIPS code for the country, and for the county and county-equivalents in the United States.
# lineList 	LineListRecord 	List of C3.ai Type LineListRecord objects associated with the location.
# assets 	BiologicalAsset 	List of C3.ai Type BiologicalAsset objects associated with the location.
# diagnoses 	Diagnosis 	List of C3.ai Type Diagnosis objects associated with the location.
# hospitals 	Hospital 	List of C3.ai Type Hospital objects associated with the location.
# policy 	LocationPolicySummary 	List of C3.ai Type LocationPolicySummary objects associated with the location.
# populationData 	PopulationData 	List of C3.ai Type PopulationData objects associated with the location.
# laborDetail 	LaborDetail 	List of C3.ai Type LaborDetail objects associated with the location.
# vaccineCoverage 	VaccineCoverage 	List of C3.ai Type VaccineCoverage objects associated with the location.
# locationExposures 	LocationExposure 	List of C3.ai Type LocationExposure objects with this location as locationTarget.
# locationExposuresVisited 	LocationExposure 	List of C3.ai Type LocationExposure objects with this location as locationVisited.
# latestTotalPopulation 	int 	Most recent population of the location based on data from The World Bank or the US Census Bureau. Data available at county-level for locations in the United States and country-level globally.
# population2019 	int 	Population of the location for the year 2019, based on data from the European Centre for Disease and Control.
# populationCDS 	int 	Population of the location, based on data from Corona Data Scraper.
# hospitalIcuBeds 	int 	Total number of hospital intensive care unit (ICU) beds. Available for locations in the United States.
# hospitalStaffedBeds 	int 	Total number of staffed hospital beds. Available for locations in the United States.
# hospitalLicensedBeds 	int 	Total number of licensed hospital beds. Available for locations in the United States.
# populationOfAllChildren 	int 	Most up-to-date total population of all sub-locations (e.g. for all counties in a state) based on available demographic data. Available for locations in the United States.
# latestLaborForce 	int 	Most up-to-date labor force population of the location based on available Bureau of Labor Statistics data. Available for county locations in the United States.
# latestEmployedPopulation 	int 	Most up-to-date employed population of the location based on available Bureau of Labor Statistics data. Available for county locations in the United States.
# latestUnemployedPopulation 	int 	Most up-to-date unemployed population of the location based on available Bureau of Labor Statistics data. Available for county locations in the United States.
# latestUnemploymentRate 	double 	Most up-to-date unemployment rate of the location based on available Bureau of Labor Statistics data, in percent. Available for county locations in the United States.
# laborForceOfAllChildren 	int 	Most up-to-date labor force population of all sub-locations (e.g. for all counties in a state) based on available Bureau of Labor Statistics data. Available for US state- and country-level locations.
# employedPopulationOfAllChildren 	int 	Most up-to-date employed population of all sub-locations (e.g. for all counties in a state) based on available Bureau of Labor Statistics data. Available for US state- and country-level locations.
# unemployedPopulationOfAllChildren 	int 	Most up-to-date unemployed population of all sub-locations (e.g. for all counties in a state) based on available Bureau of Labor Statistics data. Available for US state- and country-level locations.
# unemploymentRateOfAllChildren 	double 	Most up-to-date unemployment rate, in percent, over all sub-locations (e.g. for all counties in a state) based on available Bureau of Labor Statistics data. This value is unemployedPopulationOfAllChildren divided by laborForceOfAllChildren, in percent. Available for US state- and country-level locations.
# elementarySchoolCount 	int 	Total number of elementary schools. Available for locations in South Korea.
# kindergartenCount 	int 	Total number of kindergartens. Available for locations in South Korea.
# universityCount 	int 	Total number of universities. Available for locations in South Korea.
# nursingHomeCount 	int 	Total number of nursing homes. Available for locations in South Korea.
# elderlyPopulationRatio 	double 	Proportion of population that is elderly, as percent (0-100). Available for locations in South Korea.
# elderlyAloneRatio 	double 	Proportion of households that are elderly people living alone, as percent (0-100). Available for locations in South Korea.
# publicHealthCareCenterBeds 	int 	Total number of hospital beds available in public facilities. Available for locations in India.

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
