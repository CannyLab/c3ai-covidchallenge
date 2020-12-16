
def test_counties_download():
    pass

def test_census_download():
    pass

def test_case_counts_download():
    pass

def test_activities_by_county_creation():
    from covid_activity.dataset.dataset_constructor import CountyActivity
    ca = CountyActivity()
    ca.get_activities(county='Alameda County')
    assert True
    
def test_counties_dataset_creation():
    from covid_activity.dataset.dataset_constructor import C3aiDataLake
    c3ai_dlake = C3aiDataLake()
    c3ai_dlake.get_counties()
    assert True

def test_county_case_counts_creation():
    from covid_activity.dataset.dataset_constructor import C3aiDataLake
    c3ai_dlake = C3aiDataLake()
    c3ai_dlake.get_county_case_counts()
    assert True


def test_county_population_creation():
    from covid_activity.dataset.dataset_constructor import C3aiDataLake
    c3ai_dlake = C3aiDataLake()
    c3ai_dlake.get_county_population()
    assert True

def test_county_landarea_creation():
    pass
