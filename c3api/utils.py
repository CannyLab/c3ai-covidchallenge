import c3api.c3aidatalake as c3aidatalake

def isadjacent(county1, county2):
	f = open("counties_formatted.txt")
	while line := f.readline():
		line = line.split('\t')
		if line[0] == county1:
			return county2 in line

def get_counties_query():
    # Total number of confirmed cases, deaths, and recoveries in Santa Clara, California
	today = pd.Timestamp.now().strftime("%Y-%m-%d")
	counties_time_intervals = []
	ids = list(counties['id'].values)
	for i in range(counties.shape[0] // 10):
		if i == 3: break;
		counties_time = c3aidatalake.evalmetrics(
			"outbreaklocation",
			{
				"spec" : {
					"ids": ids[i * 10: i * 10 + 10],
					"expressions" : ["CovidTrackingProject_ConfirmedCases", "CovidTrackingProject_ConfirmedDeaths"],
					"start" : "2020-01-01",
					"end" : today,
					"interval" : "DAY",
				#"filter" : "type == 'County'"
				}
			}
		)
		counties_time_intervals +=[counties_time]
	counties_time= pd.concat(counties_time_intervals, axis=1)
