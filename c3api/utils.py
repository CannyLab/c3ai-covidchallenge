def isadjacent(county1, county2):
	f = open("counties_formatted.txt")
	while line := f.readline():
		line = line.split('\t')
		if line[0] == county1:
			return county2 in line