import pandas as pd
import time

f = open('us_places.factual.2020-03-20.1585917865.tab','r')
lines = f.readlines()

for line in range(0,len(lines),30):
	df = []
	for l in range(line):
		values = lines[l].split("\t")
		state, count = values[6], 1
		df.append((state,count))
	print(line)
	df = pd.DataFrame(df)
	df.to_csv('./my_logs/'+str(line)+".tsv",index=False)
	time.sleep(5)
