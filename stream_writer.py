import time

f = open('../data.csv','r')
lines = f.readlines()

for line in range(0,len(lines),30):
    df = []
    for l in range(line):
        df.append(lines[l])
    print(line)
    fnew = open('./my_logs/'+str(line)+".psv", "w")
    for d in df:
        fnew.write(d)
    fnew.close()
    time.sleep(5)
