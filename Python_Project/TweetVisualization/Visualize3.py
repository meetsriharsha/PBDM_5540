import matplotlib
import numpy as np
import matplotlib.pyplot as plt
import re
import time
from dateutil.parser import parse

dt = []
count = []
f = open('K:/UMKC/Docs/Subjects/PBDM/PB_Project/TweetsPerTime/part-00000', 'rU')   #open the file in read universal mode
for line in f:
    cells = (re.sub('[\(\)\"\\n]', '', line)).split(",")
    dt.append(cells[0])
    count.append(cells[1])
f.close()

epochdt = [0] * len(dt)
for i in range(0, len(dt)):
    try:
        epochdtTmp = parse(dt[i])
    except ValueError:
        epochdtTmp = parse('Sun Nov 23 01:44:10 +0000 2015')
    epochdt[i] = time.mktime(epochdtTmp.timetuple())

finalList = [0] * len(count)
for i in range(0, len(count)):
    finalList[i] = [epochdt[i], count[i]]
finalListSorted = sorted(finalList, key=lambda x: x[0], reverse=False)
#print finalListSorted
countFinal = [0] * len(finalListSorted)
countFinal = [x[1] for x in finalListSorted]
#print countFinal
width = 0.5
ind = np.arange(len(countFinal))
plt.bar(ind, countFinal, width, color='g', label="distributions")
params = plt.gcf()
plSize = params.get_size_inches()
params.set_size_inches((plSize[0]*5, plSize[1]*5))
plt.ylabel('Tweet count')
plt.xlabel('Timeline')
plt.title('Distribution of tweets by Time')
#plt.xticks(ind+width, finalListSorted[:0])
plt.legend(loc='best')
plt.show()

