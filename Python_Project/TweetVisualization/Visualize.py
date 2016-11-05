import matplotlib
import numpy as np
import matplotlib.pyplot as plt
import re
from matplotlib import cm
import matplotlib.colors as clrs
from operator import itemgetter

lang = []
count = []
langCountList = []
langCountSorted = []
topLangCount = []
topLang = []
f = open('K:/UMKC/Docs/Subjects/PBDM/PB_Project/TweetsPerLang/part-00000', 'rU')   #open the file in read universal mode
for line in f:
    cells = line.split(",")
    lang.append((re.sub('[\(\)\"]', '', cells[0])))    #since we want the first, second and third column
    count.append((re.sub('[\(\)\"]', '', cells[1])))
f.close()
file = open('K:/UMKC/Docs/Subjects/PBDM/PB_Project/TweetsPerLang/part-00000', 'r')
lines = file.readlines()
file.close()
langCountList = [(re.sub('[\(\)\"\\n]', '', line)).split(',') for line in lines]
langCountListSorted = sorted(langCountList, key=lambda x: float(x[1]), reverse=True)
#print langCountListSorted
for list in langCountListSorted[:10]:
    #cells = list.split(",")
    topLang.append(list[0])
    topLangCount.append(list[1])
#langCountSorted = sorted(langCountList, key=itemgetter(1))
#print "\n".join(langCountSorted)
print topLang
print topLangCount
ind = np.arange(len(lang))
width = 0.35
sentimentDistribution = [1, 2, 3, 4]
bar = plt.bar(ind, count, width, color='g', label="distributions")
params = plt.gcf()
plSize = params.get_size_inches()
params.set_size_inches((plSize[0]*5, plSize[1]*5))
plt.ylabel('Tweet count')
plt.xlabel('Tone')
plt.title('Distribution of tweets by Language')
plt.xticks(ind+width, lang)
plt.legend(loc='best')
plt.show()

#color_vals = [-1, 0, 1]
#my_norm = clrs.Normalize(-1, 1)    # maps your data to the range [0, 1]
#my_cmap = matplotlib.cm.get_cmap('jet')    # can pick your color map
colorArray = ['yellowgreen', 'gold', 'purple', 'skyblue', "violet", "green", "pink", "lightyellow", "red","brown"]
f = plt.figure()
params = plt.gcf()
plSize = params.get_size_inches()
params.set_size_inches((plSize[0]*5, plSize[1]*5))
plt.pie(topLangCount, labels=topLang, colors=colorArray, autopct='%1.1f%%', shadow=True, startangle=90)
plt.legend(topLang, loc='best', fontsize=12)
#           )
plt.axis('equal')
plt.show()

