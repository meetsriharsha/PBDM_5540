import matplotlib
import numpy as np
import matplotlib.pyplot as plt
import re
from matplotlib import cm
import matplotlib.colors as clrs
from mpl_toolkits.mplot3d import Axes3D
from operator import itemgetter

hr = []
lang = []
count = []
langCountList = []
langCountSorted = []
topLangCount = []
topLang = []
tot = []
f = open('K:/UMKC/Docs/Subjects/PBDM/PB_Project/TweetsPerTimeLang/part-00000', 'rU')   #open the file in read universal mode
for line in f:
    cells = (re.sub('[\(\)\"\\n]', '', line)).split(",")
    #cells = sorted(cells, key=itemgetter(2))
    #langCountList = langCountList.insert((re.sub('[\(\)\"]', '', line)).split(","))
    #langCount = (re.sub('[\(\)\"]', '', line))
    #langCount.append((re.sub('[\(\)\"]', '', line)))
    #langCountList = langCountList.append((langCount.split(',')))

    hr.append(cells[0])
    lang.append(cells[1])
    count.append(cells[2])
#    tot.append(date[11:13]+","+cells[1]+","+cells[2])
f.close()
#print tot.sort(key=lambda x: x[2], reverse=True)
#print hr
#print lang
#print count
file = open('K:/UMKC/Docs/Subjects/PBDM/PB_Project/TweetsPerTimeLang/part-00000', 'r')
lines = file.readlines()
file.close()
timeLangCountList = [(re.sub('[\(\)\"\\n]', '', line)).split(',') for line in lines]
timeLangCountListSorted = sorted(timeLangCountList, key=lambda x: float(x[2]), reverse=True)
timeLangCountListSortedNone = [ x for x in timeLangCountListSorted if x is not None ]
#print timeLangCountListSortedNone
for i in range(0, len(timeLangCountListSortedNone)):
   try:
       timeLangCountListSortedNone[0] = timeLangCountListSortedNone[0].append(timeLangCountListSortedNone[1:2])
   except:
       timeLangCountListSortedNone[0] = []
       timeLangCountListSortedNone[0] = timeLangCountListSortedNone[0].append(timeLangCountListSortedNone[1:2])
#for hr in range(0, len(timeLangCountListSorted)):
 #  if timeLangCountListSorted[hr]:
  #     if timeLangCountListSorted[hr][0] == '23':
   #     print timeLangCountListSorted[hr]
#print timeLangCountListSortedNone[1]
hrSet = set(hr)
hrList = list(hrSet)
langSet = set(lang)
langList = list(langSet)
print hrList
print langList
#print ind
colors = ["blue", "red", "brown", "green", "purple", "black"]
idx = 0
print len(hr)
print len(count)
print len(lang)
fig = plt.figure()
for j in range(0, len(hrList)-1):
    langListTemp = []
    countListTemp = []
    for k in range(0, len(langList)-1):
        subCount = [x[2] for x in [ x for x in timeLangCountListSorted if x is not None ] if x[0] == hrList[j] and x[1] == langList[k]]
        #subCount = [0 if v is None else v for v in subCount]
        langListTemp.append(langList[k])
        if not subCount:
            #print hrList[j],langList[k],"0"
            countListTemp.append(0)
        else:
            #print hrList[j],langList[k],subCount[0]
            countListTemp.append(subCount[0])
    print langListTemp+countListTemp
    barWidth = 1
    ind=np.arange(len(countListTemp))
    plt.bar(ind+barWidth, countListTemp, barWidth, color=colors[idx], label=hrList[j])
    #width += 0.15
    ind += barWidth
    idx += 1
plt.ylabel('AVERAGE SCORE')
plt.xlabel('TONES')
plt.title('Breakdown of tweets count by time and language')
plt.xticks(ind+barWidth, langList)
plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc='center',ncol=5, mode="expand", borderaxespad=0.)
plt.show()
fig.savefig('result.png')
#print timeLangCountListSorted[:40]
#for list in timeLangCountListSorted[:50]:
    #cells = list.split(",")
 #   topLang.append(list[0])
  #  topLangCount.append(list[1])
#langCountSorted = sorted(langCountList, key=itemgetter(1))
#print "\n".join(langCountSorted)
#print topLang
#print topLangCount

#ind = np.arange(len(lang))
#width = 0.35
#sentimentDistribution = [1, 2, 3, 4]
#bar = plt.bar(ind, count, width, color='g', label="distributions")

#params = plt.gcf()
#plSize = params.get_size_inches()
#params.set_size_inches((plSize[0]*2.5, plSize[1]*2))
#plt.ylabel('Tweet count')
#plt.xlabel('Tone')
#plt.title('Distribution of tweets by Language')
#plt.xticks(ind+width, lang)
#plt.legend()

#plt.show()

#color_vals = [-1, 0, 1]
#colorArray = ['yellowgreen', 'gold', 'purple', 'skyblue', "violet", "green", "pink", "lightyellow", "red","brown"]
#my_norm = clrs.Normalize(-1, 1)    # maps your data to the range [0, 1]
#my_cmap = matplotlib.cm.get_cmap('jet')    # can pick your color map
#f = plt.figure()
#params = plt.gcf()
#plSize = params.get_size_inches()
#params.set_size_inches((plSize[0]*5, plSize[1]*5))
#plt.pie(topLangCount, labels=topLang, colors=colorArray, autopct='%1.1f%%', shadow=True, startangle=90)
#plt.legend(count, lang, loc='left center', bbox_to_anchor=(-0.1, 1.),
#           fontsize=8)
#plt.axis('equal')
#plt.show()
