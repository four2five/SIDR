#!/usr/bin/python
import getopt, sys, re, time, datetime, itertools
import numpy as np
import matplotlib
import scipy.stats as stats
matplotlib.use('Agg')
#from pylab import boxplot
import matplotlib.pyplot as plt
from matplotlib.ticker import FixedLocator
import statsmodels.tools as sm
import math


# global vars
myFigSize = (9.5,6)  # seeks to scale well for different paper formats
#figureLabelList = []
#figureLabelList.append("node 1")
#figureLabelList.append("node 2")

lineColorList = []
lineColorList.append("b")
lineColorList.append("g")
#myTimesBins = [0, 100, 250, 500, 1000, 1500, 2000, 3000, 4000, 5000, 10000]
#myTimesTicksBins = [100, 500, 1000, 2000, 3000, 4000, 5000, 10000]
hatchStyles = ["/", "\\", "+", "-"]
lineStyles = ['-', '--', ':', '-.']

##################
# 
# This script parses the output from loggedfs
# and bins all reads and writes into per-second
# buckets. It then calculates the number of 
# seeks per second. 
##################

def usage():
  print "./create_shuffle_time_graph.py <file to parse>[,<another file to parse>]"


def presentData(totalIoTimes, totalMapTimes, totalRegisterOutputTimes, totalCommitTimes, totalTaskTimes, ax, datasetNum, plotName):

  #N = len(4)
  #ind = np.arange(N)
  ind = np.arange(4)
  width = 0.35
  gap = 0.3
  x = ['Input IO', 'Apply Map()', 'Register Output', 'Commit Output']

  counter = 0
  for x in range(len(totalIoTimes)):
    #ax.bar(ind - width, perApproachValues[0], yerr=perApproachStdDevs[0], \ 
          #width=width, color='r', align='center', label="QA Off")
    #ax.bar(ind, perApproachValues[1],  yerr=perApproachStdDevs[0], width=width, color='b', align='center', label="QA On")

    bp = plt.boxplot(totalTaskTimes[counter],positions=[(1+(counter * gap))])
    setColors(bp, lineColorList[counter])
    showMedianValues(bp, ax, counter)
    setCaps(bp, ax, counter, totalTaskTimes[counter])
    #printCaps(bp, ax, counter, totalTaskTimes[counter])

    bp = plt.boxplot(totalIoTimes[counter],positions=[(3.5+(counter * gap))])
    setColors(bp, lineColorList[counter])
    showMedianValues(bp, ax, counter)
    setCaps(bp, ax, counter, totalIoTimes[counter])

    bp = plt.boxplot(totalMapTimes[counter], positions=[(5.5+(counter*gap))])
    setColors(bp, lineColorList[counter])
    showMedianValues(bp, ax, counter)
    setCaps(bp, ax, counter, totalMapTimes[counter])

    bp = plt.boxplot(totalRegisterOutputTimes[counter], positions=[(7.5 + (counter*gap))])
    setColors(bp, lineColorList[counter])
    showMedianValues(bp, ax, counter)
    setCaps(bp, ax, counter, totalRegisterOutputTimes[counter])
    printCaps(bp, ax, counter, totalRegisterOutputTimes[counter])

    bp = plt.boxplot(totalCommitTimes[counter], positions=[(9.5+(counter*gap))])
    setColors(bp, lineColorList[counter])
    showMedianValues(bp, ax, counter)
    setCaps(bp, ax, counter, totalCommitTimes[counter])

    counter = counter + 1

  #plt.xticks(ind - width/2., ('Total Time', 'Input IO', 'Apply Map()', 'Register Output', 'Commit'))

  #ax.grid()

def setColors(bp, myColor):
  plt.setp(bp['boxes'], color=myColor)
  plt.setp(bp['whiskers'], color=myColor)
  #plt.setp(bp['fliers'], color=myColor, marker='+')
  plt.setp(bp['fliers'], color=myColor, marker='')
  plt.setp(bp['medians'], color='red')

def showMedianValues(bp, ax, datasetNum):
  for line in  bp['medians']:
    # get position data for median line
    x, y = line.get_xydata()[1] # top of median line
    # overlay median value

    if y > 9999:
      ax.text((x-0.6) + (datasetNum * 1), y, '%d' % y,
          horizontalalignment='center') # draw above, centered
    else:
      ax.text( (x-0.5) + (datasetNum * 0.85), y, '%d' % y,
          horizontalalignment='center') # draw above, centered

def printCaps(bp, ax, datasetNum, data):
  print "in printCaps"
  print "whiskers"
  for line in  bp['whiskers']:
    print str(datasetNum) + " whiskers: " + str(line.get_ydata())

  print "caps"
  for line in  bp['caps']:
    print str(datasetNum) + " caps: " + str(line.get_ydata())

def setCaps(bp, ax, datasetNum, data):
  # whiskers always come out as low, then high. Use counter to set the correct one
  funcCounter = 0

  myWhiskers = bp['whiskers']
  myCaps = bp['caps']

  for line in  myWhiskers:
    print str(datasetNum) + " whiskers start as: " + str(line.get_ydata())

  for line in myCaps:
    # get position data for median line
    #x, y = line.get_xydata()[1] # top of median line
    x, y = line.get_xydata() 
    # overlay median value
    print str(datasetNum) + " caps start as x, y: ", str(x), ":", str(y) 

    # calculate the 95th percentile
    my95th = np.percentile(data, 95)
    print "95th percentile: " + str(my95th)

    my75th = np.percentile(data, 75)
    print "75th percentile: " + str(my75th)

    my25th = np.percentile(data, 25)
    print "25th percentile: " + str(my25th)


    # calculate the 5th percentile
    my5th = np.percentile(data, 5)
    print "5th percentile: " + str(my5th)

    if funcCounter == 1:
      print "setting 5th percentile"
      line.set_ydata(np.array([my5th, my5th]))

      # set the whishkers for the lower cap
      print "set " + str(myWhiskers[0].get_ydata()) + " to " + str(my5th)
      print "set " + str(myWhiskers[0].get_ydata()[1]) + " to " + str(my5th)
      tempYData = myWhiskers[0].get_ydata()
      tempYData[1] = my5th
      myWhiskers[0].set_ydata(tempYData)

    elif funcCounter == 0:
      print "setting 95th percentile"
      line.set_ydata(np.array([my95th, my95th]))

      # set the whishkers for the upper cap
      print "set " + str(myWhiskers[1].get_ydata()) + " to " + str(my95th)
      print "set " + str(myWhiskers[1].get_ydata()[1]) + " to " + str(my95th)
      tempYData = myWhiskers[1].get_ydata()
      tempYData[1] = my95th
      myWhiskers[1].set_ydata(tempYData)

    else:
      print "ERROR, funcCounter is too large in setCaps"

    funcCounter = funcCounter + 1

  print "caps"
  for line in bp['caps']:
    x, y = line.get_xydata() 
    print str(datasetNum) + " x, y: ", str(x), ":", str(y) 

  print "whiskers"
  for line in bp['whiskers']:
    x, y = line.get_xydata() 
    print str(datasetNum) + " x, y: ", str(x), ":", str(y) 

def main():
  try:
    opts, args = getopt.getopt(sys.argv[1:], "hp:f")
  except getopt.GetoptError, err:
    print str(err)
    usage()
    sys.exit(2)

  # initialize this to something sensible
  parsedOutputFilename = None
  formattedInputFile = False

  for o, a in opts:
    if o in ("-h", "--help"):
      usage()
      sys.exit()
    elif o in ("-p", "--parse"):
      parsedOutputFilename = a
    elif o in ("-f", "--formatted"):
      formattedInputFile = True
    else:
      assert False, "unhandled option"

  if len(args) != 1:
    print "error: there should be one arg and it should be the file to parse"
    sys.exit(2)
  else:
    print "arg: ", args[0]
    filesToParse = args[0].split(",")

    # create the larger plot
    fig = plt.figure(figsize=myFigSize)
    ax = fig.add_subplot(111)
    #ax2 = ax.twinx()
    datasetNum = 0

    totalIoTimes = []
    totalMapTimes = []
    totalRegisterOutputTimes = []
    totalCommitTimes = []
    totalTaskTimes = []

    for filename in filesToParse:
      ioTime = []
      mapTime = []
      registerOutputTime = []
      commitTime = []
      taskTime = []

      # the input was already formatted, just read it in
      if formattedInputFile:
        ins = open(filename, "r")
        plotName = None
        columnHeaderLine = None
  
        for line in ins:
          # first line is the title of the graph
          if plotName is None:
            plotName = line
          elif columnHeaderLine is None:
            columnHeaderLine = line
          else: 
            (d1, d2, d3, d4, d5) = line.rstrip().split("$")
            ioTime.append(long(d1))
            mapTime.append(long(d2))
            registerOutputTime.append(long(d3))
            commitTime.append(long(d4))
            taskTime.append(long(d5))

        ins.close()
        totalIoTimes.append(ioTime)
        totalMapTimes.append(mapTime)
        totalRegisterOutputTimes.append(registerOutputTime)
        totalCommitTimes.append(commitTime)
        totalTaskTimes.append(taskTime)

      # else, parse the raw log files
      else:
        print "This is wrong, you should be parsing a formatted output file"
        sys.exit(2)

    presentData(totalIoTimes, totalMapTimes, totalRegisterOutputTimes, totalCommitTimes, totalTaskTimes, ax, datasetNum, plotName)  
    print "adding graph for ", plotName
    datasetNum = datasetNum + 1

    #ax.set_xlabel("Shuffle Time (milliseconds)")
    ax.set_ylabel("Time (ms)")

    #ax.legend(loc='upper right')

    plt.tight_layout()
    ax.set_xticklabels(['Total Time', 'Input IO', 'Apply Map()', 'Register Output', 'Commit'])
    ax.set_xticks([1.175, 3.65, 5.65, 7.65, 9.65])
    ax.set_xlim(0,12)

    ax.set_ylim([0, 50000])
    ax.set_title("Map Task Per-Phase Breakdown (InMemory 168 Reducers)")
    #print "calling show()"
    #plt.show()
    print "calling savefig"
    plt.savefig("foo.pdf")

if __name__ == "__main__":
  main()
