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

    bp = plt.boxplot(totalIoTimes[counter],positions=[(3+(counter * gap))])
    setColors(bp, lineColorList[counter])
    showMedianValues(bp, ax, counter)

    bp = plt.boxplot(totalMapTimes[counter], positions=[(5+(counter*gap))])
    setColors(bp, lineColorList[counter])
    showMedianValues(bp, ax, counter)

    bp = plt.boxplot(totalRegisterOutputTimes[counter], positions=[(7 + (counter*gap))])
    setColors(bp, lineColorList[counter])
    showMedianValues(bp, ax, counter)

    bp = plt.boxplot(totalCommitTimes[counter], positions=[(9+(counter*gap))])
    setColors(bp, lineColorList[counter])
    showMedianValues(bp, ax, counter)

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
    ax.set_xticks([1.175, 3.15, 5.15, 7.15, 9.15])
    ax.set_xlim(0,10)

    ax.set_ylim([0, 45000])
    ax.set_title("Map Task Per-Phase Breakdown (InMemory 168 Reducers)")
    #print "calling show()"
    #plt.show()
    print "calling savefig"
    plt.savefig("foo.pdf")

if __name__ == "__main__":
  main()
