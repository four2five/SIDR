#!/usr/bin/python
import getopt, sys, re, time, datetime, itertools
import numpy as np
import matplotlib
import scipy.stats as stats
matplotlib.use('Agg')
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
lineColorList.append("r")
lineColorList.append("b")
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
  x = ['Input IO', 'Apply Map()', 'Register Output', 'Commit Output']

  ioTimeMean = []
  ioTimeStdDev = []
  for entry in totalIoTimes:
    ioTimeMean.append(np.mean(entry))
    ioTimeStdDev.append(np.std(entry))

  #p1 = plt.bar(ind, ioTimeMean, width, color='red', error_kw=dict(ecolor='white', marker='o'), yerr=ioTimeStdDev, label="IO Time")

  mapTimeMean = []
  mapTimeStdDev = []
  for entry in totalMapTimes:
    mapTimeMean.append(np.mean(entry))
    mapTimeStdDev.append(np.std(entry))

  #p2 = plt.bar(ind, mapTimeMean, width, color='blue', ecolor='yellow', bottom=ioTimeMean, yerr=mapTimeStdDev, label="Map Time")

  registerOutputTimeMean = []
  registerOutputTimeStdDev = []
  for entry in totalRegisterOutputTimes:
    registerOutputTimeMean.append(np.mean(entry))
    registerOutputTimeStdDev.append(np.std(entry))

  # calculate a new base that is ioTimeMean[x] + mapTimeMean[x]
  # NOTE: assumes that len(ioTimeMean) equals len(mapTimeMean), which is always *should*
  #print "len(ioTimeMean): " + str(len(ioTimeMean)) + " len(mapTimeMean): " + str(len(mapTimeMean))
  newBaseMean = []
  for x in range(len(ioTimeMean)):
    #print "x: " + str(x)
    newBaseMean.append(ioTimeMean[x] + mapTimeMean[x])

  #p3 = plt.bar(ind, registerOutputTimeMean, width, color='yellow', bottom=newBaseMean, yerr=registerOutputTimeStdDev, label="Register Output Time")

  commitTimeMean = []
  commitTimeStdDev = []
  for entry in totalCommitTimes:
    commitTimeMean.append(np.mean(entry))
    commitTimeStdDev.append(np.std(entry))

  # update newBaseMean to include registerOutputTimeMean
  for x in range(len(ioTimeMean)):
    #print "x: " + str(x)
    newBaseMean[x] = newBaseMean[x] + registerOutputTimeMean[x]

  #p4 = plt.bar(ind, commitTimeMean, width, color='green', bottom=newBaseMean, yerr=commitTimeStdDev, label="Commit Output Time")

  perApproachValues = []
  perApproachStdDevs = []
  for x in range(len(ioTimeMean)):
    mapTime = mapTimeMean[x]
    mapStdDev = mapTimeStdDev[x]
    ioTime = ioTimeMean[x]
    ioStdDev = ioTimeStdDev[x]
    registerOutputTime = registerOutputTimeMean[x]
    registerOutputStdDev = registerOutputTimeStdDev[x]
    commitTime = commitTimeMean[x]
    commitStdDev = commitTimeStdDev[x]
    perApproachValues.append([ioTime, mapTime, registerOutputTime, commitTime])
    perApproachStdDevs.append([ioStdDev, mapStdDev, registerOutputStdDev, commitStdDev])

  print "len(perApproachValues): " + str(len(perApproachValues))
  print "perApproachValues[0]: " + str(perApproachValues[0])
  ax.bar(ind - width, perApproachValues[0], yerr=perApproachStdDevs[0], width=width, color='r', align='center', label="QA Off")
  #ax.bar(fakeX, fakeY2, width=0.2, color='r', align='center', label="QA Off")
  ax.bar(ind, perApproachValues[1],  yerr=perApproachStdDevs[0], width=width, color='b', align='center', label="QA On")


  plt.xticks(ind - width/2., ('Input IO', 'Apply Map()', 'Register Output', 'Commit'))

  ax.grid()

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
    #ax.set_ylim(top=70)

    #ax2.set_ylim(top=105)
    #ax2.set_ylabel("Utilization %")

    #newLns1 = itertools.chain.from_iterable(lns1)
    #ax.legend(newLns1, plotLabels1, loc='upper left')
    #ax.legend(plotLabels1, loc='upper right')
    ax.legend(loc='upper right')

    #newLns2 = itertools.chain.from_iterable(lns2)
    #ax2.legend(newLns2, plotLabels2, loc='center left')

    plt.tight_layout()

    #locator = FixedLocator(myTimesTicksBins)
    #ax.xaxis.set_major_locator(locator)
    #ax.locator_params(nbins=5)
    #plt.yscale('log', nonposy='clip')
    #plt.xscale('log', nonposy='clip')
    #ax.set_yscale('log')
    #ax.set_xlim([0, 40000])
    ax.set_ylim([-6000, 16000])
    ax.set_title("Map Task Per-Phase Breakdown (InMemory 168 Reducers)")
    #print "calling show()"
    #plt.show()
    print "calling savefig"
    plt.savefig("foo.pdf")

if __name__ == "__main__":
  main()
