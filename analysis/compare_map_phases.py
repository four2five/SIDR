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


def presentData(ioTime, mapTime, registerOutputTime, commitTime, taskTime, ax, datasetNum, plotName):

  runningTotal = 0
  for time in ioTime:
    runningTotal = runningTotal + time

  ioTime.sort()
  #ax.hist(np.array(ioTime), bins=40, label="IO Time", alpha=0.10, hatch=hatchStyles[0])
  fit = stats.norm.pdf(ioTime, np.mean(ioTime), np.std(ioTime))
  #ax.plot(ioTime, fit, linestyle=lineStyles[datasetNum], label="IO Time " + plotName)
  print "ioTime min: " + str(ioTime[0]) + " max: " + str(ioTime[-1])
  print "Total ioTime: " + str(runningTotal) + " avg: " + str(runningTotal / len(ioTime))
  print "ioTime avg: " + str(np.mean(ioTime)) + " stddev: " + str(np.std(ioTime))

  runningTotal = 0
  for time in mapTime:
    runningTotal = runningTotal + time

  #ax.hist(np.array(mapTime), bins=40, label="Map Time", alpha=0.2, hatch=hatchStyles[1])
  mapTime.sort()
  fit = stats.norm.pdf(mapTime, np.mean(mapTime), np.std(mapTime))
  #ax.plot(mapTime, fit, linestyle=lineStyles[datasetNum], label="Map Time " + plotName)
  print "mapTime min: " + str(mapTime[0]) + " max: " + str(mapTime[-1])
  print "Total mapTime: " + str(runningTotal) + " avg: " + str(runningTotal / len(mapTime))
  print "mapTime avg: " + str(np.mean(mapTime)) + " stddev: " + str(np.std(mapTime))

  runningTotal = 0
  for time in registerOutputTime:
    runningTotal = runningTotal + time

  #ax.hist(np.array(registerOutputTime), bins=40, label="Register Output Time", alpha=0.3, hatch=hatchStyles[2])
  registerOutputTime.sort()
  fit = stats.norm.pdf(registerOutputTime, np.mean(registerOutputTime), np.std(registerOutputTime))
  #ax.plot(registerOutputTime, fit, linestyle=lineStyles[2], label="Register Output Time")
  print "registerOutputTime min: " + str(registerOutputTime[0]) + " max: " + str(registerOutputTime[-1])
  print "Total registerOutputTime: " + str(runningTotal) + " avg: " + str(runningTotal / len(registerOutputTime))
  print "registerOutputTime avg: " + str(np.mean(registerOutputTime)) + " stddev: " + str(np.std(registerOutputTime))

  runningTotal = 0
  for time in commitTime:
    runningTotal = runningTotal + time

  #ax.hist(np.array(commitTime), bins=40, label="Commit Time", alpha=0.4, hatch=hatchStyles[3])
  commitTime.sort()
  fit = stats.norm.pdf(commitTime, np.mean(commitTime), np.std(commitTime))
  #ax.plot(commitTime, fit, linestyle=lineStyles[datasetNum], label="Commit Time " + plotName)
  print "commitTime min: " + str(commitTime[0]) + " max: " + str(commitTime[-1])
  print "Total commitTime: " + str(runningTotal) + " avg: " + str(runningTotal / len(commitTime))
  print "commitTime avg: " + str(np.mean(commitTime)) + " stddev: " + str(np.std(commitTime))

  runningTotal = 0
  for time in taskTime:
    runningTotal = runningTotal + time

  #ax.hist(np.array(commitTime), bins=40, label="Commit Time", alpha=0.4, hatch=hatchStyles[3])
  taskTime.sort()
  fit = stats.norm.pdf(taskTime, np.mean(taskTime), np.std(taskTime))
  ax.plot(taskTime, fit, linestyle=lineStyles[datasetNum], label="Task Time " + plotName)
  print "taskTime min: " + str(taskTime[0]) + " max: " + str(taskTime[-1])
  print "Total taskTime: " + str(runningTotal) + " avg: " + str(runningTotal / len(taskTime))
  print "taskTime avg: " + str(np.mean(taskTime)) + " stddev: " + str(np.std(taskTime))

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

      # else, parse the raw log files
      else:
        print "This is wrong, you should be parsing a formatted output file"
        sys.exit(2)

      presentData(ioTime, mapTime, registerOutputTime, commitTime, taskTime, ax, datasetNum, plotName)  
      print "adding graph for ", plotName
      datasetNum = datasetNum + 1

    ax.set_xlabel("Shuffle Time (milliseconds)")
    ax.set_ylabel("Fraction of Map Tasks")
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
    ax.set_xlim([0, 40000])
    ax.set_title("Map Phase Times InMemory 168 Reducers")
    #print "calling show()"
    #plt.show()
    print "calling savefig"
    plt.savefig("foo.pdf")

if __name__ == "__main__":
  main()
