#!/usr/bin/python
import getopt, sys, re, time, datetime, itertools
import numpy as np
import matplotlib
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
#hatchStyles = ["", "/", "\\"]

##################
# 
# This script parses the output from loggedfs
# and bins all reads and writes into per-second
# buckets. It then calculates the number of 
# seeks per second. 
##################

def usage():
  print "./create_shuffle_time_graph.py <file to parse>[,<another file to parse>]"


def presentData(ioTime, mapTime, registerOutputTime, commitTime, ax, datasetNum):

  runningTotal = 0
  for time in ioTime:
    runningTotal = runningTotal + time
    #ax.hist(np.array(sizesDict[key]), myTimesBins, alpha=0.15 * (counter+1), label=str(int(key)) + " MB", \
    #                 hatch=hatchStyles[counter])
    #ax.hist(np.array(sizesDict[key]), 40, alpha=0.2 * counter, label=str(int(key)) + " MB")
  ax.hist(np.array(ioTime), bins=40)
  ax.plot(runningTotal, label="foo")
  print "Total ioTime: " + str(runningTotal) + " avg: " + str(runningTotal / len(ioTime))

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
            (d1, d2, d3, d4) = line.rstrip().split("$")
            ioTime.append(long(d1))
            mapTime.append(long(d2))
            registerOutputTime.append(long(d3))
            commitTime.append(long(d4))

        ins.close()

      # else, parse the raw log files
      else:
        print "This is wrong, you should be parsing a formatted output file"
        sys.exit(2)

      presentData(ioTime, mapTime, registerOutputTime, commitTime, ax, datasetNum)  
      print "adding graph for ", plotName
      datasetNum = datasetNum + 1

    ax.set_xlabel("Shuffle Time (milliseconds)")
    ax.set_ylabel("Reduce Tasks per Bin")
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
    ax.set_title("Per Reduce Task Shuffle Times InMemory 168 Reducers QueryAware")
    #print "calling show()"
    #plt.show()
    print "calling savefig"
    plt.savefig("foo.pdf")

if __name__ == "__main__":
  main()
