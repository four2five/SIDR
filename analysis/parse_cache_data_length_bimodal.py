#!/usr/bin/python
import getopt, sys, re, time, datetime, itertools, os
import numpy as np
import matplotlib
from datetime import datetime
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import statsmodels.tools as sm
import pylab


# global vars
myFigSize = (9.5,6)  # seeks to scale well for different paper formats
figureLabelList = []
figureLabelList.append("node 1")
figureLabelList.append("node 2")

lineColorList = []
lineColorList.append("r")
lineColorList.append("b")

lineColorList2 = []
lineColorList2.append("m")
lineColorList2.append("g")

lineStyleList = []
lineStyleList.append("--")
lineStyleList.append(":")
lineStyleList.append("-")

markerStyleList = []
markerStyleList.append("2")
markerStyleList.append(",")
markerStyleList.append("1")

perExpLabelList = []
perExpLabelList.append("rMB/s")
perExpLabelList.append("wMB/s")
perExpLabelList.append("util")

expName = "SIDR 22 Reducers"

myBins = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40]

##################
# 
# This script parses the output from loggedfs
# and bins all reads and writes into per-second
# buckets. It then calculates the number of 
# seeks per second. 
##################

def usage():
  print "./parse_memory_preassure.py -d <directory to parse>"


def parseFile(taskTrackerLogFile):
  print "parsing file ", taskTrackerLogFile 

  # first, parse out the "Adding buffer" lines
  #searchString = ".*Shuffle time (\d+) .* (\d+) bytes.*"
  #searchString1 = "(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d\d\d).*Adding buffer.* (attempt_\d+_\d+_m_\d+_\d).*capacity: (\d+).*true deps \[(.*)\].*"
  #searchString1 = "(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d\d\d).*Adding buffer.* (attempt_\d+_\d+_m_\d+_\d).*true deps [(.*)].*capacity: (\d+).*"
  searchString1 = "(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d\d\d).*Adding buffer.* (attempt_\d+_\d+_m_\d+_\d).*true deps \[(.*)\].*capacity: (\d+).*"
  searchString2 = "(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d\d\d).*Garbage collecting.* (attempt_\d+_\d+_m_\d+_\d).*capacity: (\d+).*"
  searchString0 = "(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d\d\d).*"

  dateStringFormat =  '%Y-%m-%d %H:%M:%S,%f'

  ins = open(taskTrackerLogFile, "r")

  # we'll use a dict, key'd by buffer id, and record the add and delete time
  oneReducerDepBufferIDDict = dict()
  multiReducerDepBufferIDDict = dict()
  
  # for sanity checking
  linesParsed = 0
  validMallocLines = 0
  validFreeLines = 0

  for line in ins:

    # first, see if the lines references memory changes
    matchObj1 = re.match(searchString1, line.lstrip())
    matchObj2 = re.match(searchString2, line.lstrip())

    # first, sanity check
    if matchObj1 and matchObj2:
      print "ERROR: both regexs matched. Not good." 
    # see if either matched 
      # is it a malloc?
    elif matchObj1:
      timeString = matchObj1.group(1)
      myDateTime = datetime.strptime(timeString, dateStringFormat)
      myTime =  long(time.mktime(myDateTime.timetuple()))

      #byBufferIDDict = dict()
  
      trueDeps = matchObj1.group(3)
      depTasks = trueDeps.rsplit(",")
      #print "true deps: " + trueDeps

      attempt = matchObj1.group(2)
      validMallocLines = validMallocLines + 1

      #print attempt, " $ ", myTime

      if attempt in oneReducerDepBufferIDDict or attempt in multiReducerDepBufferIDDict:
        print "ERROR: this attempt was already"
      else:
        if len(depTasks) == 1:
          oneReducerDepBufferIDDict[attempt] = []
          oneReducerDepBufferIDDict[attempt].append(myTime)
        else:
          multiReducerDepBufferIDDict[attempt] = []
          multiReducerDepBufferIDDict[attempt].append(myTime)

    # or a free?
    elif matchObj2:
      timeString = matchObj2.group(1)
      myDateTime = datetime.strptime(timeString, dateStringFormat)
      myTime =  long(time.mktime(myDateTime.timetuple()))

      attempt = matchObj2.group(2)
      validFreeLines = validFreeLines + 1

      #print attempt, " $ ", myTime

      if attempt in oneReducerDepBufferIDDict:
        oneReducerDepBufferIDDict[attempt].append(myTime)
      elif attempt in multiReducerDepBufferIDDict:
        multiReducerDepBufferIDDict[attempt].append(myTime)
      else:
        print "ERROR: found a free before we found a malloc"

  oneReducerDepCacheDurationArray = []
  multiReducerDepCacheDurationArray = []

  # now, roll through the dict and calculate how long each
  # task was in the cache
  for attempt in oneReducerDepBufferIDDict.keys():
    data = oneReducerDepBufferIDDict[attempt]
    if len(data) <2:
      print "ERROR: found an attempt with a start and no end"
    else:
      startTime = long(data[0])
      endTime = long(data[1])
      deltaTime = endTime - startTime
      oneReducerDepCacheDurationArray.append(deltaTime)
      print "delta: ", deltaTime

  # now, roll through the dict and calculate how long each
  # task was in the cache
  for attempt in multiReducerDepBufferIDDict.keys():
    data = multiReducerDepBufferIDDict[attempt]
    if len(data) <2:
      print "ERROR: found an attempt with a start and no end"
    else:
      startTime = long(data[0])
      endTime = long(data[1])
      deltaTime = endTime - startTime
      multiReducerDepCacheDurationArray.append(deltaTime)
      print "delta: ", deltaTime

  print "mallocs: ", validMallocLines, " frees: ", validFreeLines
  if validMallocLines != validFreeLines:
    print "ERROR, mallocs: ", validMallocLines, " != frees ", validFreeLines
  print "len(oneReducerDepBufferIDDict): ", len(oneReducerDepBufferIDDict)
  print "len(multiReducerDepBufferIDDict): ", len(multiReducerDepBufferIDDict)
  return (oneReducerDepCacheDurationArray,multiReducerDepCacheDurationArray)
  
def dumpData(parsedOutputFilename, arrayOfArraysOfArrays):
  times = []
  sizes = []
  # iterate through all of the variables 
  for perHostData in arrayOfArraysOfArrays:
    for dataPoint in perHostData[0]:
      #times.append(int(dataPoint / 1000000))  # convert nanoseconds into milliseconds
      times.append(int(dataPoint))  # convert nanoseconds into milliseconds

    for dataPoint in perHostData[1]:
      sizes.append(int(dataPoint)) # turn it into KB

  outputStream = open(parsedOutputFilename, "w")
  outputStream.write(expName + "\n");
  for x in xrange(len(times)):
    outputStream.write(str(str(times[x]) +  "$" +  str(sizes[x]) + "\n"));

  outputStream.close()

def presentData(arrayOfArrays, ax1, plotLabels1, lns1):

  times = []
  maxTime = 0
  totalTime = long(0)
  # iterate through each hosts dataset
  for perHostTimes in arrayOfArrays:
    for singleTime in perHostTimes:
      times.append(singleTime)
      #print time, " $ ", size
      totalTime = totalTime + singleTime
      if singleTime > maxTime:
        maxTime = singleTime


  print "MaxTime: ", maxTime
  print "totalTime: ", long(totalTime)

  sortedTimes = sorted(times)

  #ax1.plot(sortedTimes)
  #ax1.hist(sortedTimes, bins=40)
  ax1.hist(sortedTimes, bins=myBins)

  ax1.grid()

def main():
  try:
    opts, args = getopt.getopt(sys.argv[1:], "hp:fd:")
  except getopt.GetoptError, err:
    print str(err)
    usage()
    sys.exit(2)

  # initialize this to something sensible
  parsedOutputFilename = None
  formattedInputFile = False
  dirToRead = ""

  for o, a in opts:
    if o in ("-h", "--help"):
      usage()
      sys.exit()
    elif o in ("-p", "--parse"):
      parsedOutputFilename = a
    elif o in ("-d", "--directory"):
      dirToRead = a
    elif o in ("-f", "--formatted"):
      formattedInputFile = True
    else:
      assert False, "unhandled option"

  if False:  # dead code, remove later
    print "error: there should be at least one arg and it must include the directory of files to parse"
    sys.exit(2)
  else:
    if not os.path.isdir(dirToRead):
      print "error: specified path \"", dirToRead, "\" is not a directory. Try again"
      sys.exit(2)

    # implicit else
    filesToParse = os.listdir(dirToRead)

    # create the larger plot
    fig = plt.figure(figsize=myFigSize)
    ax = fig.add_subplot(111)
    #ax2 = ax.twinx()
    plotLabels1 = []
    plotLabels2 = []
    lns1 = []
    lns2 = []
    datasetNum = 0
    array = []
    arrayOfArrays1 = []
    arrayOfArrays2 = []

    for filename in filesToParse:
      outArray1, outArray2 = parseFile(dirToRead + "/" + filename)

      arrayOfArrays1.append(outArray1)
      arrayOfArrays2.append(outArray2)

      if parsedOutputFilename is not None:
        dumpData(parsedOutputFilename, arrayOfArraysOfArrays)
      else:
        # now present it in some meaningful manner
        #presentData(arrayOfArraysOfAverages, ax, plotLabels1, lns1)
        presentData(arrayOfArrays1, ax, plotLabels1, lns1)
        presentData(arrayOfArrays2, ax, plotLabels1, lns1)

        datasetNum = datasetNum + 1

        ax.set_xlabel("Time Spent in In-Memory Cache (seconds)")
        ax.set_ylabel("Count of Map Outputs in Bin")
    

        plt.tight_layout()
        ax.set_title("Distribution of Intermediate Output Cache Residency Durations")
        #plt.show()
        print "calling savefig"
        plt.savefig("foo.pdf")

if __name__ == "__main__":
  main()
