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
  searchString1 = "(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d\d\d).*Adding buffer.* (attempt_\d+_\d+_m_\d+_\d).*capacity: (\d+).*"
  searchString2 = "(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d\d\d).*Garbage collecting.* (attempt_\d+_\d+_m_\d+_\d).*capacity: (\d+).*"
  searchString0 = "(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d\d\d).*"

  dateStringFormat =  '%Y-%m-%d %H:%M:%S,%f'

  ins = open(taskTrackerLogFile, "r")
  
  # for sanity checking
  linesParsed = 0
  validMallocLines = 0
  validFreeLines = 0
  currentlyUsedMemory = long(0)
  firstTimeStamp = 0

  # We'll log times and the current amount of allocatted memory
  memoryChangeEvents = []

  # this line basicaly says we were using zero MB of RAM at the start of the experiment
  memoryChangeEvents.append((long(0),0))

  for line in ins:
    if linesParsed == 0:
      # this *should* always match as every line has a timestamp
      matchObj0 = re.match(searchString0, line.lstrip())
      timeString = matchObj0.group(1)
      myDateTime = datetime.strptime(timeString, dateStringFormat)
      #print "JB3, ", time.mktime(myDateTime.timetuple())
      firstTime = time.mktime(myDateTime.timetuple())

    linesParsed = linesParsed + 1

    # first, see if the lines references memory changes
    matchObj1 = re.match(searchString1, line.lstrip())
    matchObj2 = re.match(searchString2, line.lstrip())

    # first, sanity check
    if matchObj1 and matchObj2:
      print "ERROR: both regexs matched. Not good." 
    # see if either matched 
      # is a malloc?
    elif matchObj1:
      timeString = matchObj1.group(1)
      myDateTime = datetime.strptime(timeString, dateStringFormat)
      myTime =  time.mktime(myDateTime.timetuple())
      deltaTime = myTime - firstTime
      attempt = matchObj1.group(2)
      eventMemorySize = long(matchObj1.group(3))
      validMallocLines = validMallocLines + 1
      currentlyUsedMemory = currentlyUsedMemory + eventMemorySize
      # use the parsed line in leiu of time for now
      memoryChangeEvents.append((long(deltaTime), currentlyUsedMemory / (1024 * 1024)))
      #print deltaTime, " $ ", (currentlyUsedMemory / (1024 * 1024))
    # or a free?
    elif matchObj2:
      timeString = matchObj2.group(1)
      myDateTime = datetime.strptime(timeString, dateStringFormat)
      #print "JB, ", myDateTime
      myTime =  time.mktime(myDateTime.timetuple())
      deltaTime = myTime - firstTime
      attempt = matchObj2.group(2)
      eventMemorySize = long(matchObj2.group(3))
      #print "line: ", line
      #print "event mem size: ", eventMemorySize
      validFreeLines = validFreeLines + 1
      currentlyUsedMemory = currentlyUsedMemory - eventMemorySize
      #print "\t\t free ", eventMemorySize, " total: ", currentlyUsedMemory
      #print attempt, "\t", " - ", eventMemorySize
      # use the parsed line in leiu of time for now
      memoryChangeEvents.append((long(deltaTime), currentlyUsedMemory / (1024 * 1024)))
      #print deltaTime, " $ ", (currentlyUsedMemory / (1024 * 1024))


  #print "mallocs: ", validMallocLines, " frees: ", validFreeLines
  if validMallocLines != validFreeLines:
    print "ERROR, mallocs: ", validMallocLines, " != frees ", validFreeLines
  return memoryChangeEvents
  
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

def presentData(arrayOfArraysOfTuples, ax1, plotLabels1, lns1):

  times = []
  sizes = []
  maxSize = 0
  # iterate through each hosts dataset
  for hostDataset in arrayOfArraysOfTuples:
    for dataPoint in hostDataset:
      (time,size) = dataPoint
      times.append(time)
      sizes.append(size)
      #print time, " $ ", size
      if size > maxSize:
        maxSize = size


  print "MaxSize: ", maxSize
  # plot each hosts data as it's parsed
  #ax1.plot(times, sizes)
  ax1.step(times, sizes, where='post')

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
    arrayOfArrays = []

    for filename in filesToParse:
      array = parseFile(dirToRead + "/" + filename)

      arrayOfArrays.append(array)

      if parsedOutputFilename is not None:
        dumpData(parsedOutputFilename, arrayOfArraysOfArrays)
      else:
        # now present it in some meaningful manner
        #presentData(arrayOfArraysOfAverages, ax, plotLabels1, lns1)
        presentData(arrayOfArrays, ax, plotLabels1, lns1)

        datasetNum = datasetNum + 1

        ax.set_xlabel("Time")
        ax.set_ylabel("Memory Consumed (MB) by Intermediate Data")
    
        #ax2.set_ylabel("Number of Shuffles")
        #ax.legend(plotLabels1, loc='upper left')

        #plt.tight_layout()
        ax.set_title("Memory Usage Over Time SIDR-IM 528 Reducers")
        #plt.show()
        print "calling savefig"
        plt.savefig("foo.pdf")

if __name__ == "__main__":
  main()
