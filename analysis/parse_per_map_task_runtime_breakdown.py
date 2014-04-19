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
  print "./parse_loggedfs.py <file to parse>[,<another file to parse>]"


def parseSubDir(subdirToParse):
  #print "parsing subdir ", subdirToParse

  # we always want to parse the "syslog" file in the subdir
  #searchString = "(sd[a-d]1)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)"
  #searchString = "Shuffle time (\d+)(\d+) bytes"
  #searchString1 = "IO time: 4179 for 103680000 bytes"
  searchString1 = ".*IO time: (\d+) for (\d+) bytes.*"
  #searchString2 = "2014-04-15 16:57:18,292 INFO edu.ucsc.srl.damasc.hadoop.map.MedianMapperInt: in mapper, corner is: [352, 0, 0, 0] shape: [2, 360, 720, 50] extsize: 25920 extShape: [2, 36, 36, 10] datatypeSize: 4"
  searchString2 = "(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d\d\d) INFO edu.ucsc.srl.damasc.hadoop.map.*in mapper.*"
  searchString3 = "(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d\d\d) INFO edu.ucsc.srl.damasc.hadoop.map.*: Wrote out.*"
  searchString4 = "(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d\d\d) .*Register final output"
  searchString5 = "(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d\d\d) .*"
  #2014-04-15 15:31:52,284 INFO org.apache.hadoop.mapred.Task: Task:attempt_201404151524_0002_m_000238_0 is done. And is in the process of commiting
  searchString6 = "(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d\d\d) .*And is in the process of commiting.*"
  searchString7 = "(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d\d\d) .*done.*"
  dateStringFormat =  '%Y-%m-%d %H:%M:%S,%f'
  ins = open(subdirToParse + "/syslog", "r")
  
  # for sanity checking
  retArray = []
  linesParsed = 0
  ioTime = None

  mapStartTime = None
  mapEndTime = None

  registerOutputStartTime = None
  registerOutputEndTime = None

  commitStartTime = None
  commitEndTime = None

  for line in ins:
    linesParsed = linesParsed + 1
    # we only want to store reads and writes, filter out the rest
    matchObj1 = re.match(searchString1, line.lstrip())

    if matchObj1:
      if ioTime is not None:
        print "ERROR, found two IO times"
        sys.exit(2)
      else:
        ioTime = long(matchObj1.group(1)) / 1000 # convert milliseconds to seconds
        ioSize = matchObj1.group(2)
        #print "Found ioTime: " + str(ioTime)
        #print line
      continue

    # if search1 didn't match, try the next one
    matchObj2 = re.match(searchString2, line.lstrip())

    if matchObj2:
      if mapStartTime is not None:
        print "ERROR, found two map start times"
        sys.exit(2)
      else:
        nextLineIsMapRuntime = True
        timeString = matchObj2.group(1)
        myDateTime = datetime.strptime(timeString, dateStringFormat)
        mapStartTime =  long(time.mktime(myDateTime.timetuple()))
        #print "Found mapStartTime: " + str(mapStartTime)
        #print line
        continue

    # this should only occur once, when the previous line was the mapStartTime
    if mapStartTime is not None and mapEndTime is None:
      matchObj3 = re.match(searchString3, line.lstrip())
      if matchObj3:
        timeString = matchObj3.group(1)
        myDateTime = datetime.strptime(timeString, dateStringFormat)
        mapEndTime =  long(time.mktime(myDateTime.timetuple()))
        #print "Found mapEndTime: " + str(mapEndTime)
        #print line
        continue
      else:
        print "THIS DID NOT MATCH string 3: " + line
        print "search string 3: " + searchString3

    matchObj4 = re.match(searchString4, line.lstrip())

    if matchObj4:
      if registerOutputStartTime is not None:
        print "ERROR, found two register output start times"
        sys.exit(2)
      else:
        timeString = matchObj4.group(1)
        myDateTime = datetime.strptime(timeString, dateStringFormat)
        registerOutputStartTime =  long(time.mktime(myDateTime.timetuple()))
        #print "Found registerOutputStartTime: " + str(registerOutputStartTime)
        #print line
        continue

    # this should only occur once, when the previous line was the mapStartTime
    if registerOutputStartTime is not None and registerOutputEndTime is None:
      matchObj5 = re.match(searchString5, line.lstrip())
      timeString = matchObj5.group(1)
      myDateTime = datetime.strptime(timeString, dateStringFormat)
      registerOutputEndTime =  long(time.mktime(myDateTime.timetuple()))
      #print "Found registerOutputEndTime: " + str(registerOutputEndTime)
      #print line
      continue

    matchObj6 = re.match(searchString6, line.lstrip())

    if matchObj6:
      if commitStartTime is not None:
        print "ERROR, found two commit start times"
        sys.exit(2)
      else:
        timeString = matchObj6.group(1)
        myDateTime = datetime.strptime(timeString, dateStringFormat)
        commitStartTime =  long(time.mktime(myDateTime.timetuple()))
        #print "Found commitStartTime: " + str(commitStartTime)
        #print line
        continue

    # this should only occur once, when the previous line was the commitStartTime
    if commitStartTime is not None and commitEndTime is None:
      matchObj7 = re.match(searchString7, line.lstrip())
      if matchObj7:
        timeString = matchObj7.group(1)
        myDateTime = datetime.strptime(timeString, dateStringFormat)
        commitEndTime =  long(time.mktime(myDateTime.timetuple()))
        #print "Found commitEndTime: " + str(commitEndTime)
        #print line
        continue
      else:
        print "THIS SHOULD HAVE BEEN A TIME LINE: " + line
        print "(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d\d\d) .*done.*"
        print subdirToParse
        continue

  if ioTime is None:
    print subdirToParse + " MISSING ioTime"
  if mapStartTime is None or mapEndTime is None:
    print subdirToParse + " MISSING mapTime"
  if registerOutputStartTime is None or registerOutputEndTime is None:
    print subdirToParse + " MISSING registerOutput"
  if commitStartTime is None or commitEndTime is None:
    print subdirToParse + " MISSING commitOutput"

  # the last task has no entries. Assume that we've hit that Map task
  if ioTime is None and mapStartTime is None and registerOutputStartTime is None:
    return None

  #print "ioTime: " + str(ioTime)
  #print "mapTime: " + str(long(mapEndTime) - long(mapStartTime)) 
  #print "registerOutputTime: " + str(long(registerOutputEndTime) - long(registerOutputStartTime)) 
  #print "commitTime: " + str(long(commitEndTime) - long(commitStartTime)) 

  #retArray = []
  retArray.append(ioTime)
  retArray.append(long(mapEndTime) - long(mapStartTime))
  retArray.append(long(registerOutputEndTime) - long(registerOutputStartTime))
  retArray.append(long(commitEndTime) - long(commitStartTime))

  return retArray
  
def dumpData(parsedOutputFilename, arrayOfArraysOfArrays):
  outputStream = open(parsedOutputFilename, "w")
  outputStream.write(expName + "\n")
  outputStream.write("ioTime$mapTime$registerOutputTime$commitTime\n")

  # iterate through all of the variables 
  for perMapData in arrayOfArraysOfArrays:
    (ioTime, mapTime, registerOutputTime, commitTime) = perMapData
    outputStream.write(str(str(ioTime) +  "$" +  str(mapTime) + "$" + \
                           str(registerOutputTime) + "$" + str(commitTime) + "\n"))

  outputStream.close()

def presentData(arrayOfArraysOfArrays, ax1, ax2, plotLabels1, lns1):

  times = []
  sizes = []
  # iterate through all of the variables 
  for perHostData in arrayOfArraysOfArrays:

    for dataPoint in perHostData[0]:
      #times.append(int(dataPoint / 1000000))  # convert nanoseconds into milliseconds
      times.append(int(dataPoint))  # convert nanoseconds into milliseconds

    for dataPoint in perHostData[1]:
      sizes.append(int(dataPoint)) # turn it into KB

  #hist, bins = np.histogram(np.array(times))

  # let's try manually creating bins
  myTimesBins = [0, 100, 250, 500, 1000, 1500, 2000, 3000, 4000, 5000, 10000]
                # 0, 100KB, 1MB, 10MB, 25MB, 50MB, 100MB, 2
  mySizesBins = [0, 100, 1024, 5120, 10240, 25600, 51200, 102400] 
  #n, bins, patches = ax1.hist(np.array(times), 10, normed=True)
  #n, bins, patches = ax1.hist(times, 10, normed=True, cumulative=True)
  #n, bins, patches = ax1.hist(np.array(times), 10, normed=True )
  #n, bins, patches = ax1.hist(times, 20, normed=True )
  #n, bins, patches = ax1.hist(times, 10)
  n, bins, patches = ax1.hist(times, myTimesBins)

  #n, bins, patches = ax2.hist(sizes, 10)
  n, bins, patches = ax2.hist(sizes, mySizesBins)

  #xticks_labels = ax1.get_xticklabels()
  #for counter in xrange(len(xticks_labels)):
  #  print "counter: ", str(counter), " xl: ", xticks_labels[counter], " b: ", bins[(counter + 1)]
    #xticks_labels[counter] = bins[(counter + 1)]
  #  xticks_labels[counter] = bins[(counter)]

  #ax1.set_xticklabels(xticks_labels)
  #ax1.set_xticks(myBins)
  #print "all hists:"
  for dataPoint in n:
    print "\t", dataPoint

  print "all bins:"
  for dataPoint in bins:
    print "\t", dataPoint

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
    ax2 = ax.twinx()
    plotLabels1 = []
    plotLabels2 = []
    lns1 = []
    lns2 = []
    datasetNum = 0
    arrayOfArraysOfArrays = []

    for filename in filesToParse:
      data1 = []
      data2 = []

      toRead = dirToRead + "/" + filename

      # the input was already formatted, just read it in
      if formattedInputFile:
        ins = open(toRead, "r")
  
        for line in ins:
          d1, d2 = line.rstrip().split("$")
          data1.append(d1)
          data2.append(d2)

        ins.close()

      # else, parse the raw log files
      else:
        # pull out the per-second seek rates
        retVal = parseSubDir(toRead)
        if retVal is not None:
          arrayOfArraysOfArrays.append(retVal)

    if parsedOutputFilename is not None:
      dumpData(parsedOutputFilename, arrayOfArraysOfArrays)
    else:
      # now present it in some meaningful manner
      #presentData(arrayOfArraysOfAverages, ax, plotLabels1, lns1)
      #presentData(arrayOfArraysOfArrays, ax, ax2, plotLabels1, lns1)

      print "adding graph for ", figureLabelList[datasetNum]
      datasetNum = datasetNum + 1

      ax.set_xlabel("Time per Shuffle (milliseconds)")
      ax.set_ylabel("Number of Shuffles")
    
      ax2.set_ylabel("Number of Shuffles")
      #ax.legend(plotLabels1, loc='upper left')

      #plt.tight_layout()
      ax.set_title("Average Shuffle Times " + expName)
      #plt.show()
      print "calling savefig"
      plt.savefig("foo.pdf")

if __name__ == "__main__":
  main()
