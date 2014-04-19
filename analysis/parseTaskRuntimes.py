#!/usr/bin/python
import getopt, sys, re, datetime, time, os
from datetime import datetime, date
import numpy as np
from pylab import *
from bisect import bisect_left
from scipy.stats import norm
from scipy.stats import cumfreq
import matplotlib.pyplot as plt
import statsmodels.tools as sm

myFigSize = (9.5,6)

def usage():
  print "./parseHadoopTimers.py -d <hadoop job directory>"

def parseReduceFiles(filename):
  filename = filename.strip()

  # read the data
  FILE = open(filename, "r")
  # make sure that you have enough memory (but test file is ~8 MB so we should be fine)
  data = FILE.readlines() 
  FILE.close()

  # get the reduce times
  #searchString = "reducer took: (\d+) ms"
  #searchString = "(\d+\-\d+\-\d+) (\d+\:\d+\:\d+),\d+"
  searchString = "(\d+\-\d+\-\d+ \d+\:\d+\:\d+),\d+"

  #runningReduceTime = 0
  #reducesEncountered = 0
  #startTime = 0
  startDateTime = 0
  #endTime = 0
  endDateTime = 0

  #for line in data:
  while 0 == startDateTime:
    for line in data:
      matchObj = re.match(searchString, line)
      #print "line: ", line

      if matchObj:
        startDateTime = datetime.datetime.strptime(matchObj.group(1), "%Y-%m-%d %H:%M:%S")
        break
  
  while 0 == endDateTime:
    for line in reversed(data):
      matchObj = re.match(searchString, line)

      if matchObj:
        endDateTime = datetime.datetime.strptime(matchObj.group(1), "%Y-%m-%d %H:%M:%S")
        break

  #mytimeDelta = datetime.combine(endDate, endTime) - datetime.combine(startDate, startTime)
  #mytimeDelta = endDateTime - startDateTime
  #print "file: ",filename," start: ",startDateTime," end: ",endDateTime," delta: ", mytimeDelta

  #print "file: ",filename," start: ",startDateTime," end: ",endDateTime

  return (startDateTime, endDateTime)

# prints out all the keywords and their values
def debugPrintResults(results, time):
  print "test ran in %s:%s" % (divmod(time,60))
  for k,v in results.iteritems():
    for entry in v:
      print k, "=>", entry

def addNormalizedTimesPlot( ax, normalizedTimes):
  #noah's approach
  ecdf = sm.tools.ECDF(normalizedTimes)
  x = np.linspace(min(normalizedTimes), max(normalizedTimes))
  y = ecdf(x)
  #fig = plt.figure()
  ax.plot(x,y)
  ax.set_title("Reduce Task Completion CDF")
  ax.set_ylabel("Fraction of Jobs Complete")
  ax.set_xlabel("Time from Job Start (seconds)")
  #plt.show()
  ax.grid()

def getJobStartTime(dirPath):
  # find the hadoop-buck-jobtracker.... file
  currentDirectoryContents = os.listdir("./")
  jobTrackerSearchString = "hadoop-buck-jobtracker-issdm-39.log"

  for entry in currentDirectoryContents:
    if os.path.isdir(entry) == True:
      pass
    else:
      matchObj = re.match(jobTrackerSearchString, entry)
      dirPath = dirPath.strip()

      if matchObj:
        #now search for the time stamp of the first time our job is mentioned
        filename = matchObj.group(0).strip()

        # read the data
        FILE = open(filename, "r")
        # make sure that you have enough memory (but test file is ~8 MB so we should be fine)
        data = FILE.readlines() 
        FILE.close()

        print "looking for entry: ", dirPath, " in file: ", filename
        for line in data:
          line = line.strip()
          matchObj2 = re.match(".*"+dirPath+".*", line)
          #print dirPath, "%*%", line


          if matchObj2:
            myLine = matchObj2.group(0)
            print "found it: ", myLine
            dateTimeSearchString = "(\d+\-\d+\-\d+ \d+\:\d+\:\d+).*"
            matchObj3 = re.match(dateTimeSearchString, myLine)

            if matchObj3:
              print "match3: ", matchObj3.group(0)

              firstStartTime = datetime.datetime.strptime(matchObj3.group(1), "%Y-%m-%d %H:%M:%S")
              print "firstStartTime: ", firstStartTime

              return firstStartTime
       

def parseDirectory(dirPath):
  print "parsing", dirPath

  # dirName should look something like this:
  # job_201104262343_0002
  # and it has subdirs that look like this
  # attempt_201104262343_0002_m_000005_0
  # attempt_201104262343_0002_r_000000_0

  dirList = os.listdir(dirPath)

  mapSubDirsToProcess = []
  mapSubDirSearchString = "attempt_\d+_\d+_m_\d+_\d+"

  for mapSubDir in dirList:
    matchObj = re.match(mapSubDirSearchString, mapSubDir)

    if matchObj:
      #print "adding ", matchObj.group(0)
      mapSubDirsToProcess.append(matchObj.group(0))

  reduceSubDirsToProcess = []
  reduceSubDirSearchString = "attempt_\d+_\d+_r_\d+_\d+"

  for reduceSubDir in dirList:
    matchObj = re.match(reduceSubDirSearchString, reduceSubDir)

    if matchObj:
      #print "adding ", matchObj.group(0)
      reduceSubDirsToProcess.append(matchObj.group(0))


  fileSearchString = "syslog"

  #reduceStartTimes = []
  reduceEndTimes = []

  for subDir in reduceSubDirsToProcess:
    #print "subdir: ", subDir
    fileList = os.listdir(dirPath + "/" + subDir)

    for file in fileList:
      matchObj = re.match(fileSearchString, file)

      if matchObj:
        #print "calling parseFile on: ",\
        #   (dirPath + "/" + subDir + "/" + matchObj.group(0))

        (reduceStartTime, reduceEndTime) =\
          parseReduceFiles( dirPath + "/" + subDir + "/" + matchObj.group(0))

        #reduceStartTimes.append(reduceStartTime)
        reduceEndTimes.append(reduceEndTime)

        #print "post-call, file: ",(dirPath + "/" + subDir + "/" + matchObj.group(0)),\
        #      " start: ",reduceStartTime," end: ",reduceEndTime;
          

  # find the earliest start time
  #firstStartTime = 0
  #for startTime in reduceStartTimes:
  #  if( 0 == firstStartTime):
  #    firstStartTime = startTime
  #  else:
  #    if( startTime < firstStartTime):
  #      firstStartTime = startTime
  #
  #print "firstStartTime: ", firstStartTime

  #normalizedRunTimes = []
  #for endTime in reduceEndTimes:
  #  normalizedRunTimes.append( endTime - firstStartTime)
    #print "first Start: ", firstStartTime, "endTime: ", endTime, " normalized runtime: ", (endTime - firstStartTime)

  #normalizedRunTimes.sort()

  #for normalizedTime in normalizedRunTimes:
  #  print normalizedTime

  reduceEndTimes.sort()
  print "returnign list with ", len(reduceEndTimes), " elements"
  return reduceEndTimes

def normalizeTimeToJobStart(jobStartTime, reducerEndTimes):
  normalizedTimes = []

  for taskTime in reducerEndTimes:
    totalTaskTime = taskTime - jobStartTime
    normalizedTimes.append(totalTaskTime.seconds)

  
  return normalizedTimes

def main():
  try:
    opts, args = getopt.getopt(sys.argv[1:], "hd:")
  except getopt.GetoptError, err:
    # print help information and exit:
    print str(err) # will print something like "option -a not recognized"
    usage()
    sys.exit(2)

  dirname = ""

  for o, a in opts:
    if o in ("-d", "--dirname"):
      dirnamesString = a
    elif o in ("-h", "--help"):
      usage()
      sys.exit()
    else:
      assert False, "unhandled option"

  if dirnamesString != "":
    dirNames = dirnamesString.split(",")
    fig = plt.figure(figsize=myFigSize)
    ax = fig.add_subplot(111)
    plotLabels = []
    for name in dirNames:
      if os.path.isdir(name):
        reducerEndTimes = parseDirectory(name)
        jobStartTime = getJobStartTime(name)
        normalizedTimeAsSeconds = normalizeTimeToJobStart(jobStartTime, reducerEndTimes)
        # convert the deltas to seconds
        #for myTime in normalizedTimeAsSeconds:
          #print myTime
          #normalizedTimesAsSeconds.append(myTime.seconds)

        #def addNormalizedTimesPlt( ax, normalizedTimes, testName):
        #plotNormalizedTimes( normalizedTimeAsSeconds)
        addNormalizedTimesPlot( ax, normalizedTimeAsSeconds)
        if( normalizedTimeAsSeconds[0] < 300):
          plotLabels.append(str(len(normalizedTimeAsSeconds))+" Reducers")
        else:
          plotLabels.append(str(len(normalizedTimeAsSeconds))+" Reducers (stock)")

      else:
        print "specified directory (",name, ") is not a valid directory"
    plt.legend(plotLabels, 'upper left')
    plt.show()
  else:
    print "a dirname(%s) was not specified. Poor form", dirnamesString 
    exit

  #debugPrintResults(results, time)

if __name__ == "__main__":
  main()


