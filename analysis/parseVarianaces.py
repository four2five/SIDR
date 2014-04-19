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

plotSymbolArray = []
plotColorArray = []
plotLineStyleArray = []
plotSymbolCounter = 0
plotColorCounter = 0
plotLineStyleCounter = 0
myFigSize = (9.5,6)

#plotSymbolArray.append("g^")
#plotSymbolArray.append("gv")
#plotSymbolArray.append("r<")
#plotSymbolArray.append("r>")
#plotSymbolArray.append("bh")
#plotSymbolArray.append("bH")
#plotSymbolArray.append("black-.")
#plotSymbolArray.append("black--")
#plotSymbolArray.append("black:")

plotSymbolArray.append("*") # mappers for 22 red stock
plotColorArray.append("r")
#plotSymbolArray.append("kh") # mappers for 22 red er
plotSymbolArray.append(".") # mappers for 132 red er
plotColorArray.append("b")
#plotSymbolArray.append("r.") # mappers ofr 1056 red er
plotSymbolArray.append("x") # 22 red sock
plotColorArray.append("g")

plotSymbolArray.append("v") # 132 red er
plotColorArray.append("k")

plotSymbolArray.append("o") # 132 red er
plotColorArray.append("r")

plotSymbolArray.append("s") # 132 red er
plotColorArray.append("k")

plotLineStyleArray.append("dashed")
plotLineStyleArray.append("dotted")
plotLineStyleArray.append("solid")


def usage():
  print "./parseHadoopTimers.py -d <hadoop job directory>"

def parseTaskFile(filename):
  filename = filename.strip() 
  # read the data
  FILE = open(filename, "r")
  # make sure that you have enough memory (but test file is ~8 MB so we should be fine)
  data = FILE.readlines() 
  FILE.close()

  # get the reduce times
  searchString = "(\d+\-\d+\-\d+ \d+\:\d+\:\d+),\d+"

  startDateTime = 0
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

  #print "file: ",filename," start: ",startDateTime," end: ",endDateTime," delta: ", mytimeDelta
  #print "file: ",filename," start: ",startDateTime," end: ",endDateTime

  return (startDateTime, endDateTime)

# prints out all the keywords and their values
def debugPrintResults(results, time):
  print "test ran in %s:%s" % (divmod(time,60))
  for k,v in results.iteritems():
    for entry in v:
      print k, "=>", entry

def addNormalizedTimesPlotWithErrorBars( ax, normalizedTimes):

  # grab teh first array and check its length. 
  numRuns = len(normalizedTimes)
  if( numRuns <= 0 ):
    print "addNormalizedTimesPlotWithErrorBars was len ", numRuns, ". Bad news"
    return

  numTasksPerRun = len(normalizedTimes[0]) 
  print "there are ", numRuns, " runs with each having length ", numTasksPerRun

  for run in normalizedTimes:
    if( len(run) != numTasksPerRun):
      print "run had ", len(run), " entries. Expected:", numTasksPerRun

  ecdfArray = []
  xArray = []
  yArray = []

  # find the max value for all runs
  maxVal = 0;
  for i in range(numRuns):
    print "run[", i, "]: last time:",  normalizedTimes[i][numTasksPerRun - 1]
    if( normalizedTimes[i][numTasksPerRun - 1] > maxVal):
      maxVal =  normalizedTimes[i][numTasksPerRun - 1]

  print "maxVal: ", maxVal

  counter = 0
  for i in range(numRuns):
    tempArray = []
    for j in range(numTasksPerRun):
      tempArray.append(normalizedTimes[i][j])

    ecdfArray.append( sm.tools.ECDF(tempArray))
    xArray.append(np.linspace(0, maxVal))
    yArray.append(ecdfArray[counter](xArray[counter]))
    #print "temp[-1]: ", tempArray[-1]
    #print "xArray[", counter,"]:", xArray[counter] 
    #print " yArray[", counter, "]:", str(yArray[counter])
    counter = counter + 1

  cdfAvg = []
  cdfStdDev = []
  counter = 0
  for k in range(len(yArray[0])):
    tempArray = []
    for i in range(numRuns):
      tempArray.append(yArray[i][k])
      #print "appending: ", yArray[i][k], " for ", counter

    cdfAvg.append(average(tempArray))
    cdfStdDev.append(std(tempArray))
    counter = counter + 1

  counter = 0
  for value in cdfAvg:
    print "cdfAvg[", counter, "]: ", cdfAvg[counter], " stddev:", cdfStdDev[counter]
    counter = counter + 1


  #ecdf = sm.tools.ECDF(avgArray)
  #x = np.linspace(0, maxVal)
  #y = ecdf(x)
  global plotLineStyleCounter 
  global plotLineStyle 
  global plotSymbolCounter 
  global plotSymbolArray
  global plotColorCounter 
  global plotColorArray
  #ecdf2 = sm.tools.ECDF(stdDevArray)
  #ecdf2 = sm.tools.ECDF(correctedStdDevArray)
  #errorLine = ecdf2(x)

  #for i in range(50):
  #  print i,": ", x[i], ", ", y[i], ",", errorLine[i]

  #ax.plot(x,y,marker=plotSymbolArray[plotSymbolCounter],linestyle=plotLineStyle,\
  #        color=plotColorArray[plotColorCounter])
  #print "len(x)", len(x), " len(y)", len(y), " len(stddev)", len(stdDevArray)
  #print "len(x)", len(x), " len(y)", len(y), " len(errorLine)", len(errorLine)

  if numTasksPerRun == 2783:
    labelNameForPlot = str(numTasksPerRun) + " Mappers"
  else:
    labelNameForPlot = str(numTasksPerRun) + " Reducers"

  print "plotLineStyle[",plotLineStyleCounter, "]:", plotLineStyleArray[plotLineStyleCounter], " plot label: ", labelNameForPlot
  errLeg = ax.errorbar(xArray[0], cdfAvg, yerr=cdfStdDev, linestyle=plotLineStyleArray[plotLineStyleCounter], color=plotColorArray[plotColorCounter], marker=plotSymbolArray[plotSymbolCounter], label=labelNameForPlot)
  #ax.errorbar(range(len(avgArray)), avgArray, yerr=stdDevArray)
  plotSymbolCounter = plotSymbolCounter + 1
  plotColorCounter = plotColorCounter + 1
  plotLineStyleCounter = plotLineStyleCounter + 1

  return ax 

def getJobStartTime(dirPath):
  # find the hadoop-buck-jobtracker.... file
  print "dirPath: ", dirPath
  currentDirectoryContents = os.listdir(dirPath)
  jobTrackerSearchString = "hadoop-buck-jobtracker-issdm-39.log"

  for entry in currentDirectoryContents:
    if os.path.isdir(entry) == True:
      #print "entry that is a dir: ", entry
      pass
    else:
      #print "entry that is not a dir: ", entry
      matchObj = re.match(jobTrackerSearchString, entry)
      dirPath = dirPath.strip()

      if matchObj:
        #now search for the time stamp of the first time our job is mentioned
        filename = matchObj.group(0).strip()

        filename = dirPath + "/" + filename  
        # read the data
        print "filename: ", filename
        FILE = open(filename, "r")
        # make sure that you have enough memory (but test file is ~8 MB so we should be fine)
        data = FILE.readlines() 
        FILE.close()

        # parse out the job name from the dirPath passed in
        jobSearchString = ".*(job_\d+_\d+).*"
        jobMatchObj = re.match(jobSearchString, dirPath)

        jobName = ""
        if None != jobMatchObj:
          jobName = jobMatchObj.group(1)
          print "job name: ", jobName 

        print "looking for entry: ", jobName, " in file: ", filename
        for line in data:
          line = line.strip()
          matchObj2 = re.match(".*"+jobName+".*", line)
          #print dirPath, "%*%", line


          if matchObj2:
            myLine = matchObj2.group(0)
            print "found it: ", myLine
            dateTimeSearchString = "(\d+\-\d+\-\d+ \d+\:\d+\:\d+).*"
            matchObj3 = re.match(dateTimeSearchString, myLine)

            if matchObj3:
              #print "match3: ", matchObj3.group(0)

              firstStartTime = datetime.datetime.strptime(matchObj3.group(1), "%Y-%m-%d %H:%M:%S")
              #print "firstStartTime: ", firstStartTime

              return firstStartTime
       

def parseDirectoryForMapTasks(dirPath):
  print "parsing", dirPath, " for Map tasks"

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

  #reduceSubDirsToProcess = []
  #reduceSubDirSearchString = "attempt_\d+_\d+_r_\d+_\d+"

  #for reduceSubDir in dirList:
  #  matchObj = re.match(reduceSubDirSearchString, reduceSubDir)

  #  if matchObj:
      #print "adding ", matchObj.group(0)
  #    reduceSubDirsToProcess.append(matchObj.group(0))


  fileSearchString = "syslog"

  #reduceStartTimes = []
  mapEndTimes = []

  validCounter = 0
  mapSubDirsToProcess.sort()
  for subDir in mapSubDirsToProcess:
    #print "subdir: ", subDir
    fileList = os.listdir(dirPath + "/" + subDir)

    for file in fileList:
      matchObj = re.match(fileSearchString, file)

      if matchObj:
        #print "calling parseFile on: ",\
        #   (subDir)

        validCounter = validCounter + 1
        (mapStartTime, mapEndTime) =\
          parseTaskFile( dirPath + "/" + subDir + "/" + matchObj.group(0))

        #reduceStartTimes.append(reduceStartTime)
        mapEndTimes.append(mapEndTime)

  print "returning ", len(mapEndTimes), " for dir ", dirPath, " counter: ", validCounter
  mapEndTimes.sort()
  return mapEndTimes 

def parseDirectoryForReduceTasks(dirPath):
  print "parsing", dirPath, " for reduce tasks"

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
    #else:
      #print mapSubDir, " is not a map match"

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
          parseTaskFile( dirPath + "/" + subDir + "/" + matchObj.group(0))

        #reduceStartTimes.append(reduceStartTime)
        reduceEndTimes.append(reduceEndTime)

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
    opts, args = getopt.getopt(sys.argv[1:], "h2:m:8:")
  except getopt.GetoptError, err:
    # print help information and exit:
    print str(err) # will print something like "option -a not recognized"
    usage()
    sys.exit(2)

  dirname = ""
  mapDirNameString = ""
  reducerDirNameString = ""
  reducerDirNameString2 = ""

  for o, a in opts:
    print "o:",o," a:",a
    if o in ("-2"):
      print "-2: ", a
      reducerDirNameString = a
    elif o in ("-m", "--map_dir"):
      mapDirNameString = a
    elif o in ("-8"):
      print "-8: ", a
      reducerDirNameString2 = a
    elif o in ("-h", "--help"):
      usage()
      sys.exit()
    else:
      assert False, "unhandled option"


  fig = plt.figure(figsize=myFigSize)
  ax = fig.add_subplot(111)
  plotLabels = []

  print "reducerString: ", reducerDirNameString," mapString: ", mapDirNameString
  if (reducerDirNameString != "") or (mapDirNameString != ""):
    errLegendEntries = []
    if mapDirNameString != "":
      mapDirNames = mapDirNameString.split(",")
      #fig = plt.figure()
      #ax = fig.add_subplot(111)
      #plotLabels = []
      allMapEndTimes = []
      normalizedMTimeAsSeconds = []
      counter = 0
      jobStartTime = 0
      normalizedReduceTimeAsSeconds = 0

      for name in mapDirNames:
        print "name1: ", name
        if os.path.isdir(name):
          #mapEndTimes = parseDirectoryForMapTasks(name)
          allMapEndTimes.append(parseDirectoryForMapTasks(name))
          reducerEndTimes = parseDirectoryForReduceTasks(name)
          jobStartTime = getJobStartTime(name)
          if jobStartTime is None:
            print "jobStartTime is None, skipping directory:", name
            continue
          print "allMapEndsTimes[", counter, "]: len:", len(allMapEndTimes[counter])
          normalizedMTimeAsSeconds.append(normalizeTimeToJobStart(jobStartTime, allMapEndTimes[counter]))
          normalizedReduceTimeAsSeconds = normalizeTimeToJobStart(jobStartTime, reducerEndTimes)
          counter = counter + 1
        else:
          print "specified directory (",name, ") is not a valid directory"

      print "first normalized Reduce Time: " + str(normalizedReduceTimeAsSeconds[0])
      ax = addNormalizedTimesPlotWithErrorBars( ax, normalizedMTimeAsSeconds)


    if reducerDirNameString != "":
      reducerDirNames = reducerDirNameString.split(",")
      allReducerEndTimes = []
      normalizedRTimeAsSeconds = []
      counter = 0
      jobStartTime = 0
      normalizedReduceTimeAsSeconds = 0

      for name in reducerDirNames:
        print "name2: ", name
        if os.path.isdir(name):
          allReducerEndTimes.append(parseDirectoryForReduceTasks(name))
          print "allReducerEndTimes len: ", len(allReducerEndTimes)
          reducerEndTimes = parseDirectoryForReduceTasks(name)
          jobStartTime = getJobStartTime(name)
          if jobStartTime is None:
            print "jobStartTime is None, skipping directory:", name
            continue
          normalizedRTimeAsSeconds.append(normalizeTimeToJobStart(jobStartTime, reducerEndTimes))
          counter = counter + 1
          # convert the deltas to seconds
          #for myTime in normalizedTimeAsSeconds:
            #print myTime
            #normalizedTimesAsSeconds.append(myTime.seconds)
          print name + " last time: " + str(normalizedRTimeAsSeconds[ len(normalizedRTimeAsSeconds) - 1])
#
        else:
          print "specified directory (",name, ") is not a valid directory"

      ax = addNormalizedTimesPlotWithErrorBars( ax, normalizedRTimeAsSeconds)
      plotLabels.append(str(len(normalizedRTimeAsSeconds))+" Reducers")

    if reducerDirNameString2 != "":
      reducerDirNames = reducerDirNameString2.split(",")
      allReducerEndTimes = []
      normalizedRTimeAsSeconds = []
      counter = 0
      jobStartTime = 0
      normalizedReduceTimeAsSeconds = 0

      for name in reducerDirNames:
        print "name3: ", name
        if os.path.isdir(name):
          allReducerEndTimes.append(parseDirectoryForReduceTasks(name))
          reducerEndTimes = parseDirectoryForReduceTasks(name)
          jobStartTime = getJobStartTime(name)
          if jobStartTime is None:
            print "jobStartTime is None, skipping directory:", name
            continue
          normalizedRTimeAsSeconds.append(normalizeTimeToJobStart(jobStartTime, reducerEndTimes))
          counter = counter + 1
          # convert the deltas to seconds
          #for myTime in normalizedTimeAsSeconds:
            #print myTime
            #normalizedTimesAsSeconds.append(myTime.seconds)
          print name + " last time: " + str(normalizedRTimeAsSeconds[ len(normalizedRTimeAsSeconds) - 1])
#
        else:
          print "specified directory (",name, ") is not a valid directory"

      ax = addNormalizedTimesPlotWithErrorBars( ax, normalizedRTimeAsSeconds)
      plotLabels.append(str(len(normalizedRTimeAsSeconds))+" Reducers")

    ax.set_title("MapReduce Task Completions Averaged Over 10 Runs")
    ax.set_ylabel("Fraction of Total Output Available", fontsize=14)
    ax.set_xlabel("Time from Job Start (seconds)", fontsize=14)
    ax.set_xlim([0,1400])
    ax.set_ylim([0,1.0])
    ax.grid()

    #plt.legend(plotLabels, 'upper left')
    #plt.legend(errLegendEntries, 'upper left')
    handles, labels = ax.get_legend_handles_labels()

    for label in labels:
      print "label: ", label
    #ax.legend(handles, labels, loc='upper left')
    #ax.legend(loc='upper left')
    ax.legend(loc='upper left')
    plt.show()

  else:
    print "a neither reducer nor map directories were specified. Poor form"
    exit

  #debugPrintResults(results, time)

if __name__ == "__main__":
  main()


