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
myAnchor = (0.55,0.02)

sciShuffleCutoff = 400
sciHadoopCutoff = 800

#plotSymbolArray.append("g^")
#plotSymbolArray.append("gv")
#plotSymbolArray.append("r<")
#plotSymbolArray.append("r>")
#plotSymbolArray.append("bh")
#plotSymbolArray.append("bH")
#plotSymbolArray.append("black-.")
#plotSymbolArray.append("black--")
#plotSymbolArray.append("black:")

plotSymbolArray.append("s") # mappers for 132 red er
plotColorArray.append("b")
plotLineStyleArray.append("solid")

plotSymbolArray.append("x") # 22 red sock
plotColorArray.append("b")
plotLineStyleArray.append("dashed")

plotSymbolArray.append("v") # 132 red er
plotColorArray.append("g")
plotLineStyleArray.append("dashed")

plotSymbolArray.append("o") # mappers for 22 red stock
plotColorArray.append("g")
plotLineStyleArray.append("dashed")

plotSymbolArray.append(".") # 132 red er
plotColorArray.append("g")
plotLineStyleArray.append("dashed")

plotSymbolArray.append("^") # mappers for 22 red stock
plotColorArray.append("g")
plotLineStyleArray.append("dashed")

#plotSymbolArray.append(">") # 132 red er
#plotColorArray.append("g")
#plotLineStyleArray.append("solid")

#plotSymbolArray.append("v") # 132 red er
#plotColorArray.append("g")
#plotLineStyleArray.append("dashed")


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

def addNormalizedTimesPlot( ax, normalizedTimes):
  #noah's approach
  ecdf = sm.tools.ECDF(normalizedTimes)
  #x = np.linspace(min(normalizedTimes), max(normalizedTimes))
  x = np.linspace(0, max(normalizedTimes))
  y = ecdf(x)
  #fig = plt.figure()
  #plotSymbolArray = []
  #plotSymbolCounter = 0
  global plotLineStyleCounter
  global plotLineStyleArray
  global plotSymbolCounter 
  global plotSymbolArray
  global plotColorCounter 
  global plotColorArray

  ax.plot(x,y,marker=plotSymbolArray[plotSymbolCounter],\
          linestyle=plotLineStyleArray[plotLineStyleCounter],\
          color=plotColorArray[plotColorCounter])
  plotSymbolCounter = plotSymbolCounter + 1
  plotColorCounter = plotColorCounter + 1
  plotLineStyleCounter = plotLineStyleCounter + 1

  ax.set_title("MapReduce Task Completion Over Time", fontsize=18)
  ax.set_ylabel("Fraction of Total Output Available", fontsize=16)
  ax.set_xlabel("Time from Job Start (seconds)", fontsize=16)
  #plt.show()
  #ax.grid(True)
  #ax.xaxis.grid(True)
  #ax.yaxis.grid(True)

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

  for subDir in mapSubDirsToProcess:
    #print "subdir: ", subDir
    fileList = os.listdir(dirPath + "/" + subDir)

    for file in fileList:
      matchObj = re.match(fileSearchString, file)

      if matchObj:
        #print "calling parseFile on: ",\
        #   (dirPath + "/" + subDir + "/" + matchObj.group(0))

        (mapStartTime, mapEndTime) =\
          parseTaskFile( dirPath + "/" + subDir + "/" + matchObj.group(0))

        #reduceStartTimes.append(reduceStartTime)
        mapEndTimes.append(mapEndTime)

  mapEndTimes.sort()
  return mapEndTimes 

def parseDirectoryForReduceTasks(dirPath):
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
          parseTaskFile( dirPath + "/" + subDir + "/" + matchObj.group(0))

        #reduceStartTimes.append(reduceStartTime)
        reduceEndTimes.append(reduceEndTime)

  reduceEndTimes.sort()
  #print "returnign list with ", len(reduceEndTimes), " elements"
  return reduceEndTimes

def normalizeTimeToJobStart(jobStartTime, reducerEndTimes):
  normalizedTimes = []

  for taskTime in reducerEndTimes:
    totalTaskTime = taskTime - jobStartTime
    normalizedTimes.append(totalTaskTime.seconds)

  
  return normalizedTimes

def main():
  try:
    opts, args = getopt.getopt(sys.argv[1:], "hr:m:")
  except getopt.GetoptError, err:
    # print help information and exit:
    print str(err) # will print something like "option -a not recognized"
    usage()
    sys.exit(2)

  dirname = ""
  mapDirNameString = ""
  reducerDirNameString = ""

  for o, a in opts:
    #print "o:",o," a:",a
    if o in ("-r", "--reducer_dir"):
      reducerDirNameString = a
    elif o in ("-m", "--map_dir"):
      mapDirNameString = a
    elif o in ("-h", "--help"):
      usage()
      sys.exit()
    else:
      assert False, "unhandled option"

  #rcParams['figure.figsize']= 11,6
  fig = plt.figure(figsize=myFigSize)
  ax = fig.add_subplot(111)
  ax.grid(True,linestyle="-",color='0.75')
  #ax.grid(True)
  #pylab.grid()
  #ax.grid(True)
  #ax.xaxis.grid(True)
  #ax.yaxis.grid(True)
  plotLabels = []

  print "reducerString: ", reducerDirNameString," mapString: ", mapDirNameString
  if (reducerDirNameString != "") or (mapDirNameString != ""):
    if mapDirNameString != "":
      mapDirNames = mapDirNameString.split(",")
      #fig = plt.figure()
      #ax = fig.add_subplot(111)
      #plotLabels = []
      for name in mapDirNames:
        if os.path.isdir(name):
          mapEndTimes = parseDirectoryForMapTasks(name)
          reducerEndTimes = parseDirectoryForReduceTasks(name)
          jobStartTime = getJobStartTime(name)
          if jobStartTime is None:
            print "jobStartTime is None, skipping directory:", name
            continue
          normalizedTimeAsSeconds = normalizeTimeToJobStart(jobStartTime, mapEndTimes)
          normalizedReduceTimeAsSeconds = normalizeTimeToJobStart(jobStartTime, reducerEndTimes)

          print "first normalized Reduce Time: " + str(normalizedReduceTimeAsSeconds)
          addNormalizedTimesPlot( ax, normalizedTimeAsSeconds)
          if( normalizedReduceTimeAsSeconds[0] < sciShuffleCutoff):
            #plotLabels.append(str(len(normalizedTimeAsSeconds))+" Mappers for "+str(len(reducerEndTimes))+" Reducers")
            plotLabels.append("Map for "+str(len(reducerEndTimes))+" Reduces(SS)")
          elif (normalizedReduceTimeAsSeconds[0] < sciHadoopCutoff):
            #plotLabels.append(str(len(normalizedTimeAsSeconds))+" Mappers(stock)")
            plotLabels.append("Map for "+str(len(reducerEndTimes))+" Reduces(SH)")
          else:
            #plotLabels.append(str(len(normalizedTimeAsSeconds))+" Mappers(stock)")
            plotLabels.append("Map for "+str(len(reducerEndTimes))+" Reducers(H)")

        else:
          print "specified directory (",name, ") is not a valid directory"
      #ax.grid(True)
      #ax.xaxis.grid(True)
      #ax.yaxis.grid(True)

    if reducerDirNameString != "":
      reducerDirNames = reducerDirNameString.split(",")
      #fig = plt.figure()
      #ax = fig.add_subplot(111)
      for name in reducerDirNames:
        if os.path.isdir(name):
          reducerEndTimes = parseDirectoryForReduceTasks(name)
          jobStartTime = getJobStartTime(name)
          if jobStartTime is None:
            print "jobStartTime is None, skipping directory:", name
            continue
          normalizedTimeAsSeconds = normalizeTimeToJobStart(jobStartTime, reducerEndTimes)
          # convert the deltas to seconds
          #for myTime in normalizedTimeAsSeconds:
            #print myTime
            #normalizedTimesAsSeconds.append(myTime.seconds)
          print name + " last time: " + \
            str(normalizedTimeAsSeconds[ len(normalizedTimeAsSeconds) - 1])
          print name + " first time: " + \
            str(normalizedTimeAsSeconds[0])

          addNormalizedTimesPlot( ax, normalizedTimeAsSeconds)
          if( normalizedTimeAsSeconds[0] < sciShuffleCutoff):
            plotLabels.append(str(len(normalizedTimeAsSeconds))+" Reduces(SS)")
          elif( normalizedTimeAsSeconds[0] < sciHadoopCutoff):
            plotLabels.append(str(len(normalizedTimeAsSeconds))+" Reduces(SH)")
          else:
            plotLabels.append(str(len(normalizedTimeAsSeconds))+" Reduces(H)")

        else:
          print "specified directory (",name, ") is not a valid directory"

    #plt.legend(plotLabels, 'upper left')
    #plt.legend(plotLabels, 'lower center', bbox_to_anchor = myAnchor)
    plt.legend(plotLabels, 'best')
    #ax.grid(True)
    #ax.xaxis.grid(True)
    #ax.yaxis.grid(True)
    gca().yaxis.grid(True)
    ax.yaxis.grid(True)
    plt.show()

  else:
    print "neither reducer nor map directories were specified. Poor form"
    exit

  #debugPrintResults(results, time)

if __name__ == "__main__":
  main()


