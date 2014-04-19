#!/usr/bin/python

import sys
import os
import shlex
import numpy as np
import pylab as p

from collections import defaultdict 


def readFileIntoDict( dirPath, fileToRead, inDict):
  openFile = open(dirPath + "/" + fileToRead, "r")
  line = openFile.readline()

  while line:
    #print "file: " + fileToRead + ":" + line   
    words = shlex.split(line)
    tempStr = words[0] + "_" + words[1] + "_" +  words[2]
    #print fileToRead + ":" + tempStr + ":" + words[10]
    inDict[tempStr].append(words[10])

    line = openFile.readline()

def makeGraph(inDict):
  fig = p.figure()
  ax = fig.add_subplot(1,1,1)
  #x = [1,2,3]
  #y = [4,5,6]
  speeds = []
  nodeNames = []
  stdevs = []
  counters = []
  counter = 0

  for key, value in sorted((float(key), value) for key,value in inDict.iteritems() ):
    #if( counter > 20 ):
    #  break
    speeds.append(key)
    nodeName, stdev = value
    nodeNames.append(nodeName)
    stdevs.append(stdev)
    counters.append(counter)
    counter = counter + 1
    #print "%s: %s %s" % (key, stdev, nodeName)

  ax.bar(counters,speeds, yerr=stdevs)
  p.xticks(counters, nodeNames, rotation=90)
  p.show()

def is_float_try(str):
  try:
    float(str)
    return True
  except ValueError:
    return False

def printSorted( myDict ):
  for key, value in sorted((float(key), value) for key,value in myDict.iteritems() ):
    print "%s: %s" % (key, value)

#print "arg[1] " + sys.argv[1]
if( len(sys.argv) < 2 ):
  print "usage: heat_stdev.py <directory>"
  sys.exit

dirPath = sys.argv[1]

myDict = defaultdict(list)

dirList = os.listdir(dirPath)
for fileName in dirList:
  readFileIntoDict(dirPath, fileName, myDict)

#define the dictionary for mathplotlib
graphDict = defaultdict(list)

print "dumping the dictionary"
for key,dataList in myDict.items():
  dataIn = []
  for item in dataList:
    if( is_float_try(item)):
      dataIn.append(item)

  dataIn = np.array(dataIn)
  dataIn = dataIn.astype(float)
  std1 = np.std(dataIn)
  avg1 = np.mean(dataIn)
  graphDict[avg1] = [key, std1]

  # create the dictionary for the mathplotlib stuff

  #if( key[-2:] == "_/"):
  #  print key, '\t\t %3f %.4f' % (avg1,std1)
  #else:
  #  print key, '\t %3f %.4f' % (avg1,std1)
 
#print out the data in average sorted order
#printSorted( graphDict)
makeGraph( graphDict)

