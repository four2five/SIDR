#!/usr/bin/python
import getopt, sys, re, datetime, time, os
from subprocess import *


debug=False
#debug=True

def usage():
    print "./parseTimes.py -d <dirname>"

def parseTopDirectory(dirPath):
    dirList = os.listdir(dirPath)

    # this should be top_dir/1307135687-12
    for dirFound in dirList:
        print "searching "  + (dirPath + "/" + dirFound)
        outputString = parseSubDirectory(dirPath + "/" + dirFound)

        matchObj = re.search("\d+-(.*)", dirFound)

        if ( matchObj ):
            testName = matchObj.group(1)
            #print testName + " , " + outputString

    
# dirPath looks like <top dir>/1307135687-12
def parseSubDirectory(dirPath):
    if (debug):
        print "parsing", dirPath

    filesToProcess = []
    searchString = "attempt.*_\d"

    dirList = os.listdir(dirPath)
    for dirName in dirList:
        matchObj = re.match(searchString, dirName)

        #print fileName

        if matchObj:
            #print "MATCH: " + matchObj.group(0)
            #filesToProcess.append(matchObj.group(0))
            filesToProcess.append( dirPath + "/" + matchObj.group(0) )


    tempCount = 0
    runningTotal = 0
    tempList = []
    for fileName in filesToProcess:
        #print "process " + dirName 
        tempList = parseFiles(fileName)
        #print "\tin parseSubDirectory, %s" % tempList
        for result in tempList:
            #print "time: %s" % result
            runningTotal += int(result) 
            tempCount = tempCount + 1

    if ( tempCount == 0):
        avg = 0
    else:
        avg = runningTotal / tempCount

    print "dir %s avg spill time %d over %d spills" %\
    (dirPath, avg, tempCount)



def parseFiles(filePath):
  FILE = open(filePath + "/syslog", "r")
  data = FILE.read()
  FILE.close()

  # remember, there could be multiple spills, so take that into account 
  matchObj = re.findall(".*Finished spill \d+ took (\d+) ms.*", data)

  #if len(matchObj) > 0:
  #  numEntries = len(matchObj)
  # print "file %s has %d entries" % (filePath, numEntries)
  #else:
  #  print "file %s has no keys" % (filePath)

  if len(matchObj) > 0:
    return matchObj 
  else:
    #print "No spill for file %s" % filePath
    emptyList = []
    return emptyList
  
    
    
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
            dirname = a
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        else:
            assert False, "unhandled option"

    if dirname != "":
        totalOutput = parseTopDirectory(dirname)
    else:
        print "dirname(%s) was not specified. Poor form",dirname
        exit

    #print totalOutput

    #debugPrintResults(results, time)

if __name__ == "__main__":
    main()
