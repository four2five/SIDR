#!/usr/bin/python
import getopt, sys, re, datetime, time, os,shutil
from datetime import datetime, date

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

  for line in data:
    matchObj = re.match(searchString, line)
    #print "line: ", line

    if matchObj:
      #print "matchObj: ",matchObj.group()
      if startDateTime == 0:
        #startTime = datetime.strptime(matchObj.group(2), "%H:%M:%S")
        #startDate = datetime.strptime(matchObj.group(1), "%Y-%m-%d")
        startDateTime = datetime.strptime(matchObj.group(1), "%Y-%m-%d %H:%M:%S")
      else:
        #endTime = datetime.strptime(matchObj.group(2), "%H:%M:%S")
        #endDate = datetime.strptime(matchObj.group(1), "%Y-%m-%d")
        endDateTime = datetime.strptime(matchObj.group(1), "%Y-%m-%d %H:%M:%S")

  #mytimeDelta = datetime.combine(endDate, endTime) - datetime.combine(startDate, startTime)
  #mytimeDelta = endDateTime - startDateTime
  #print "file: ",filename," start: ",startDateTime," end: ",endDateTime," delta: ", mytimeDelta
  print "file: ",filename," start: ",startDateTime," end: ",endDateTime

  return (startDateTime, endDateTime)

# prints out all the keywords and their values
def debugPrintResults(results, time):
  print "test ran in %s:%s" % (divmod(time,60))
  for k,v in results.iteritems():
    for entry in v:
      print k, "=>", entry

def parseDirectory(dirPath):
  print "parsing", dirPath

  # dirName should look something like this:
  # job_201104262343_0002
  # and it has subdirs that look like this
  # attempt_201104262343_0002_m_000005_0
  # attempt_201104262343_0002_r_000000_0

  dirList = os.listdir(dirPath)

  taskAttemptsToCleanup = []
  cleanupSearchString = "(attempt_\d+_\d+_[mr]_\d+_\d+)\.cleanup"

  for subDir in dirList:
    matchObj = re.match(cleanupSearchString, subDir)

    if matchObj:
      #print "adding ", matchObj.group(0)," and ", matchObj.group(1)
      taskAttemptsToCleanup.append(matchObj.group(0))
      taskAttemptsToCleanup.append(matchObj.group(1))

  for path in taskAttemptsToCleanup:
    pathToDelete = os.path.join(dirPath, path)
    #print dirPath + "/" + path
    print pathToDelete
    #os.unlink(pathToDelete)
    shutil.rmtree(pathToDelete)

  # now delete dulpicate directories. This occurs when a 
  # task and a copy of that task finish at the same time
  dirList = os.listdir(dirPath)

  taskAttemptsToCleanup = []
  previousShortString = ""
  currentString = ""
  duplicateSearchString = "(attempt_\d+_\d+_[mr]_\d+_)\d+"

  dirList.sort()

  for taskDir in dirList:
    #print "taskDir: ", taskDir

    matchObj = re.match(duplicateSearchString, taskDir)
    if matchObj:
      currentString = matchObj.group(0)
      currentShortString = matchObj.group(1)

      #only do this test if this is not the first path encountered
      if( previousShortString != ""):
        if( currentShortString == previousShortString):
          print "prev: " + previousShortString  + " ==  curr: " + currentShortString
          #delete the second instace
          taskAttemptsToCleanup.append(currentString)
          
        #else:
          #print "prev: " + previousShortString  + " != curr: " + currentShortString

      else:
        print "prevString == \"\""

      # always do this
      previousShortString = currentShortString
      #print "current short string: " + currentShortString

    else:
      print "no match object: ", taskDir

      #if( previousTaskString == taskDir):
      #  print previousTaskString + " == " + taskDir
      #  taskAttemptsToCleanup.append(taskDir)

  #print "paths to delete"

  for path in taskAttemptsToCleanup:
    pathToDelete = os.path.join(dirPath, path)
    print "\t"+pathToDelete
    shutil.rmtree(pathToDelete)

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
    if os.path.isdir(dirname):
      files = parseDirectory(dirname)
    else:
      print "specified directory (", dirname, ") is not a valid directory"
  else:
    print "a dirname(%s) was not specified. Poor form",  dirname
    exit

  #debugPrintResults(results, time)

if __name__ == "__main__":
  main()
