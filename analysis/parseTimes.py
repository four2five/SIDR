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
            print testName + " , " + outputString

    
# dirPath looks like <top dir>/1307135687-12
def parseSubDirectory(dirPath):
    output = ""
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


    #genStatsString = "./parseStatsFromXML.py -f " + dirPath + "/job*";
    #genStats = os.system(genStatsString);

    for dirName in filesToProcess:
      #print "process " + dirName 
      parseSubSubDirectory(dirName)

    return output

def parseSubSubDirectory(dirPath):
    
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
