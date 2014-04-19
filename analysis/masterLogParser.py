#!/usr/bin/python
import getopt, sys, re, datetime, time, os
from subprocess import *


debug=False
#debug=True

def usage():
    print "./masterParser.py -d <dirname>"

def parseFile(filename):
    filename = filename.strip()

def parseTopDirectory(dirPath, printHeaders):
    dirList = os.listdir(dirPath)
    for dirFound in dirList:
        outputString = parseDirectory(dirPath + "/" + dirFound, printHeaders)

        matchObj = re.search("\d+-(.*)", dirFound)

        if ( matchObj ):
            testName = matchObj.group(1)
            if ( printHeaders ):
                print "testNumber, ", outputString
            else:
                print testName + " , " + outputString

    
def parseDirectory(dirPath, printHeaders):
    finalOutput = ""
    headerOutput = ""
    if (debug):
        print "parsing", dirPath

    filesToProcess = []
    searchString = "job.*_buck"
    genStatsFile = ""

    dirList = os.listdir(dirPath)
    for fileName in dirList:
        matchObj = re.match(searchString, fileName)

        #print fileName

        if matchObj:
            #print "MATCH: " + matchObj.group(0)
            #filesToProcess.append(matchObj.group(0))
            genStatsFile = dirPath + "/" + matchObj.group(0)


    #genStatsString = "./parseStatsFromXML.py -f " + dirPath + "/job*";
    #genStats = os.system(genStatsString);

    if ( printHeaders ) :
        p1 = Popen(["./parseStatsFromXML.py", "-v",  "-f",  genStatsFile], stdout=PIPE)
    else :
        p1 = Popen(["./parseStatsFromXML.py", "-f", genStatsFile], stdout=PIPE)

    genStatsResult = p1.communicate()[0]
    if (debug):
        print "genStats output: ", genStatsResult

    finalOutput = genStatsResult.rstrip()


    # now get the iostat info
    if (printHeaders):
        p1 = Popen(["./analyzeIOStatOutput.py", "-v", "-d", dirPath], stdout=PIPE)
    else:
        p1 = Popen(["./analyzeIOStatOutput.py", "-d", dirPath], stdout=PIPE)

    ioStatOutput = p1.communicate()[0]

    if (debug):
        print "iostat output: ", ioStatOutput

    finalOutput += " , , " + ioStatOutput.rstrip()

    # now get the cpu info

    if ( printHeaders):
        p1 = Popen(["./getCPUInfoFromIOStat.py", "-v", "-d", dirPath], stdout=PIPE)
    else:
        p1 = Popen(["./getCPUInfoFromIOStat.py","-d", dirPath], stdout=PIPE)

    cpuStatOutput = p1.communicate()[0]

    if (debug):
        print "cpustat output: ", cpuStatOutput

    finalOutput += ", " + cpuStatOutput.rstrip()


    return finalOutput

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "vhd:")
    except getopt.GetoptError, err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)

    dirname = ""
    printHeaders = False

    for o, a in opts:
        if o in ("-d", "--dirname"):
            dirname = a
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-v", "--Header"):
            printHeaders = True
        else:
            assert False, "unhandled option"

    #results,time = parseFile(filename)
    if dirname != "":
        totalOutput = parseTopDirectory(dirname, printHeaders)
    else:
        print "dirname(%s) was not specified. Poor form",dirname
        exit

    #print totalOutput

    #debugPrintResults(results, time)

if __name__ == "__main__":
    main()
