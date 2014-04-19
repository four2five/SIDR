#!/usr/bin/python
import getopt, sys, re, datetime, time, os

#Device:         rrqm/s   wrqm/s     r/s     w/s    rkB/s    wkB/s avgrq-sz avgqu-sz   await  svctm  %util
# define a handy key for pulling the right values out of the iostat output when it's parsed
key = {}
key['rrqms'] = 1
key['wrqms'] = 2
key['rs'] = 3
key['ws'] = 4
key['rkBs'] = 5
key['wkBs'] = 6
key['avgrq'] = 7
key['avgqu'] = 8
key['wait'] = 9
key['svctm'] = 10
key['util'] = 11

globalDebug = False

                

def usage():
    print "./analyzeIOStatOutput.py -f <filename>"

def parseFile(filename, printHeaders):
    filename = filename.strip()

    # read the data
    FILE = open(filename, "r")
    data = FILE.read() # make sure that you have enough memory (but test file is ~8 MB so we should be fine)
    FILE.close()

    # this is what an example line looks like
    #Device:         rrqm/s   wrqm/s     r/s     w/s    rkB/s    wkB/s avgrq-sz avgqu-sz   await  svctm  %util

    # let's sort out the time delta
    searchString = "((\d+)\/(\d+)\/(\d\d\d\d) (\d+)\:(\d+)\:(\d+) (AM|PM))" 
    matchObj = re.findall(searchString, data)

    secondsPerSample = 1;

    unixTime = []
    for entry in matchObj:
        myTime = time.strptime(entry[0], "%m/%d/%Y %H:%M:%S %p")
        unixTime.append(time.mktime(myTime))


    #if ( matchObj ): 
        #print "time 1", matchObj[0]
        #print "time 1", matchObj[0][0]
        #print "time 2", matchObj[1]
        #print "time 2", matchObj[1][0]

        # see if the hours are different, if so, increment
        #if ( matchObj[0][6] == "PM"):
        #    dateString = "%s %s %s %d %s %s" %\
        #                (matchObj[0][0], matchObj[0][1], matchObj[0][2],\
       #                 (int(matchObj[0][3]) + 12), matchObj[0][4], matchObj[0][5])
        #else:
        #    dateString = "%s %s %s %d %s %s" %\
        #                (matchObj[0][0], matchObj[0][1], matchObj[0][2],\
        #                matchObj[0][3], matchObj[0][4], matchObj[0][5])
        # 04/05/2011 06:28:53 PM
        #myTime = time.strptime(matchObj[0][0], "%d/%m/%Y %H:%M:%S %p")
        #print "unix time 1", time.mktime(myTime)
        #myTime2 = time.strptime(matchObj[1][0], "%d/%m/%Y %H:%M:%S %p")
        #print "unix time 2", time.mktime(myTime2)

    if ( len(unixTime) >= 2 ):
        timeDelta = (unixTime[1] - unixTime[0])
    else:
        timeDelta = 10
    
    if ( globalDebug):
        print "delta", timeDelta

    # now, parse out all entries for each device in sd[abcd] 

    sdaSearchString = 'sda.*'
    sdaMatchObj = re.findall(sdaSearchString, data)
    i = 0
    sdaTotalkBRead = 0
    for line in sdaMatchObj: 
        splits = line.split()
        #key['rkBs'] = 5
        #key['wkBs'] = 6
        #print "sdb %d: %s %s %s" % (unixTime[i], splits[key['rs']], splits[key['ws']], splits[key['svctm']])
        #print "sdb %d: %f %f %f" % (unixTime[i], float(splits[key['rkBs']]) * timeDelta, float(splits[key['wkBs']]) * timeDelta,\
        #                           (float(splits[key['rkBs']]) * timeDelta) + (float(splits[key['wkBs']]) * timeDelta) )
        i = i + 1
        sdaTotalkBRead = sdaTotalkBRead + (float(splits[key['rkBs']]) * timeDelta) + (float(splits[key['wkBs']]) * timeDelta)
    if (globalDebug):
        print "total kB read sda", sdaTotalkBRead

    sdbSearchString = 'sdb.*'
    sdbMatchObj = re.findall(sdbSearchString, data)
    i = 0
    sdbTotalkBRead = 0
    for line in sdbMatchObj: 
        splits = line.split()
        #key['rkBs'] = 5
        #key['wkBs'] = 6
        #print "sdb %d: %s %s %s" % (unixTime[i], splits[key['rs']], splits[key['ws']], splits[key['svctm']])
        #print "sdb %d: %f %f %f" % (unixTime[i], float(splits[key['rkBs']]) * timeDelta, float(splits[key['wkBs']]) * timeDelta,\
        #                           (float(splits[key['rkBs']]) * timeDelta) + (float(splits[key['wkBs']]) * timeDelta) )
        i = i + 1
        sdbTotalkBRead = sdbTotalkBRead + (float(splits[key['rkBs']]) * timeDelta) + (float(splits[key['wkBs']]) * timeDelta)
    if (globalDebug):
        print "total kB read sdb", sdbTotalkBRead

    sdcSearchString = 'sdc.*'
    sdcMatchObj = re.findall(sdcSearchString, data)
    i = 0
    sdcTotalkBRead = 0
    for line in sdcMatchObj: 
        splits = line.split()
        #print "sdc %d: %s %s %s" % (unixTime[i], splits[key['rs']], splits[key['ws']], splits[key['svctm']])
        #print "sdb %d: %f %f %f" % (unixTime[i], float(splits[key['rkBs']]) * timeDelta, float(splits[key['wkBs']]) * timeDelta,\
        #                           (float(splits[key['rkBs']]) * timeDelta) + (float(splits[key['wkBs']]) * timeDelta) )
        i = i + 1
        sdcTotalkBRead = sdcTotalkBRead + (float(splits[key['rkBs']]) * timeDelta) + (float(splits[key['wkBs']]) * timeDelta)

    if (globalDebug):
        print "total kB read sdc", sdcTotalkBRead

    sddSearchString = 'sdd.*'
    sddMatchObj = re.findall(sddSearchString, data)
    #print "total time found: %d total measurements: %d" % ( len(matchObj), len(sdaMatchObj) )
    #Device:         rrqm/s   wrqm/s     r/s     w/s    rkB/s    wkB/s avgrq-sz avgqu-sz   await  svctm  %util
    i = 0
    sddTotalkBRead = 0
    for line in sddMatchObj: 
        splits = line.split()
        #print "%d:%s" % (i,splits)
        #key['ws'] = 4
        #print "sdd %d: %s %s %s" % (unixTime[i], splits[key['rs']], splits[key['ws']], splits[key['svctm']])
        #print "concurency %s" % ( ( float(splits[key['rs']]) + float(splits[key['ws']])) * ( float(splits[key['svctm']])/1000))  
        #print "sdb %d: %f %f %f" % (unixTime[i], float(splits[key['rkBs']]) * timeDelta, float(splits[key['wkBs']]) * timeDelta,\
        #                           (float(splits[key['rkBs']]) * timeDelta) + (float(splits[key['wkBs']]) * timeDelta) )
        i = i + 1
        sddTotalkBRead = sddTotalkBRead + (float(splits[key['rkBs']]) * timeDelta) + (float(splits[key['wkBs']]) * timeDelta)

    if (globalDebug):
        print "total kB read sdd", sddTotalkBRead

    md0SearchString = 'md0.*'
    md0MatchObj = re.findall(md0SearchString, data)
    #print "total time found: %d total measurements: %d" % ( len(matchObj), len(sdaMatchObj) )
    #Device:         rrqm/s   wrqm/s     r/s     w/s    rkB/s    wkB/s avgrq-sz avgqu-sz   await  svctm  %util
    i = 0
    md0TotalkBRead = 0
    for line in md0MatchObj: 
        splits = line.split()
        #print "%d:%s" % (i,splits)
        #key['ws'] = 4
        #print "sdd %d: %s %s %s" % (unixTime[i], splits[key['rs']], splits[key['ws']], splits[key['svctm']])
        #print "concurency %s" % ( ( float(splits[key['rs']]) + float(splits[key['ws']])) * ( float(splits[key['svctm']])/1000))  
        #print "sdb %d: %f %f %f" % (unixTime[i], float(splits[key['rkBs']]) * timeDelta, float(splits[key['wkBs']]) * timeDelta,\
        #                           (float(splits[key['rkBs']]) * timeDelta) + (float(splits[key['wkBs']]) * timeDelta) )
        i = i + 1
        md0TotalkBRead = md0TotalkBRead + (float(splits[key['rkBs']]) * timeDelta) + (float(splits[key['wkBs']]) * timeDelta)
    if ( globalDebug):
        print "total kB read md0", md0TotalkBRead


    if ( globalDebug) :
        print "total for all devices on host", (sdbTotalkBRead + sdcTotalkBRead + sddTotalkBRead), " raid ", md0TotalkBRead

    return sdaTotalkBRead, sdbTotalkBRead, sdcTotalkBRead, sddTotalkBRead, md0TotalkBRead

# prints out all the keywords and their values
def debugPrintResults(results, time):
    print "test ran in %s:%s" % (divmod(time,60))
    for k,v in results.iteritems():
      for entry in v:
        print k, "=>", entry

def parseDirectory(dirPath, printHeaders):
    if ( globalDebug ):
        print "parsing", dirPath

    filesToProcess = []
    searchString = "iostat\.(\w+)\.log"

    dirList = os.listdir(dirPath)
    for fileName in dirList:
        matchObj = re.match(searchString, fileName)

        if matchObj:
            #print matchObj.group(0)
            filesToProcess.append(matchObj.group(0))

    # now, process each file
    runningSdaTotal = 0
    runningSdbTotal = 0
    runningSdcTotal = 0
    runningSddTotal = 0
    runningRAID = 0

    for fileName in filesToProcess:
        tempSda, tempSdb, tempSdc, tempSdd, tempRAID = parseFile(dirPath + "/" + fileName, False)
        runningSdaTotal = runningSdaTotal + tempSda
        runningSdbTotal = runningSdbTotal + tempSdb
        runningSdcTotal = runningSdcTotal + tempSdc
        runningSddTotal = runningSddTotal + tempSdd
        runningRAID = runningRAID + tempRAID
        #runningTotal = runningTotal + parseFile(dirPath + "/" + fileName)

    if (printHeaders):
        print "sda, sdb, sdc, sdd, md0 "
    else:
        if ( globalDebug):
            pass
            #print "total for all iostat files", runningTotal, " for raid ", runningRAID
        else:
            print runningSdaTotal, ",", runningSdbTotal, ",", runningSdcTotal, ",",\
            runningSddTotal, "," , runningRAID



def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "vhf:d:")
    except getopt.GetoptError, err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)

    filename = ""
    dirname = ""
    printHeaders = False
    total = 0
    RAID = 0

    for o, a in opts:
        if o in ("-f", "--filename"):
            filename = a
        elif o in ("-d", "--dirname"):
            dirname = a
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-v", "--headers"):
            printHeaders = True
        else:
            assert False, "unhandled option"

    if filename != "" and dirname != "":
        print "cannot specify both dirname and filename"
        exit

    #results,time = parseFile(filename)
    if dirname != "":
        files = parseDirectory(dirname, printHeaders)
    elif filename != "":
        total, RAID = parseFile(filename, printHeaders)
        if (printHeaders):
            print "sd*total, md0 total"
        else:
            print "", total, " , ", RAID
    else:
        print "neither filename(%s) nor dirname(%s) were specified. Poor form" % (filename, dirname)
        exit

    #debugPrintResults(results, time)

if __name__ == "__main__":
    main()
