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
#key['util'] = 11
#key['wait'] = 9
debug = False

    #avg-cpu:  %user   %nice %system %iowait  %steal   %idle
        #          13.33    0.00    1.63    7.02    0.00   78.02
key2 = {}
key2['user'] = 1
key2['nice'] = 2
key2['system'] = 3
key2['iowait'] = 4
key2['steal'] = 5
key2['idle'] = 6

                

def usage():
    print "./getCPUStatsFromIOStats.py -d <dirname>"

def parseFile(filename):
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

    if (debug):
        print "delta", timeDelta

    # now, parse out all entries for each device in sd[bcd] (sda is the root drive and not used

    sdaSearchString = 'sda.*'
    sdaMatchObj = re.findall(sdaSearchString, data)
    i = 0
    sdaTotalWait = 0.0
    sdaTotalUtil = 0.0
    sdaAbove95 = 0 #track the number of times the util is >= 95
    sdaList = []

    if (debug):
        print "sda <time stamp>: <util> <wait>"

    for line in sdaMatchObj: 
        splits = line.split()
        #print "sda %d: %s %s" % (unixTime[i], splits[key['util']], splits[key['wait']])
        sdaList.append("sda time %d: util:%s wait:%s" % (unixTime[i], splits[key['util']], splits[key['wait']]))
        i = i + 1
        #  filter out unreasonable values, in this case a wait of > 20 seconds
        if ( float(splits[key['wait']]) < 20000 ):
            sdaTotalWait += float(splits[key['wait']])

        sdaTotalUtil += float(splits[key['util']])
        if ( float(splits[key['util']]) >= 95.0):
            sdaAbove95 = sdaAbove95 + 1

    if ( i > 0):
        sdaAvgWait = sdaTotalWait / i
        sdaAvgUtil = sdaTotalUtil / i
        returnI = i
    else:
        sdaAvgWait = 0
        sdaAvgUtil = 0
        returnI = 0

    if (debug):
        print "sda measurements %d avg wait %f avg util %f measurements above 95:%d" % \
        (i, (sdaTotalWait / i), (sdaTotalUtil/i), sdaAbove95)

    sdbSearchString = 'sdb.*'
    sdbMatchObj = re.findall(sdbSearchString, data)
    i = 0
    sdbTotalWait = 0.0
    sdbTotalUtil = 0.0
    sdbAbove95 = 0 #track the number of times the util is >= 95
    sdbList = []

    if (debug):
        print "sdb <time stamp>: <util> <wait>"

    for line in sdbMatchObj: 
        splits = line.split()
        #print "sda %d: %s %s" % (unixTime[i], splits[key['util']], splits[key['wait']])
        sdbList.append("sdb time %d: util:%s wait:%s" % (unixTime[i], splits[key['util']], splits[key['wait']]))
        i = i + 1
        #  filter out unreasonable values, in this case a wait of > 20 seconds
        if ( float(splits[key['wait']]) < 20000 ):
            sdbTotalWait += float(splits[key['wait']])

        sdbTotalUtil += float(splits[key['util']])
        if ( float(splits[key['util']]) >= 95.0):
            sdbAbove95 = sdbAbove95 + 1

    if ( i > 0):
        sdbAvgWait = sdbTotalWait / i
        sdbAvgUtil = sdbTotalUtil / i
        returnI = i
    else:
        sdbAvgWait = 0
        sdbAvgUtil = 0
        returnI = 0

    if (debug):
        print "sdb measurements %d avg wait %f avg util %f measurements above 95:%d" % (i, (sdbTotalWait / i), (sdbTotalUtil/i), sdbAbove95)
    sdcSearchString = 'sdc.*'
    sdcMatchObj = re.findall(sdcSearchString, data)
    i = 0
    sdcTotalWait = 0.0
    sdcTotalUtil = 0.0
    sdcAbove95 = 0 #track the number of times the util is >= 95
    sdcList = []

    if (debug):
        print "sdc <time stamp>: <util> <wait>"

    for line in sdcMatchObj: 
        splits = line.split()
        #print "sda %d: %s %s" % (unixTime[i], splits[key['util']], splits[key['wait']])
        sdcList.append("sdc time %d: util:%s wait:%s" % (unixTime[i], splits[key['util']], splits[key['wait']]))
        i = i + 1
        #  filter out unreasonable values, in this case a wait of > 20 seconds
        if ( float(splits[key['wait']]) < 20000 ):
            sdcTotalWait += float(splits[key['wait']])

        sdcTotalUtil += float(splits[key['util']])
        if ( float(splits[key['util']]) >= 95.0):
            sdcAbove95 = sdcAbove95 + 1

    if ( i > 0):
        sdcAvgWait = sdcTotalWait / i
        sdcAvgUtil = sdcTotalUtil / i
        returnI = i
    else:
        sdcAvgWait = 0
        sdcAvgUtil = 0
        returnI = 0

    if (debug):
        print "sdc measurements %d avg wait %f avg util %f measurements above 95:%d" % (i, (sdcTotalWait / i), (sdcTotalUtil/i), sdcAbove95)

    sddSearchString = 'sdd.*'
    sddMatchObj = re.findall(sddSearchString, data)
    i = 0
    sddTotalWait = 0.0
    sddTotalUtil = 0.0
    sddAbove95 = 0 #track the number of times the util is >= 95
    sddList = []

    if (debug):
        print "sdd <time stamp>: <util> <wait>"

    for line in sddMatchObj: 
        splits = line.split()
        #print "sda %d: %s %s" % (unixTime[i], splits[key['util']], splits[key['wait']])
        sddList.append("sdd time %d: util:%s wait:%s" % (unixTime[i], splits[key['util']], splits[key['wait']]))
        i = i + 1
        #  filter out unreasonable values, in this case a wait of > 20 seconds
        if ( float(splits[key['wait']]) < 20000 ):
            sddTotalWait += float(splits[key['wait']])

        sddTotalUtil += float(splits[key['util']])
        if ( float(splits[key['util']]) >= 95.0):
            sddAbove95 = sddAbove95 + 1

    if ( i > 0):
        sddAvgWait = sddTotalWait / i
        sddAvgUtil = sddTotalUtil / i
        returnI = i
    else:
        sddAvgWait = 0
        sddAvgUtil = 0
        returnI = 0

    if (debug):
        print "sdd measurements %d avg wait %f avg util %f measurements above 95:%d" % (i, (sddTotalWait / i), (sddTotalUtil/i), sddAbove95)

    md0SearchString = 'md0.*'
    md0MatchObj = re.findall(md0SearchString, data)
    i = 0
    md0TotalWait = 0.0
    md0TotalUtil = 0.0
    md0Above95 = 0 #track the number of times the util is >= 95
    md0List = []

    if (debug):
        print "md0 <time stamp>: <util> <wait>"

    for line in md0MatchObj: 
        splits = line.split()
        #print "sda %d: %s %s" % (unixTime[i], splits[key['util']], splits[key['wait']])
        md0List.append("md0 time %d: util:%s wait:%s" % (unixTime[i], splits[key['util']], splits[key['wait']]))
        i = i + 1
        #  filter out unreasonable values, in this case a wait of > 20 seconds
        if ( float(splits[key['wait']]) < 20000 ):
            md0TotalWait += float(splits[key['wait']])

        md0TotalUtil += float(splits[key['util']])
        if ( float(splits[key['util']]) >= 95.0):
            md0Above95 = md0Above95 + 1

    if ( i > 0):
        md0AvgWait = md0TotalWait / i
        md0AvgUtil = md0TotalUtil / i
        returnI = i
    else:
        md0AvgWait = 0
        md0AvgUtil = 0
        returnI = 0

    if (debug):
        print "md0 measurements %d avg wait %f avg util %f measurements above 95:%d" % (i, (md0TotalWait / i), (md0TotalUtil/i), md0Above95)

    searchString = "avg-cpu.*\n(\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+)"
    matchObj = re.findall(searchString, data)

    cpuList = []
    runningUser = 0
    runningIOWait = 0
    runningIdle = 0

    i = 0
    for entry in matchObj:
        #print "user:%f iowait:%f idle:%f" % ( float(entry[key2['user']]), float(entry[key2['iowait']]), float(entry[key2['idle']]) )
        cpuList.append("user:%f iowait:%f idle:%f" % ( float(entry[key2['user']]), float(entry[key2['iowait']]), float(entry[key2['idle']])) )
        runningUser += float(entry[key2['user']])
        runningIOWait += float(entry[key2['iowait']])
        runningIdle += float(entry[key2['idle']]) 
        i += 1
        #print "foentry[0]

    if ( i > 0):
        avgUser = runningUser / i
        avgIOWait = runningIOWait / i
        avgIdle = runningIdle / i
    else:
        avgUser = 0
        avgIOWait = 0
        avgIdle = 0

    i = 0
    for line in sdaList:
        if (debug):
            print sdaList[i], cpuList[i]
        #print cpuList[i]
        i += 1


    return (returnI, sdaAvgWait, sdaAvgUtil, sdaAbove95, sdbAvgWait, sdbAvgUtil, sdbAbove95,\
                     sdcAvgWait, sdcAvgUtil, sdcAbove95, sddAvgWait, sddAvgUtil, sddAbove95,\
                     md0AvgWait, md0AvgUtil, md0Above95, avgUser, avgIOWait, avgIdle)

# prints out all the keywords and their values
def debugPrintResults(results, time):
    print "test ran in %s:%s" % (divmod(time,60))
    for k,v in results.iteritems():
      for entry in v:
        print k, "=>", entry

def parseDirectory(dirPath, printHeaders):
    if (debug):
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
    runningSdaUtilVal = 0
    runningSdaWaitVal = 0
    runningSdaAbove95 = 0
    runningSdbUtilVal = 0
    runningSdbWaitVal = 0
    runningSdbAbove95 = 0
    runningSdcUtilVal = 0
    runningSdcWaitVal = 0
    runningSdcAbove95 = 0
    runningSddUtilVal = 0
    runningSddWaitVal = 0
    runningSddAbove95 = 0
    runningMd0UtilVal = 0
    runningMd0WaitVal = 0
    runningMd0Above95 = 0
    runningUser = 0
    runningIOWait = 0
    runningIdle = 0
    runningUnits = 0
    i = 0
    for fileName in filesToProcess:
        #return (returnI, avgWait, avgUtil, above95, avgUser, avgIOWait, avgIdle)
        tempUnits, tempSdaWaitVal, tempSdaUtilVal, tempSdaAbove95,\
        tempSdbWaitVal, tempSdbUtilVal, tempSdbAbove95,\
        tempSdcWaitVal, tempSdcUtilVal, tempSdcAbove95,\
        tempSddWaitVal, tempSddUtilVal, tempSddAbove95,\
        tempMd0WaitVal, tempMd0UtilVal, tempMd0Above95,\
        tempUser,\
        tempIOWait, tempIdle =  parseFile(dirPath + "/" + fileName)
        runningSdaUtilVal += tempSdaUtilVal
        runningSdaWaitVal += tempSdaWaitVal
        runningSdaAbove95 += tempSdaAbove95
        runningSdbUtilVal += tempSdbUtilVal
        runningSdbWaitVal += tempSdbWaitVal
        runningSdbAbove95 += tempSdbAbove95
        runningSdcUtilVal += tempSdcUtilVal
        runningSdcWaitVal += tempSdcWaitVal
        runningSdcAbove95 += tempSdcAbove95
        runningSddUtilVal += tempSddUtilVal
        runningSddWaitVal += tempSddWaitVal
        runningSddAbove95 += tempSddAbove95
        runningMd0UtilVal += tempMd0UtilVal
        runningMd0WaitVal += tempMd0WaitVal
        runningMd0Above95 += tempMd0Above95
        runningUnits += tempUnits
        runningUser += tempUser
        runningIOWait += tempIOWait
        runningIdle += tempIdle
        i = i + 1


    if ( i == 0 ):
        i = 1

    if (debug):
        print "total for all iostat files: %d units avg wait %f avg util %f above 95 %d avg user %f avg IOWait %f avg CPU Idle %f\n" %\
            (runningUnits, (runningWaitVal / i), (runningUtilVal / i), runningAbove95, (runningUser / i), (runningIOWait / i), (runningIdle / i) )

    if (printHeaders):
        print "sda avg_wait, sda_avg_util, sda_above_95, sdb_avg_wait, sdb_avg_util, sdb_above_95,\
               sdc_avg_wait, sdc_avg_util, sdc_above_95, sdd_avg_wait, sdd_avg_util, sdd_above_95,\
               md0_avg_wait, md0_avg_util, md0_above_95, sample count, \
        Average user %, Average IOWait %, Average Idle %"
    else:
        print "%f, %f, %d, %f, %f, %d,\
               %f, %f, %d, %f, %f, %d,\
               %f, %f, %d,\
               %d, %f, %f, %f" %\
            ((runningSdaWaitVal / i), (runningSdaUtilVal / i), runningSdaAbove95, 
            (runningSdbWaitVal / i), (runningSdbUtilVal / i), runningSdbAbove95, 
            (runningSdcWaitVal / i), (runningSdcUtilVal / i), runningSdcAbove95, 
            (runningSddWaitVal / i), (runningSddUtilVal / i), runningSddAbove95, 
            (runningMd0WaitVal / i), (runningMd0UtilVal / i), runningMd0Above95, 
            runningUnits, 
            (runningUser / i), (runningIOWait / i), (runningIdle / i) )


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

    for o, a in opts:
        if o in ("-f", "--filename"):
            filename = a
        elif o in ("-d", "--dirname"):
            dirname = a
        elif o in ("-v", "--headers"):
            printHeaders = True
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        else:
            assert False, "unhandled option"

    if filename != "" and dirname != "":
        print "cannot specify both dirname and filename"
        exit

    #results,time = parseFile(filename)
    if dirname != "":
        files = parseDirectory(dirname, printHeaders)
    elif filename != "":
        parseFile(filename, printHeaders)
    else:
        print "neither filename(%s) nor dirname(%s) were specified. Poor form" % (filename, dirname)
        exit

    #debugPrintResults(results, time)

if __name__ == "__main__":
    main()
