#!/usr/bin/python
import getopt, sys, re, datetime, time

# tuples representing the key to look for and which one to read (multiple entries for each,
# corresponding to mapper, combiner and total (generally))
valuesToParse = [ "HDFS_BYTES_READ_LOCAL", "HDFS_BYTES_READ_REMOTE", "MAP_INPUT_RECORDS", 
                  "MAP_OUTPUT_RECORDS", "COMBINE_INPUT_RECORDS", "COMBINE_OUTPUT_RECORDS", 
                  "REDUCE_INPUT_RECORDS", "REDUCE_OUTPUT_RECORDS", "DATA_LOCAL_MAPS",
                  "RACK_LOCAL_MAPS", "REDUCE_SHUFFLE_BYTES", "FILE_BYTES_WRITTEN" 
                ]
# format for the entries is:
# (<key in Hadoop job output file>, <which column entry in the Hadoop output to read from>, <printable name for the value>)
valuesToParse2 = [ ("HDFS_BYTES_READ_LOCAL",2,"HDFS_BYTES_READ_LOCAL"), ("HDFS_BYTES_READ_REMOTE",2,"HDFS_BYTES_READ_REMOTE"), 
                   ("MAP_INPUT_RECORDS",1,"MAP_INPUT_RECORDS"), 
                  ("MAP_OUTPUT_RECORDS",1,"MAP_OUTPUT_RECORDS"), ("COMBINE_INPUT_RECORDS",1,"COMBINE_INPUT_RECORDS"), 
                  ("COMBINE_OUTPUT_RECORDS",1,"COMBINE_OUTPUT_RECORDS"), 
                  ("REDUCE_INPUT_RECORDS",2,"REDUCE_INPUT_RECORDS"), ("REDUCE_OUTPUT_RECORDS",2,"REDUCE_OUTPUT_RECORDS"), 
                  ("DATA_LOCAL_MAPS",1,"DATA_LOCAL_MAPS"),
                  ("RACK_LOCAL_MAPS",1,"RACK_LOCAL_MAPS"), ("REDUCE_SHUFFLE_BYTES",2,"REDUCE_SHUFFLE_BYTES"), 
                  ("FILE_BYTES_WRITTEN",2,"MAPPER_FILE_BYTES_WRITTEN"),
                  ("FILE_BYTES_WRITTEN",3,"REDUCER_FILE_BYTES_WRITTEN", ), 
                  ("SPILLED_RECORDS",2,"MAPPER_SPILLED_RECORDS"), 
                  ("SPILLED_RECORDS",3,"REDUCER_SPILLED_RECORDS")
                ]


def usage():
    print "./parseStatsFromXML.py -f <filename>"

def parseFile(filename):
    filename = filename.strip()
    #print "about to parse file", filename

    # read the data
    FILE = open(filename, "r")
    data = FILE.read() # make sure that you have enough memory (but test file is ~8 MB so we should be fine)
    FILE.close()

    # find the line for "JOB_FINISHED" and then read from that
    matchObj = re.search("\ \{\"type\":\"JOB_FINISHED\".*", data)

    if ( matchObj ):
        #print "line match", matchObj.group(0)
        lastLine = matchObj.group(0)
    else:
        print "no line starting with \" {\"type\":\"JOB_FINISHED\" found"

    # create a dictionary to store the values
    results = {}

    # first, pull out the launchTime and fininshTime. These are wonky, so we do them seperately
    searchString = '"finishTime":(\d+),'
    matchObj = re.search(searchString, lastLine)

    if (matchObj):
        #print "times"
        endTime = matchObj.group(1) 
        #print "giggles", time.localtime(long(endTime)/1000)
        #print time.time()
        #print datetime.datetime.fromtimestamp(time.time())
        #print "readable", datetime.utcfromtimestamp(matchObj.group(1))
        #print "readable", datetime.fromtimestamp(long(matchObj.group(1)))
        #print time.ctime(long(matchObj.group(1)))
        #print time.ctime(1302075415655)
        #print time.localtime(long(matchObj.group(1)))
        #utc = datetime.datetime.utcfromtimestamp(matchObj.group(1))
        #utcstr = utc.strftime("%Y/%m/%d %H:%M:%S.%f")
        #print utcstr

    searchString = '"launchTime":(\d+),'
    matchObj = re.search(searchString, data)

    if (matchObj):
        #print "times"
        startTime = matchObj.group(1) 

    deltaTime = (long(endTime) - long(startTime))/1000
    #print "start %s end %s delta %u or %s minutes and %s seconds" % (startTime, endTime, deltaTime, deltaTime/60, deltaTime%60)

    # pull out the entry for each entry in valuesToParse
    for key,column,printname in valuesToParse2:
        searchString = '{"name":"(%s)","displayName":"([\w -]+)","value":(\d+)' % key
        matchObj = re.findall(searchString, lastLine)

    	if ( matchObj ): 
            # first, make sure there's a key and an empty list as a value for this key
            if (printname in results.keys() ):
                pass
            else:
                results[printname] = []

            if column == 1: 
                #print "col 1 match ",matchObj[0][0], matchObj[0][2]
                #results[matchObj[0][0]] = matchObj[0][2]
                #results[matchObj[0][0]].append(matchObj[0][2])
                results[printname].append(matchObj[0][2])
            elif column == 2 and (len(matchObj) >= 2): 
                #print "col 2 match ",matchObj[1][0], matchObj[1][2]
                #results[matchObj[1][0]] = matchObj[1][2]
                #results[matchObj[1][0]].append(matchObj[1][2])
                results[printname].append(matchObj[1][2])
            elif column == 3 and (len(matchObj) >= 3): 
                #print "col 3 match ",matchObj[2][0], matchObj[2][2]
                #results[matchObj[2][0]] = matchObj[2][2]
                #results[matchObj[2][0]].append(matchObj[2][2])
                results[printname].append(matchObj[2][2])
            else:
                print "specified column %d was not found for key %s" % (column, key)
            #print "length of match for %s is %d" % (key, len(matchObj))
            #print "match: ", matchObj.group(0)
            #print "match: ", matchObj.group(1)
            #print "match: ", matchObj.group(3)
            #results[matchObj.group(1)] = matchObj.group(3)
    	else:
        	results[printname] = []
        	results[printname].append("0")  

    return results, deltaTime

# prints out all the keywords and their values
def debugPrintResults(results, time):
    print "test ran in %s:%s" % (divmod(time,60))
    for k,v in results.iteritems():
      for entry in v:
        print k, "=>", entry

# prints out the string in the format we want for the spread sheet that we use to manage the results
# going with csv lines for now
def formattedPrintResults(results, time, printHeader):
    # time, HDFS_LOCAL, HDFS_REMOTE, total_hdfs, fraction_local, map_input_records, map_output_records,
    # combine_input, combine_output, reduce_input_records, reduce_output, data_local_mappers, rack_local_mappers
    # killed map, total map, useful map, reduce shuffle bytes, map_fbw, reducer_fbw
    #print "time, HDFS_LOCAL, HDFS_REMOTE, total_hdfs, fraction_local, map_input_records, map_output_records,\
    #combine_input, combine_output, reduce_input_records, reduce_output, data_local_mappers, rack_local_mappers,\
    #killed map, total map, useful map, reduce shuffle bytes, map_fbw, reducer_fbw, total_fbw"
    if ( printHeader ) : 
        print " , , , , time,   HDFS_BYTES_READ_LOCAL, HDFS_BYTES_READ_REMOTE, TOTAL_GB_READ, \
        PERCENT_HDFS_BYTES_READ_LOCAL, MAP_INPUT_RECORDS, MAP_OUTPUT_RECORDS, , COMBINE_INPUT_RECORDS,\
        COMBINE_OUTPUT_RECORDS, REDUCE_INPUT_RECORDS, REDUCE_OUTPUT_RECORDS, DATA_LOCAL_MAPS,\
        RACK_LOCAL_MAPS, , , ,  REDUCE_SHUFFLE_BYTES, MAPPER_FBW,  REDUCER_FBW, TOTAL_FBW"
    else:
        print " , , , , %d, %d, %d, %3.4f, %1.8f, %d, %d,\
        , %d, %d, %d, %d, %d, %d,\
        , , , %d, %d, %d, %d" % \
        (long(time), long(results["HDFS_BYTES_READ_LOCAL"][0]), long(results["HDFS_BYTES_READ_REMOTE"][0]),\
        float(float(long(results["HDFS_BYTES_READ_LOCAL"][0]) + long(results["HDFS_BYTES_READ_REMOTE"][0])) / (1024 * pow(2,20))),\
        (float(results["HDFS_BYTES_READ_LOCAL"][0]) / (float(results["HDFS_BYTES_READ_LOCAL"][0]) + float(results["HDFS_BYTES_READ_REMOTE"][0]))),\
        long(results["MAP_INPUT_RECORDS"][0]),\
        long(results["MAP_OUTPUT_RECORDS"][0]), \
        #this is the end of the first line in the above comments
        long(results["COMBINE_INPUT_RECORDS"][0]), \
        long(results["COMBINE_OUTPUT_RECORDS"][0]), \
        long(results["REDUCE_INPUT_RECORDS"][0]), \
        long(results["REDUCE_OUTPUT_RECORDS"][0]), \
        long(results["DATA_LOCAL_MAPS"][0]), \
        long(results["RACK_LOCAL_MAPS"][0]), \
        # empty entry for no killed/total/useful jobs info
        long(results["REDUCE_SHUFFLE_BYTES"][0]), \
        long(results["MAPPER_FILE_BYTES_WRITTEN"][0]), \
        long(results["REDUCER_FILE_BYTES_WRITTEN"][0]), \
        (long(results["REDUCER_FILE_BYTES_WRITTEN"][0]) +\
        long(results["MAPPER_FILE_BYTES_WRITTEN"][0]))
        )
    # -jbuck

#note, the number of fields output needs to be kept in lockstep with formattedPrintResults()
def emptyPrint():
    print " , , , , %d, %d, %d, %3.4f, %1.8f, %d, %d,\
    , %d, %d, %d, %d, %d, %d,\
    , , , %d, %d, %d, %d" % \
    (0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "vh:f:")
    except getopt.GetoptError, err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)

    filename = ""
    printHeader = False

    for o, a in opts:
        if o in ("-f", "--filename"):
            filename = a
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-v", "-Header"):
            printHeader = True
        else:
            assert False, "unhandled option"

    if ( filename != "" ):
      results,time = parseFile(filename)
      formattedPrintResults(results, time, printHeader)
    else:
        emptyPrint()
    #debugPrintResults(results, time)

if __name__ == "__main__":
    main()
