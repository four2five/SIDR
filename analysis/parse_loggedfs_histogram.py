#!/usr/bin/python
import getopt, sys, re, time, datetime
import numpy as np
import matplotlib
#matplotlib.use('Agg')
import matplotlib.pyplot as plt
import statsmodels.tools as sm


# global vars
myFigSize = (9.5,6)  # seeks to scale well for different paper formats
figureLabelList = []
figureLabelList.append("SciHadoop")
figureLabelList.append("SIDR")


##################
# 
# This script parses the output from loggedfs
# and bins all reads and writes into per-second
# buckets. It then calculates the number of 
# seeks per second. 
##################

def usage():
  print "./parse_loggedfs.py <file to parse>[,<another file to parse>]"


def parseFile(fileToParse):
  # dict to hold the per-second data 
  bucketedData = dict()

  # build the regex for filtering out logged messages that we don't want
  searchString = "(\d\d:\d\d:\d\d) .+ (bytes read from|bytes written to) ([a-zA-Z0-9_\-\.\/]+)"
  #searchString = "(\d\d:\d\d:\d\d) .+ (bytes read from|bytes written to) ([\/]+)"
  ins = open(fileToParse, "r")
  
  # for sanity checking
  linesParsed = 0
  validLines = 0

  for line in ins:
    linesParsed = linesParsed + 1
    # we only want to store reads and writes, filter out the rest
    matchObj = re.match(searchString, line)

    if matchObj:
      validLines = validLines + 1
      #print "1: ", matchObj.group(1), " 2: ", matchObj.group(2), " 3: ", matchObj.group(3), " line: ", line
      timeStamp = matchObj.group(1)
      #timeStamp = time.mktime(time.strptime(timeStamp, "%H:%M:%S").timetuple())
      #timeStamp = time.mktime(time.strptime(timeStamp, "%H:%M:%S"))
      #timeStamp = time.strptime(timeStamp, "%H:%M:%S")
      action = matchObj.group(2)
      fileAccessed = matchObj.group(3)
      
      # add a list for this timestamp if it's not already present
      if (timeStamp not in bucketedData):
        bucketedData[timeStamp] = []

      bucketedData[timeStamp].append(action + ":" + fileAccessed)


  # sanity check the results of parsing
  print "lines: ", linesParsed, " num entries in dict: ", len(bucketedData), " total valid lines: ", validLines

  # Now, for each bucket, sort how many seeks would have occurred. 
  # We define a seek as an access to a file that is not the same as 
  # the previous access. 
  # TODO: update this to account for seeks within the same file

  retData = dict()
  for key in sorted(bucketedData.iterkeys()):
    #print " ", key, "\t", len(bucketedData[key])
    lastFileAccessed = ""
    lastAction = ""
    seekCount = 0
    
    accesses = bucketedData[key]

    for entry in accesses:
      action, fileAccessed = entry.split(":")

      # debug a single buckets entries
      #if key == "00:44:48":
      #  print "\t\t", action, " $$$ ", fileAccessed

      # A seek is avoided only if subsequent accesses are taking the same 
      # action on the same file
      if (lastAction == action) and (lastFileAccessed == fileAccessed):
        pass
      else:
        seekCount = seekCount + 1

      # update the lastAction and lastFileAccessed for the next pass
      lastAction = action
      lastFileAccessed = fileAccessed

    #print "key: ", key, " total: ", len(accesses), " seeks: ", seekCount
    retData[key] = seekCount

  print "retData, keys: ", len(retData)
  return retData

def fillInMissingTimepoints(bucketedSeeks):
  sortedKeys = sorted(bucketedSeeks.keys())
  firstKey = sortedKeys[0]
  lastKey = sortedKeys[-1]

  startOfTime =  time.mktime(time.strptime("00:00:00", "%H:%M:%S"))
  minTimestamp = time.mktime(time.strptime(firstKey, "%H:%M:%S"))
  maxTimestamp = time.mktime(time.strptime(lastKey, "%H:%M:%S"))

  print "counting from ", firstKey, " to ", lastKey
  print "counting from ", minTimestamp, " to ", maxTimestamp

  counter = minTimestamp
  while counter < maxTimestamp:
    counterAsTimestamp = time.strftime("%H:%M:%S", time.localtime(counter))
    # if a given timestamp doesn't exist, insert it with zero seeks
    if counterAsTimestamp not in bucketedSeeks:
      #print "counter : ", time.strftime("%H:%M:%S", counter)
      #print "counter : ", counterAsTimestamp
      #bucketedSeeks[time.strftime("%H:%M:%S", counter)] = [0]
      #bucketedSeeks[time.strftime("%H:%M:%S", time.localtime(counter))] = 0
      bucketedSeeks[counterAsTimestamp] = 0

    # debug
    #print counterAsTimestamp, " : ", bucketedSeeks[counterAsTimestamp] 
    counter = counter + 1

  return bucketedSeeks

# from http://wiki.scipy.org/Cookbook/SignalSmooth  
def smooth(x, window_len=10, window='hanning'):
    """smooth the data using a window with requested size.
    
    This method is based on the convolution of a scaled window with the signal.
    The signal is prepared by introducing reflected copies of the signal 
    (with the window size) in both ends so that transient parts are minimized
    in the begining and end part of the output signal.
    
    input:
        x: the input signal 
        window_len: the dimension of the smoothing window
        window: the type of window from 'flat', 'hanning', 'hamming', 'bartlett', 'blackman'
            flat window will produce a moving average smoothing.

    output:
        the smoothed signal
        
    example:


    import numpy as np    
    t = np.linspace(-2,2,0.1)
    x = np.sin(t)+np.random.randn(len(t))*0.1
    y = smooth(x)
    
    see also: 
    
    numpy.hanning, numpy.hamming, numpy.bartlett, numpy.blackman, numpy.convolve
    scipy.signal.lfilter
 
    TODO: the window parameter could be the window itself if an array instead of a string   
    """

    if x.ndim != 1:
        raise ValueError, "smooth only accepts 1 dimension arrays."

    if x.size < window_len:
        raise ValueError, "Input vector needs to be bigger than window size."

    if window_len < 3:
        return x

    if not window in ['flat', 'hanning', 'hamming', 'bartlett', 'blackman']:
        raise ValueError, "Window is on of 'flat', 'hanning', 'hamming', 'bartlett', 'blackman'"

    s=np.r_[2*x[0]-x[window_len:1:-1], x, 2*x[-1]-x[-1:-window_len:-1]]
    #print(len(s))

    
    if window == 'flat': #moving average
        w = np.ones(window_len,'d')
    else:
        w = getattr(np, window)(window_len)
    y = np.convolve(w/w.sum(), s, mode='same')
    return y[window_len-1:-window_len+1]

# Input should be a dict where the key is a string
# representing a second in time from the parsed log file
# and the value is the number of seeks observed in that 
# second
def presentData(seeksBySecond, ax):
  sortedKeys = sorted(seeksBySecond.keys())
  sortedValues = []
  sortedKeysAsFloats = []

  # "beginning" of time
  startOfTime =  time.mktime(time.strptime("00:00:00", "%H:%M:%S"))
  # our first observed measurement
  relStartOfTime = time.mktime(time.strptime(sortedKeys[0], "%H:%M:%S"))

  for key in sortedKeys:
    #print "\t\t", key
    sortedValues.append(seeksBySecond[key])
    #thisRelTime =  abs(startOfTime - time.mktime(time.strptime(key, "%H:%M:%S")))
    thisRelTime =  abs(relStartOfTime - time.mktime(time.strptime(key, "%H:%M:%S")))
    # let's graph relative to the first write
    #print "\t\t", (thisRelTime - relStartOfTime)
    #print "\t\t", thisRelTime 
    sortedKeysAsFloats.append(thisRelTime) 

  minKeyString = sortedKeys[0]
  minTimestamp = time.mktime(time.strptime(minKeyString, "%H:%M:%S"))
  maxKeyString = sortedKeys[-1]
  maxTimestamp = time.mktime(time.strptime(maxKeyString, "%H:%M:%S"))
  startOfTime =  time.mktime(time.strptime("00:00:00", "%H:%M:%S"))
  #minKeyTime = time.strptime(minKeyString, "%H:%M:%S")
  #timeStamp = time.mktime(time.strptime(timeStamp, "%H:%M:%S"))
  #minDateTime = datetime.date((1,1,1), minKeyTime)
  #maxKeyTime = time.strptime(maxKeyString, "%H:%M:%S")
  #maxDateTime = datetime.date((1,1,1), maxKeyTime)
  timeDelta = maxTimestamp - minTimestamp

  print "min: ", minKeyString, " max: ", maxKeyString, " delta: ", timeDelta
  print "min: ", minTimestamp, " max: ", maxTimestamp, " delta: ", (maxTimestamp - minTimestamp)
  print "min: ", sortedKeysAsFloats[0], " max: ", sortedKeysAsFloats[-1],\
        " delta: ", (sortedKeysAsFloats[-1] - sortedKeysAsFloats[0])
  print "startOfTime: ", startOfTime, " relMin: ", abs(startOfTime - minTimestamp),\
        " relMax: ", abs(startOfTime - maxTimestamp)
  print "x len: ", len(sortedKeysAsFloats), " y len: ", len(sortedValues) 
  #x = np.linspace(minTimestamp, maxTimestamp)
  #ax.plot(sortedKeysAsFloats, sortedValues)

  #smoothedValues = smooth(np.array(sortedValues), window_len=10, window='flat')
  ecdf = sm.tools.ECDF(sortedValues)
  x = np.linspace(min(sortedValues), max(sortedValues))
  y = ecdf(x)
  #ax.plot(sortedKeysAsFloats, smoothedValues)
  #ax.plot(x, y)
  ax.hist(sortedValues, histtype='step')
  ax.set_title("Seeks per second histogram")
  ax.set_xlabel("Seeks per second")
  ax.set_ylabel("Frequency")
  #ax.set_yscale('log')
  ax.grid()

def main():
  try:
    opts, args = getopt.getopt(sys.argv[1:], "h")
  except getopt.GetoptError, err:
    print str(err)
    usage()
    sys.exit(2)

  for o, a in opts:
    if o in ("-h", "--help"):
      usage()
      sys.exit()
    else:
      assert False, "unhandled option"

  if len(args) != 1:
    print "error: there should be one arg and it should be the file to parse"
    sys.exit(2)
  else:
    print "arg: ", args[0]
    filesToParse = args[0].split(",")

    # create the larger plot
    fig = plt.figure(figsize=myFigSize)
    ax = fig.add_subplot(111)
    plotLabels = []
    foo = 0

    for filename in filesToParse:

      # pull out the per-second seek rates
      bucketedSeeks = parseFile(filename)

      preFillCount = len(bucketedSeeks)
      print "pre-fillin len ", preFillCount
      # fill in missing seconds so that the graph is accurate
      bucketedSeeks = fillInMissingTimepoints(bucketedSeeks)
      postFillCount = len(bucketedSeeks)
      print "post-fillin len ", postFillCount


      # now present it in some meaningful manner
      presentData(bucketedSeeks, ax)  
      plotLabels.append(figureLabelList[foo])
      print "adding graph for ", figureLabelList[foo]
      foo = foo + 1

    plt.legend(plotLabels, 'upper right')
    print "calling show()"
    plt.show()

if __name__ == "__main__":
  main()
