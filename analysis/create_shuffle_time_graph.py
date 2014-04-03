#!/usr/bin/python
import getopt, sys, re, time, datetime, itertools
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import FixedLocator
import statsmodels.tools as sm


# global vars
myFigSize = (9.5,6)  # seeks to scale well for different paper formats
#figureLabelList = []
#figureLabelList.append("node 1")
#figureLabelList.append("node 2")

lineColorList = []
lineColorList.append("r")
lineColorList.append("b")
myTimesBins = [0, 100, 250, 500, 1000, 1500, 2000, 3000, 4000, 5000, 10000]
myTimesTicksBins = [100, 500, 1000, 2000, 3000, 4000, 5000, 10000]


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
  print "parsing ", fileToParse

  # order of columns
  # rrqm/s  wrqm/s  r/s w/s rmB/s wmB/s avgrq-sz  avgqu-sz await r_await w_await svctm %util
  #searchString = "(sd[a-d]1)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)"
  searchString = "(sda1)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)"
  ins = open(fileToParse, "r")
  
  # for sanity checking
  linesParsed = 0
  validLines = 0
  tmpData1 = []
  tmpData2 = []
  tmpData3 = []

  retArray = []

  for line in ins:
    linesParsed = linesParsed + 1
    # we only want to store reads and writes, filter out the rest
    matchObj = re.match(searchString, line)

    if matchObj:
      validLines = validLines + 1
      #print "1: ", matchObj.group(1), "2: ", matchObj.group(2), " 14: ", matchObj.group(14)
      tmpData1.append(float(matchObj.group(6)))
      tmpData2.append(float(matchObj.group(7)))
      tmpData3.append(float(matchObj.group(14)))

  retArray.append(tmpData1)
  retArray.append(tmpData2)
  retArray.append(tmpData3)

  return retArray


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

# input is two arrays, representing time and size for the shuffles from a given
# MapReduce job
def presentData(data1, data2, plotName, ax, plotLabels1, datasetNum, lns1):

  #myTimesBins = [0, 100, 250, 500, 1000, 1500, 2000, 3000, 4000, 5000, 10000]
  #ax.hist(np.array(data1), 10)
  #ax.hist(np.array(data1), myTimesBins, alpha=0.5, normed=1)
  ax.hist(np.array(data1), myTimesBins, alpha=0.5)
  ax.set_xticks(myTimesBins)

  #lns1.append(line)
  print "adding plotLabel ", plotName
  plotLabels1.append(plotName)

  #plotLabels.append(figureLabelList[foo])
  #ax.hist(sortedValues, histtype='step')
  #ax.hist(np.array(data1), histtype='step', label=perExpLabelList[0], color=lineColorList[datasetNum])
  #ax.hist(np.array(data2), histtype='step', label=perExpLabelList[1], color=lineColorList[datasetNum])
  #ax.set_title("Seeks per second histogram")

  #ax.set_yscale('log')
  ax.grid()

def main():
  try:
    opts, args = getopt.getopt(sys.argv[1:], "hp:f")
  except getopt.GetoptError, err:
    print str(err)
    usage()
    sys.exit(2)

  # initialize this to something sensible
  parsedOutputFilename = None
  formattedInputFile = False

  for o, a in opts:
    if o in ("-h", "--help"):
      usage()
      sys.exit()
    elif o in ("-p", "--parse"):
      parsedOutputFilename = a
    elif o in ("-f", "--formatted"):
      formattedInputFile = True
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
    #ax2 = ax.twinx()
    plotLabels1 = []
    plotLabels2 = []
    lns1 = []
    lns2 = []
    datasetNum = 0

    for filename in filesToParse:
      data1 = []
      data2 = []

      # the input was already formatted, just read it in
      if formattedInputFile:
        ins = open(filename, "r")
        plotName = None
  
        for line in ins:
          # first line is the title of the graph
          if plotName is None:
            plotName = line
          else: 
            d1, d2 = line.rstrip().split("$")
            #print key, " $ ", count
            #bucketedSeeks[key.rstrip()] = int(count.rstrip())
            data1.append(long(d1) / 1000000)
            data2.append(long(d2))

        ins.close()

      # else, parse the raw log files
      else:
        # pull out the per-second seek rates
        (data1, data2) = parseFile(filename)

      # now present it in some meaningful manner
      #presentData(data1, data2, plotName, ax, ax2, plotLabels1, plotLabels2, datasetNum, lns1, lns2)  
      presentData(data1, data2, plotName, ax, plotLabels1, datasetNum, lns1)  
      #plotLabels.append(figureLabelList[datasetNum])
      #print "adding graph for ", figureLabelList[datasetNum]
      print "adding graph for ", plotName
      datasetNum = datasetNum + 1

    ax.set_xlabel("Shuffle Time (milliseconds)")
    ax.set_ylabel("Reduce Tasks per Bin")
    #ax.set_ylim(top=70)

    #ax2.set_ylim(top=105)
    #ax2.set_ylabel("Utilization %")

    #newLns1 = itertools.chain.from_iterable(lns1)
    #ax.legend(newLns1, plotLabels1, loc='upper left')
    ax.legend(plotLabels1, loc='upper right')

    #newLns2 = itertools.chain.from_iterable(lns2)
    #ax2.legend(newLns2, plotLabels2, loc='center left')

    plt.tight_layout()

    locator = FixedLocator(myTimesTicksBins)
    ax.xaxis.set_major_locator(locator)
    #ax.locator_params(nbins=5)
    #plt.yscale('log', nonposy='clip')
    #plt.xscale('log', nonposy='clip')
    #ax.set_yscale('log')
    ax.set_title("Per Reduce Task Shuffle Times")
    #plt.legend(plotLabels, 'upper left')
    #print "calling show()"
    #plt.show()
    print "calling savefig"
    plt.savefig("foo.pdf")

if __name__ == "__main__":
  main()
