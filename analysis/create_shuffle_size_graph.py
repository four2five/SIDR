#!/usr/bin/python
import getopt, sys, re, time, datetime, itertools
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import statsmodels.tools as sm


# global vars
myFigSize = (9.5,6)  # seeks to scale well for different paper formats
#figureLabelList = []
#figureLabelList.append("node 1")
#figureLabelList.append("node 2")

lineColorList = []
lineColorList.append("r")
lineColorList.append("b")

lineColorList2 = []
lineColorList2.append("m")
lineColorList2.append("g")

lineStyleList = []
lineStyleList.append("--")
lineStyleList.append(":")
lineStyleList.append("-")

markerStyleList = []
markerStyleList.append("2")
markerStyleList.append(",")
markerStyleList.append("1")

#perExpLabelList = []
#perExpLabelList.append("SciHadoop 22 Reducers")
#perExpLabelList.append("SIDR 22 Reducers")

crossHatchList = []
crossHatchList.append("/")
crossHatchList.append('\\')
crossHatchList.append('+')
crossHatchList.append('.')

##################
# 
# This script parses the output from loggedfs
# and bins all reads and writes into per-second
# buckets. It then calculates the number of 
# seeks per second. 
##################

def usage():
  print "./parse_loggedfs.py <file to parse>[,<another file to parse>]"

# input is two arrays, representing time and size for the shuffles from a given
# MapReduce job
def presentData(sizeData, plotName, ax, plotLabels):

  print "len(sizeData): " + str(len(sizeData))
  print "len(sizeData[0]): " + str(len(sizeData[0]))

  #mySizesBins = [0, 1024, 10240, 51200, 1048576]
  #mySizesBins = [0, 1, 10, 25, 50, 75, 100, 120]
  mySizesBins = [0, 1, 5, 10, 20, 40, 60, 80, 100, 120, 140]
  index = np.arange(len(mySizesBins) -1)
  print "index: " + str(index)
  width = 0.40
  gap = 0.05
  binnedData = []
  for runData in sizeData:
    #print "crossHatchList[" + str(datasetNum) + "]: " + str(crossHatchList[datasetNum])
    #ax.hist(np.array(sizes), mySizesBins, alpha=(0.20 * (datasetNum+1)), histtype='stepfilled', edgecolor="black", hatch=crossHatchList[datasetNum])
    #(n, retBins, patches) = np.histogram(np.array(sizes), mySizesBins, alpha=(0.20 * (datasetNum+1)), histtype='bar', edgecolor="black", hatch=crossHatchList[datasetNum])
    (n, retBins)  = np.histogram(runData, mySizesBins)
    binnedData.append(n)
    print "bins: "
    for element in retBins:
      print "\t" + str(element)

    print "n: " 
    for element in n:
      print "\t" + str(element)

  counter = 0
  rects = []
  for dataset in binnedData:
    tempRect = ax.bar(index + (counter * width), np.array(dataset),  \
            width - gap, \
            #alpha = (counter + 1) * 0.2, \
            alpha = 0.4, \
            hatch=crossHatchList[counter], \
            color=lineColorList[counter], \
            label=plotLabels[counter], \
            )
    counter = counter + 1
    rects.append(tempRect)

  # set the xticks_labels
  xlabels = [item.get_text() for item in ax.get_xticklabels()]
  print "len(xlabels): " + str(len(xlabels)) + " len(mySizesBins): " + str(len(mySizesBins))
  for x in range(len(xlabels)):
    xlabels[x] = str(mySizesBins[x+1])

  ax.set_xticklabels(xlabels)
  ax.set_xticks(index+width)

  # try adding the actual values into the graph
  def autolabel(rects):
    # attach some text labels
    for rect in rects:
      height = rect.get_height()   
      if height > 0:
        print "height: " + str(height)
        #ax.text(rect.get_x()+rect.get_width()/2., height + 100, '%d'%int(height), \
        ax.text(rect.get_x()+rect.get_width()/2., height + 30, '%d'%int(height), \
                ha='center', va='bottom')

  for myRect in rects:
    autolabel(myRect)


  #lns1.append(line)
  print "adding plotLabel ", plotName

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
    lns1 = []
    lns2 = []
    datasetNum = 0
    dataArray = []
    plotLabels = []

    for filename in filesToParse:
      timeData = []
      sizeData = []

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
            timeData.append(long(d1) / 1000000) # turn nano seconds into milliseconds
            sizeData.append(long(d2) / (1024*1024)) # turn bytes into MB
            #print d2 + " : " + str(long(d2) / (1024*1024))

        ins.close()
        dataArray.append(sizeData)
        plotLabels.append(plotName)
      # else, parse the raw log files
      else:
        # pull out the per-second seek rates
        (timeData, sizeData) = parseFile(filename)
        dataArray.append(sizeData)

      # now present it in some meaningful manner
      # data1 -> time, data2 -> sizes

    presentData(dataArray, plotName, ax, plotLabels)  
    #print "adding graph for ", plotName 

    ax.set_xlabel("Shuffle Size (MB)")
    ax.set_ylabel("Unique Shuffles per Bin")

    ax.legend(loc='upper right')

    plt.tight_layout()

    (ylim_low, ylim_high)= ax.get_ylim()
    ylim_high = ylim_high * 1.05
    ax.set_ylim(ylim_low, ylim_high)
    ax.set_title("Per Shuffle Transfer Sizes")
    #print "calling show()"
    #plt.show()
    print "calling savefig"
    plt.savefig("foo.pdf")

if __name__ == "__main__":
  main()
