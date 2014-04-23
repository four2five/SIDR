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
#hatchStyles = ["", "/", "\\"]
hatchStyles = ["/", "\\"]

crossHatchList = []
crossHatchList.append("/")
crossHatchList.append('\\')
crossHatchList.append('+')
crossHatchList.append('.')

perExpLabelList = []
perExpLabelList.append("SciHadoop 22 Reducers")
perExpLabelList.append("SIDR 22 Reducers")


##################
# 
# This script parses the output from loggedfs
# and bins all reads and writes into per-second
# buckets. It then calculates the number of 
# seeks per second. 
##################

def usage():
  print "./create_shuffle_time_graph.py -f <file to parse>[,<another file to parse>]"

# input is two arrays, representing time and size for the shuffles from a given
# MapReduce job
def presentData(timeData, ax):

  index = np.arange(len(myTimesBins) -1)
  width = 0.40
  gap = 0.05
  binnedData = []
  for runData in timeData:
    (n, retBins) = np.histogram(runData, myTimesBins)
    binnedData.append(n)

  counter = 0
  rects = []
  for dataset in binnedData:
    tempRect = ax.bar(index + (counter * width), np.array(dataset),  \
            width - gap, \
            #alpha = (counter + 1) * 0.2, \
            alpha = 0.4, \
            hatch=crossHatchList[counter], \
            color=lineColorList[counter], \
            label=perExpLabelList[counter], \
            )
    counter = counter + 1
    rects.append(tempRect)

  # set the xticks_labels
  xlabels = [item.get_text() for item in ax.get_xticklabels()]
  print "len(xlabels): " + str(len(xlabels)) + " len(myTimesBins): " + str(len(myTimesBins))
  #for x in range(len(xlabels)):
  #  xlabels[x] = str(myTimesBins[x+1])
#myTimesBins = [0, 100, 250, 500, 1000, 1500, 2000, 3000, 4000, 5000, 10000]
  myXLabels = []
  for x in range(len(myTimesBins)):
    #xlabels[x] = str(myTimesBins[x+1])
    myXLabels.append(str(myTimesBins[x]))

  ax.set_xticks(index+width)
  ax.set_xticklabels(myXLabels)

  # try adding the actual values into the graph
  def autolabel(rects):
    # attach some text labels
    for rect in rects:
      height = rect.get_height()
      if height > 0:
        print "height: " + str(height)
        #ax.text(rect.get_x()+rect.get_width()/2., 1.05*height, '%d'%int(height), \
        ax.text(rect.get_x()+rect.get_width()/2., height + 100, '%d'%int(height), \
                ha='center', va='bottom')

  for myRect in rects:
    autolabel(myRect)

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
    datasetNum = 0
    dataArray = []

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
            #print key, " $ ", count
            #bucketedSeeks[key.rstrip()] = int(count.rstrip())
            timeData.append(long(d1) / 1000000)
            sizeData.append(long(d2))

        ins.close()
        dataArray.append(timeData)

      # else, parse the raw log files
      else:
        # pull out the per-second seek rates
        (timeData, sizeData) = parseFile(filename)
        dataArray.append(timeData)

    presentData(dataArray, ax)  
    #print "adding graph for ", figureLabelList[datasetNum]
    print "adding graph for ", plotName

    ax.set_xlabel("Shuffle Time (milliseconds)")
    ax.set_ylabel("Unique Shuffles per Bin")
    #ax.set_ylim(top=70)

    #ax2.set_ylim(top=105)
    #ax2.set_ylabel("Utilization %")

    ax.legend(loc='upper right')

    plt.tight_layout()

    ax.set_title("Per Shuffle Transfer Times")
    #print "calling show()"
    #plt.show()
    print "calling savefig"
    plt.savefig("foo.pdf")

if __name__ == "__main__":
  main()
