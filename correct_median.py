#!/usr/bin/python
from numpy import *


def printOutValues(varShape, exShape):
  if( len(varShape) != len(exShape)):
    print "len(varShape): ", len(varShape), " len(exShape): ", len(exShape)
    return -1

  strides = []

  for x in range(len(varShape)):
    val = 1
    for y in range(len(varShape)):
      if ( y > x ):
        val *= varShape[y]
    strides.append(val)

  #print "strides: "
  #for x in strides:
  #  print x

  start = strides[0]# 360*360*50 = 6,480,000
  #value = start
  for a in range(varShape[0]/exShape[0]):
    print "\t buck, value = ", ( (strides[0] * a * exShape[0]) + strides[0])
    #value = (strides[0] * a * exShape[0]) + strides[0]# should be 12960000
    for b in range(varShape[1]/exShape[1]):
      for c in range(varShape[2]/exShape[2]):
        for d in range(varShape[3]/exShape[3]):
          value = (a * strides[0] * exShape[0]) + \
          (b * strides[1] * exShape[1]) + \
          (c * strides[2] * exShape[2]) + \
          (d * strides[3] * exShape[3]) + \
          start
          print value
          #value += strides[3] * exShape[3] # should be 10
        #value += strides[2] * exShape[2] # should be 500
      #value += strides[1] * exShape[1] # should be 180000



variableShape = array( [10, 360, 360, 50] )
extractionShape = array( [2, 36, 36, 10] )

printOutValues( variableShape, extractionShape)
          
