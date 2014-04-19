#!/usr/bin/python
import pylab as p

fig = p.figure()

ax = fig.add_subplot(1,1,1)

x = [1,2,3]
y = [4,5,6]

ax.bar(x,y)

p.show()
