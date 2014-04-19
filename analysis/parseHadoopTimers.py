#!/usr/bin/python
import getopt, sys, re, datetime, time, os

def usage():
	print "./parseHadoopTimers.py -d <hadoop job directory>"

def parseReduceFiles(filename):
	filename = filename.strip()

	# read the data
	FILE = open(filename, "r")
	# make sure that you have enough memory (but test file is ~8 MB so we should be fine)
	data = FILE.read() 
	FILE.close()

	# get the reduce times
	searchString = "total reducer took: (\d+) ms"
	matchObj = re.findall(searchString, data)

	runningReduceTime = 0
	reducesEncountered = 0

	for entry in matchObj:
		#print "entry:", entry
		runningReduceTime += int(entry)
		reducesEncountered += 1

	return (runningReduceTime, reducesEncountered)

def parseMapFiles(filename):
	filename = filename.strip()
	#print "parsing file ", filename
	#return (1,1)

	# read the data
	FILE = open(filename, "r")
	# make sure that you have enough memory (but test file is ~8 MB so we should be fine)
	data = FILE.read() 
	FILE.close()

	# get the startup times
	searchString = "Startup time: (\d+) ms"
	matchObj = re.findall(searchString, data)

	runningStartTime = 0
	startsEncountered = 0

	for entry in matchObj:
		#print "entry:", entry
		runningStartTime += int(entry)
		startsEncountered += 1

	# get the map loop times
	searchString = ".* map loop time: (\d+) ms.*"
	matchObj = re.findall(searchString, data)

	runningMapLoopTime = 0
	mapsEncountered = 0

	for entry in matchObj:
		#print "entry:", entry
		runningMapLoopTime += int(entry)
		mapsEncountered += 1

	# get the context.write time
	searchString = "writing data.* (\d+) ms"
	matchObj = re.findall(searchString, data)

	runningWriteTime = 0
	writesEncountered = 0

	for entry in matchObj:
		#print "entry:", entry
		runningWriteTime += int(entry)
		writesEncountered += 1

	# get the combiner time
	searchString = "Entire combiner took (\d+) ms"
	matchObj = re.findall(searchString, data)

	runningCombinerTime = 0
	combinersEncountered = 0

	for entry in matchObj:
		#print "entry:", entry
		runningCombinerTime += int(entry)
		combinersEncountered += 1

	return (runningStartTime, startsEncountered, runningMapLoopTime, mapsEncountered,\
			runningWriteTime, writesEncountered, runningCombinerTime, combinersEncountered)

# prints out all the keywords and their values
def debugPrintResults(results, time):
	print "test ran in %s:%s" % (divmod(time,60))
	for k,v in results.iteritems():
	  for entry in v:
		print k, "=>", entry

def parseDirectory(dirPath):
	print "parsing", dirPath

	# dirName should look something like this:
	#	job_201104262343_0002
	# and it has subdirs that look like this
	# attempt_201104262343_0002_m_000005_0
	# attempt_201104262343_0002_r_000000_0

	dirList = os.listdir(dirPath)

	mapSubDirsToProcess = []
	mapSubDirSearchString = "attempt_\d+_\d+_m_\d+_\d+"

	for mapSubDir in dirList:
		matchObj = re.match(mapSubDirSearchString, mapSubDir)

		if matchObj:
			#print "adding ", matchObj.group(0)
			mapSubDirsToProcess.append(matchObj.group(0))

	reduceSubDirsToProcess = []
	reduceSubDirSearchString = "attempt_\d+_\d+_r_\d+_\d+"

	for reduceSubDir in dirList:
		matchObj = re.match(reduceSubDirSearchString, reduceSubDir)

		if matchObj:
			#print "adding ", matchObj.group(0)
			reduceSubDirsToProcess.append(matchObj.group(0))


	globalStartTime = 0
	globalStartCount = 0
	globalMapTime = 0
	globalMapCount = 0
	globalWriteTime = 0
	globalWriteCount = 0
	globalCombinerTime = 0
	globalCombinerCount = 0

	fileSearchString = "syslog"

	for subDir in mapSubDirsToProcess:
		#print "subdir: ", subDir
		fileList = os.listdir(dirPath + "/" + subDir)

		for file in fileList:
			matchObj = re.match(fileSearchString, file)

			if matchObj:
				#print "calling parseFile on: ",\
				#	  (dirPath + "/" + subDir + "/" + matchObj.group(0))

				(fileStartTime, fileStartCount, fileMapTime, fileMapCount,\
				 fileWriteTime, fileWriteCount, fileCombinerTime, fileCombinerCount) =\
					parseMapFiles( dirPath + "/" + subDir + "/" + matchObj.group(0))

				# the if is to prevent jobs without results from messing up the data
				globalStartTime += fileStartTime
				globalStartCount += fileStartCount
				globalMapTime += fileMapTime
				globalMapCount += fileMapCount
				globalWriteTime += fileWriteTime
				globalWriteCount += fileWriteCount
				globalCombinerTime += fileCombinerTime
				globalCombinerCount += fileCombinerCount

	globalReduceTime = 0
	globalReduceCount = 0
	fileSearchString = "syslog"

	for subDir in reduceSubDirsToProcess:
		#print "subdir: ", subDir
		fileList = os.listdir(dirPath + "/" + subDir)

		for file in fileList:
			matchObj = re.match(fileSearchString, file)

			if matchObj:
				#print "calling parseFile on: ",\
				#	  (dirPath + "/" + subDir + "/" + matchObj.group(0))

				(fileReduceTime, fileReduceCount) =\
					parseReduceFiles( dirPath + "/" + subDir + "/" + matchObj.group(0))

				globalReduceTime += fileReduceTime
				globalReduceCount += fileReduceCount

					
	print "test:\ttotal time\t| count\t\t average"
#	print "total start time: ", globalStartTime
#	print "start times seen: ", globalStartCount
#	if (globalStartCount > 0): # prevent divide by zero errors
#		print "average start time: ", (globalStartTime / globalStartCount)
	print "start:\t\t", globalStartTime, "\t| ", globalStartCount,\
			"\t\t| ", (globalStartTime / globalStartCount if globalStartCount > 0 else 0)
#
#	print "total map loop time: ", globalMapTime
#	print "map loops seen: ", globalMapCount
#	if (globalMapCount > 0): # prevent divide by zero errors
#		print "average map loop time: ", (globalMapTime / globalMapCount)
	print "map loop:\t", globalMapTime, "\t| ", globalMapCount,\
			"\t\t| ", (globalMapTime / globalMapCount if globalMapCount > 0 else 0)
#
#	print "total write time: ", globalWriteTime
#	print "writes seen: ", globalWriteCount
#	if (globalWriteCount > 0): # prevent divide by zero errors
#		print "average write time: ", (globalWriteTime / globalWriteCount)
	print "write:\t\t", globalWriteTime, "\t| ", globalWriteCount,\
			"\t\t| ", (globalWriteTime / globalWriteCount if globalWriteCount > 0 else 0)
#
#	print "total combiner time: ", globalCombinerTime
#	print "combiners seen: ", globalCombinerCount
#	if (globalCombinerCount > 0): # prevent divide by zero errors
#		print "average combiner time: ", (globalCombinerTime / globalCombinerCount)
	print "combiner:\t", globalCombinerTime, "\t| ", globalCombinerCount,\
			"\t| ", (globalCombinerTime / globalCombinerCount if globalCombinerCount > 0 else 0)
#
#	print "total reduce time: ", globalReduceTime
#	print "reduces seen: ", globalReduceCount
#	if (globalReduceCount > 0): # prevent divide by zero errors
#		print "average reduce time: ", (globalReduceTime / globalReduceCount)
	print "reduce:\t\t", globalReduceTime, "\t| ", globalReduceCount,\
			"\t| ", (globalReduceTime / globalReduceCount if globalReduceCount > 0 else 0)

	#for fileName in filesToProcess:
		#return (returnI, avgWait, avgUtil, above95, avgUser, avgIOWait, avgIdle)
		#tempUnits, tempWaitVal, tempUtilVal, tempAbove95, tempUser, tempIOWait, tempIdle =  parseFile(dirPath + "/" + fileName)
		#i = i + 1


def main():
	try:
		opts, args = getopt.getopt(sys.argv[1:], "hd:")
	except getopt.GetoptError, err:
		# print help information and exit:
		print str(err) # will print something like "option -a not recognized"
		usage()
		sys.exit(2)

	dirname = ""

	for o, a in opts:
		if o in ("-d", "--dirname"):
			dirname = a
		elif o in ("-h", "--help"):
			usage()
			sys.exit()
		else:
			assert False, "unhandled option"

	if dirname != "":
		if os.path.isdir(dirname):
			files = parseDirectory(dirname)
		else:
			print "specified directory (", dirname, ") is not a valid directory"
	else:
		print "a dirname(%s) was not specified. Poor form",  dirname
		exit

	#debugPrintResults(results, time)

if __name__ == "__main__":
	main()
