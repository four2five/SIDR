package edu.ucsc.srl.damasc.hadoop.tools.netcdf_hdfs;


import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.ucsc.srl.damasc.hadoop.Utils;
import edu.ucsc.srl.damasc.hadoop.NetCDFUtils;
import edu.ucsc.srl.damasc.hadoop.Utils.PartitionerClass;
import edu.ucsc.srl.damasc.hadoop.io.ArraySpec;
import edu.ucsc.srl.damasc.hadoop.io.MultiVarData;
import edu.ucsc.srl.damasc.hadoop.io.NetCDFHDFSTools;
import edu.ucsc.srl.damasc.hadoop.io.input.ArrayBasedFileInputFormat;
import edu.ucsc.srl.damasc.hadoop.map.CoordVarFilterMapper;
import edu.ucsc.srl.damasc.hadoop.partition.ArraySpecPartitioner;
import edu.ucsc.srl.damasc.hadoop.partition.PerFileArraySpecPartitioner;
import edu.ucsc.srl.damasc.hadoop.reduce.CoordVarFilterReducer;

import java.net.URI;
import java.util.Arrays;

import edu.ucsc.srl.damasc.hadoop.io.input.NetCDFHDFSFileInputFormat;

public class CoordVarFilter extends Configured implements Tool {

	public int run(String[] args) throws Exception {

    System.out.println(" in netcdf_hdfs.CoordVarFilter.run(), args len: " + args.length + " args content ");

    for( int i=0; i<args.length; i++) { 
      System.out.println(args[i]);
    }

		if (args.length != 2) {
			System.err.println("Usage: netcdf_hdfs_filter <input> <output>");
			System.exit(2);
		}

    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);
    System.out.println("JB, I think the input is " + inputPath.toString());

    NetCDFHDFSTools netcdfTools = new NetCDFHDFSTools();

    JobConf conf = new JobConf(getConf(), CoordVarFilter.class);
    String jobNameString = "CoordVarFilter ";

    // get the buffer size
    int bufferSize = Utils.getBufferSize(conf);
    jobNameString += " buffersize: " + bufferSize + " ";

    String variableName = Utils.getVariableName(conf);

    float low = Utils.getLowThreshold(conf);
    float high = Utils.getHighThreshold(conf);
    float equal = Utils.getEqualValue(conf);

    System.out.println("low: " + low + " high: " + high + " equal: " + equal);

    // get the ceph conf file path
    String cephConfPath = Utils.getCephConfPath(conf);
    System.out.println("Ceph conf path: " + cephConfPath);

    // sort out if there is a coordinate variable
    String[] coordVarNames = Utils.getCoordinateVariableName(conf).split(",");
    String[] variablesToPersist = Utils.getVariablesToPersist(conf).split(",");
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] inputFiles = fs.globStatus(inputPath);
    for (FileStatus input : inputFiles) { 
      System.out.println("  if: " + input.getPath().toString());
    }

    Path cachedFileName = new Path(inputFiles[0].getPath().getParent(), "cached_coord_data");
    System.out.println("cache file: " + cachedFileName.toString());
    conf.set(Utils.CACHED_COORD_FILE_NAME, cachedFileName.toString());

    for (String coordVarName : coordVarNames) { 
      System.out.println("\tCoord Var Name: " + coordVarName);
    }

    for (String varToPersist: variablesToPersist) { 
      System.out.println("\t Persisting var: " + varToPersist);
    }

    NetCDFHDFSTools.extractCoordinateVarsToFile(conf, inputFiles[0].getPath().toString(), 
                                                variablesToPersist, 
                                                cachedFileName.toString()); 
    // then add the file to the distributed cache so each node has it handy
    DistributedCache.addCacheFile(new URI(cachedFileName.toString()), conf);
    System.out.println("extracted vars to file " + cachedFileName.toString() + 
                       " and added it to the distributed cache");

    // get the variable name
    int[] variableShape =  NetCDFHDFSTools.getVariableShape( 
                                inputFiles[0].getPath().toString(), 
                                variableName, conf); 
    System.out.println("Variable name: " + variableName);

    int[] extractionShape = Utils.getExtractionShape(conf);
    System.out.println("Extraction shape: " + Arrays.toString(extractionShape));

    if (variableShape.length != extractionShape.length) {
        System.out.println("Configured extraction shape is a different length than " + 
                           " the observed variable length. " + variableShape.length + 
                           " != " + extractionShape.length);
        System.exit(-2);
    }

    int numReducers = 1;

    Utils.setVariableShape(conf, variableShape);

    String fooString = conf.get(Utils.VARIABLE_SHAPE_PREFIX);

    System.out.println(" reading VariableString from conf: " + fooString);

    long maxReducerKeyCount = Utils.getReducerKeyLimit(conf);
    if( (long)-1 != maxReducerKeyCount) {
      // get the record dimension given a maximum number of keys per step
      numReducers = Utils.determineNumberOfReducers(conf);
      System.out.println("Using maxReducerKeyCount, this job has " + 
                          numReducers + " reducers");
      Utils.setNumberReducers(conf, numReducers);

    } else { 
      numReducers = Utils.getNumberReducers(conf);
      Utils.setNumberReducers(conf, numReducers);
      System.out.println("Using configured number of reducers, " + 
        numReducers);
    }

    Job job = new Job(conf);
    job.setNumReduceTasks( numReducers );
    jobNameString += " with " + numReducers + 
                     " reducers ";

    job.setJarByClass(CoordVarFilter.class);
    job.setMapperClass(CoordVarFilterMapper.class);
    job.setReducerClass(CoordVarFilterReducer.class);
	
	  // mapper output
	  job.setMapOutputKeyClass(ArraySpec.class);
	  job.setMapOutputValueClass(DoubleWritable.class);

    // reducer output
    job.setOutputKeyClass(ArraySpec.class);
    job.setOutputValueClass(DoubleWritable.class);

    job.setInputFormatClass(NetCDFHDFSFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    PartitionerClass partitionerClass = Utils.getPartitionerClass(conf);
    if( PartitionerClass.hash == partitionerClass) { 
      //default, no action needed
      job.setPartitionerClass(HashPartitioner.class);
    } else if( PartitionerClass.arrayspec == partitionerClass) { 
      job.setPartitionerClass(ArraySpecPartitioner.class);
    } else if( PartitionerClass.perfilearrayspec == partitionerClass) { 
      job.setPartitionerClass(PerFileArraySpecPartitioner.class);
    } else { 
      System.out.println("I don't understand the specified PartitionerClass. Bailing");
      return -1;
    }

    String partitionerType = job.getPartitionerClass().getCanonicalName();
    System.out.println("Partitioner: " + partitionerType);

    if( Utils.noScanEnabled(conf) ) 
      jobNameString += " with noscan ";

    if( Utils.queryDependantEnabled(conf) ) 
      jobNameString += " and query dependant";

    if( Utils.startReducerDynamically(conf) ) 
      jobNameString += " and dynamic reducer starts ";

    jobNameString += Utils.getPartModeString(conf) + ", " + 
                     Utils.getPlacementModeString(conf);

    job.setJobName(jobNameString);

    for (FileStatus input : inputFiles) { 
      NetCDFHDFSFileInputFormat.addInputPath(job, input.getPath());
    }

    FileOutputFormat.setOutputPath(job, outputPath);

    job.waitForCompletion(true);

    return 0;
	}

	public static void main(String[] args) throws Exception {
    System.out.println("in netcdf_hdfs.CoordVarFilter.main()");
		int res = ToolRunner.run(new Configuration(), new CoordVarFilter(), args);
		System.exit(res);
	}
}
