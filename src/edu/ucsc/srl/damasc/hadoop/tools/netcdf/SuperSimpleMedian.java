package edu.ucsc.srl.damasc.hadoop.tools.netcdf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.ucsc.srl.damasc.hadoop.Utils;
import edu.ucsc.srl.damasc.hadoop.Utils.PartitionerClass;
import edu.ucsc.srl.damasc.hadoop.io.ArraySpec;
import edu.ucsc.srl.damasc.hadoop.io.HolisticResult;
import edu.ucsc.srl.damasc.hadoop.io.NetCDFTools;
//import edu.ucsc.srl.damasc.hadoop.io.ArrayTools;
import edu.ucsc.srl.damasc.hadoop.io.input.ArrayBasedFileInputFormat;
import edu.ucsc.srl.damasc.hadoop.map.SuperSimpleMedianMapper;
import edu.ucsc.srl.damasc.hadoop.partition.ArraySpecPartitioner;
import edu.ucsc.srl.damasc.hadoop.reduce.SuperSimpleMedianReducer;

public class SuperSimpleMedian extends Configured implements Tool {

	public int run(String[] args) throws Exception {

    System.out.println(" in SuperSimpleMedian.run(), args len: " + args.length + " args content ");

    for( int i=0; i<args.length; i++) { 
      System.out.println(args[i]);
    }

		if (args.length != 2) {
			System.err.println("Usage: netcdf_supersimplemedian <input> <output>");
			System.exit(2);
		}


		Configuration conf = getConf();
    String jobNameString = "NetCDF.SuperSimpleMedian";

    // get the buffer size
    int bufferSize = Utils.getBufferSize(conf);
    jobNameString += " buffersize: " + bufferSize + " ";

    // get the variable name
    String variableName = Utils.getVariableName(conf);
    System.out.println("Variable name: " + variableName);

    // get the ceph conf file path
    String cephConfPath = Utils.getCephConfPath(conf);
    //System.out.println("Ceph conf path: " + cephConfPath);


    String inputFilePath = args[0];

    //int[] variableShape = NetCDFTools.getVariableShape(cephConfPath, inputFilePath, 
    //                                              variableName, conf);
    //int dataTypeSize = NetCDFTools.getDataTypeSize( cephConfPath, inputFilePath,
                                              //variableName,conf );
    int[] variableShape = Utils.getVariableShape(conf); 
    int dataTypeSize = NetCDFTools.getDataTypeSize(inputFilePath, variableName, conf);

    int numReducers = 1;

    System.out.println("variable shape: " + Utils.arrayToString(variableShape) + 
                       " datatype size: " + dataTypeSize);

    //Utils.setDataTypeSize(dataTypeSize);
    //Utils.setVariableShape(variableShape);
    Utils.setVariableShape(conf, variableShape);
    //Utils.setOutputDataTypeSize(conf, dataTypeSize);

    // specify the file format specific Tools class to use
    Utils.setArrayToolsClass(conf, "edu.ucsc.srl.damasc.hadoop.io.NetCDFTools");

    //Utils.setInputDataTypeSize(conf, dataTypeSize);

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


    job.setJarByClass(SuperSimpleMedian.class);
    job.setMapperClass(SuperSimpleMedianMapper.class);
    job.setReducerClass(SuperSimpleMedianReducer.class);
	
	  // mapper output
	  job.setMapOutputKeyClass(ArraySpec.class);
	  job.setMapOutputValueClass(IntWritable.class);

    // reducer output
    job.setOutputKeyClass(ArraySpec.class);
    job.setOutputValueClass(IntWritable.class);
	

    job.setInputFormatClass(ArrayBasedFileInputFormat.class);
    //job.setOutputFormatClass(HDF5FileOutputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    //job.setPartitionerClass(ArraySpecPartitioner.class);


   
    PartitionerClass partitionerClass = Utils.getPartitionerClass(conf);
    if( PartitionerClass.hash == partitionerClass) { 
      //default, no action needed
      job.setPartitionerClass(HashPartitioner.class);
    } else if( PartitionerClass.arrayspec == partitionerClass) { 
      System.out.println("\t!! Setting ArraySpec partitioner");
      job.setPartitionerClass(ArraySpecPartitioner.class);
    } else { 
      System.out.println("I don't understand the specified PartitionerClass. Bailing");
      return -1;
    }

    if( Utils.getDependencyScheduling(conf)) { 
      System.out.println("dependency scheduling is on");
    } else { 
      System.out.println("dependency scheduling is NOT on");
    }  

    job.setDependencyScheduling(Utils.getDependencyScheduling(conf));

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

    ArrayBasedFileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);

    return 0;
	}

	public static void main(String[] args) throws Exception {
    System.out.println("in netcdf.supersimplemedian.main()");
		int res = ToolRunner.run(new Configuration(), new SuperSimpleMedian(), args);
		System.exit(res);
	}
}
