package edu.ucsc.srl.damasc.hadoop.tools.hdf5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.ucsc.srl.damasc.hadoop.Utils;
import edu.ucsc.srl.damasc.hadoop.io.ArraySpec;
import edu.ucsc.srl.damasc.hadoop.io.AverageResult;
//import edu.ucsc.srl.damasc.hadoop.io.GroupID;
import edu.ucsc.srl.damasc.hadoop.io.HDF5Tools;
import edu.ucsc.srl.damasc.hadoop.io.HolisticResult;
import edu.ucsc.srl.damasc.hadoop.io.Result;
import edu.ucsc.srl.damasc.hadoop.map.AverageMapper;
import edu.ucsc.srl.damasc.hadoop.partition.ArraySpecPartitioner;
import edu.ucsc.srl.damasc.hadoop.reduce.AverageReducer;

import edu.ucsc.srl.damasc.hadoop.io.input.HDF5FileInputFormat;
import edu.ucsc.srl.damasc.hadoop.io.output.HDF5FileOutputFormat;

import edu.ucsc.srl.damasc.hadoop.Utils.Operator;
import edu.ucsc.srl.damasc.hadoop.Utils.PartitionerClass;

public class Average extends Configured implements Tool {

	public int run(String[] args) throws Exception {

    System.out.println(" in hdf5.Average.run(), args len: " + args.length + " args content ");

    for( int i=0; i<args.length; i++) { 
      System.out.println(args[i]);
    }

		if (args.length != 2) {
			System.err.println("Usage: hdf5_average <input> <output>");
			System.exit(2);
		}

    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);

		Configuration conf = getConf();
    String jobNameString = "HDF5.Average ";

    // get the buffer size
    int bufferSize = Utils.getBufferSize(conf);
    jobNameString += " buffersize: " + bufferSize + " ";

    HDF5Tools hdf5Tools = new HDF5Tools();
    // get the variable name
    String variableName = Utils.getVariableName(conf);
    System.out.println("Variable name: " + variableName);

    // get the ceph conf file path
    String cephConfPath = Utils.getCephConfPath(conf);
    System.out.println("Ceph conf path: " + cephConfPath);

    //int numDims = HDF5Tools.getNDims(cephConfPath, inputPath.toString(), variableName);

    int[] variableShape = hdf5Tools.getVariableShape(inputPath.toString(), variableName, conf);
    //int dataTypeSize = HDF5Tools.getDataTypeSize( cephConfPath, inputFilePath,
     //                                         variableName);
    int numReducers = 1;

    System.out.println("variable shape: " + Utils.arrayToString(variableShape)); 
                       //" datatype size: " + dataTypeSize);

    //Utils.setDataTypeSize(dataTypeSize);
    //Utils.setVariableShape(variableShape);
    Utils.setVariableShape(conf, variableShape);
    //Utils.setOutputDataTypeSize(conf, dataTypeSize);

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

    JobConf jobConf = new JobConf(conf);
    int test = jobConf.getNumReduceTasks();
    System.out.println("Double checking. NumReducers for job: " + test);
    test = jobConf.getNumMapTasks();
    System.out.println("Double checking. NumMappersfor job: " + test);

    Job job = new Job(conf);

    job.setNumReduceTasks( numReducers );

    jobNameString += " with " + numReducers + 
                     " reducers ";

    job.setJarByClass(Average.class);
    job.setMapperClass(AverageMapper.class);
    job.setReducerClass(AverageReducer.class);
	
	  // mapper output
	  job.setMapOutputKeyClass(ArraySpec.class);
	  job.setMapOutputValueClass(AverageResult.class);

    // reducer output
    job.setOutputKeyClass(ArraySpec.class);
    job.setOutputValueClass(IntWritable.class);
	
    job.setInputFormatClass(HDF5FileInputFormat.class);
    //job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputFormatClass(HDF5FileOutputFormat.class);

    PartitionerClass partitionerClass = Utils.getPartitionerClass(conf);
    if( PartitionerClass.hash == partitionerClass) { 
      //default, no action needed
      job.setPartitionerClass(HashPartitioner.class);
    } else if( PartitionerClass.arrayspec == partitionerClass) { 
      job.setPartitionerClass(ArraySpecPartitioner.class);
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

    HDF5FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.waitForCompletion(true);

    return 0;
	}

	public static void main(String[] args) throws Exception {
    System.out.println("in hdf5.average.main()");
		int res = ToolRunner.run(new Configuration(), new Average(), args);
		System.exit(res);
	}
}
