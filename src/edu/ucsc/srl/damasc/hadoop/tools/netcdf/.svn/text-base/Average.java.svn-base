package edu.ucsc.srl.damasc.hadoop.tools.netcdf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.ucsc.srl.damasc.hadoop.Utils;
import edu.ucsc.srl.damasc.hadoop.Utils.PartitionerClass;
import edu.ucsc.srl.damasc.hadoop.io.ArraySpec;
import edu.ucsc.srl.damasc.hadoop.io.AverageResult;
import edu.ucsc.srl.damasc.hadoop.io.NetCDFTools;
import edu.ucsc.srl.damasc.hadoop.io.input.ArrayBasedFileInputFormat;
import edu.ucsc.srl.damasc.hadoop.map.AverageMapper;
import edu.ucsc.srl.damasc.hadoop.partition.ArraySpecPartitioner;
import edu.ucsc.srl.damasc.hadoop.reduce.AverageReducer;

import edu.ucsc.srl.damasc.hadoop.io.input.NetCDFFileInputFormat;
import edu.ucsc.srl.damasc.hadoop.io.output.NetCDFFileOutputFormat;

public class Average extends Configured implements Tool {

	public int run(String[] args) throws Exception {

    System.out.println(" in netcdf.Average.run(), args len: " + args.length + " args content ");

    for( int i=0; i<args.length; i++) { 
      System.out.println(args[i]);
    }

		if (args.length != 2) {
			System.err.println("Usage: netcdf_identity <input> <output>");
			System.exit(2);
		}

    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);
    System.out.println("JB, I think the input is " + inputPath.toString());

    NetCDFTools netcdfTools = new NetCDFTools();

		Configuration conf = getConf();
    String jobNameString = "Average ";

    // get the buffer size
    int bufferSize = Utils.getBufferSize(conf);
    jobNameString += " buffersize: " + bufferSize + " ";

    String variableName = Utils.getVariableName(conf);
    // get the variable name
    int[] variableShape =  netcdfTools.getVariableShape( inputPath.toString(), 
                                 variableName, conf); 
    System.out.println("Variable name: " + variableName);

    // get the ceph conf file path
    String cephConfPath = Utils.getCephConfPath(conf);
    System.out.println("Ceph conf path: " + cephConfPath);

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
	

    //job.setInputFormatClass(ArrayBasedFileInputFormat.class);
    job.setInputFormatClass(NetCDFFileInputFormat.class);
    //job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputFormatClass(NetCDFFileOutputFormat.class);

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

    //HDF5FileInputFormat.addInputPath(job, new Path(args[0]));
    NetCDFFileInputFormat.addInputPath(job, inputPath);
    NetCDFFileOutputFormat.setOutputPath(job, outputPath);

    job.waitForCompletion(true);

    return 0;
	}

	public static void main(String[] args) throws Exception {
    System.out.println("in netcdf.average.main()");
		int res = ToolRunner.run(new Configuration(), new Average(), args);
		System.exit(res);
	}
}
