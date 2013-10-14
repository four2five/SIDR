package edu.ucsc.srl.damasc.hadoop.tools.netcdf_hdfs;

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
import edu.ucsc.srl.damasc.hadoop.combine.MedianCombinerInt;
import edu.ucsc.srl.damasc.hadoop.combine.MedianCombinerShort;
import edu.ucsc.srl.damasc.hadoop.io.HolisticResultInt;
import edu.ucsc.srl.damasc.hadoop.io.HolisticResultShort;
import edu.ucsc.srl.damasc.hadoop.io.ArraySpec;
import edu.ucsc.srl.damasc.hadoop.io.NetCDFHDFSTools;
import edu.ucsc.srl.damasc.hadoop.io.input.ArrayBasedFileInputFormat;
import edu.ucsc.srl.damasc.hadoop.map.MedianMapperInt;
import edu.ucsc.srl.damasc.hadoop.map.MedianMapperShort;
import edu.ucsc.srl.damasc.hadoop.partition.ArraySpecPartitioner;
import edu.ucsc.srl.damasc.hadoop.reduce.MedianReducerShort;
import edu.ucsc.srl.damasc.hadoop.reduce.MedianReducerInt;

import edu.ucsc.srl.damasc.hadoop.io.input.NetCDFHDFSFileInputFormat;

import ucar.ma2.DataType;

public class Median extends Configured implements Tool {

	public int run(String[] args) throws Exception {

    System.out.println(" in netcdf_hdfs.Median.run(), args len: " + args.length + " args content ");

    for( int i=0; i<args.length; i++) { 
      System.out.println(args[i]);
    }

		if (args.length != 2) {
			System.err.println("Usage: netcdf_hdfs_median <input> <output>");
			System.exit(2);
		}

    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);
    System.out.println("JB, I think the input is " + inputPath.toString());

    NetCDFHDFSTools netcdfTools = new NetCDFHDFSTools();

		Configuration conf = getConf();
    String jobNameString = "Median ";

    // get the buffer size
    int bufferSize = Utils.getBufferSize(conf);
    jobNameString += " buffersize: " + bufferSize + " ";

    // get the variable name and shape
    String variableName = Utils.getVariableName(conf);
    int[] variableShape =  NetCDFHDFSTools.getVariableShape( inputPath.toString(), 
                                 variableName, conf); 
    System.out.println("Variable name: " + variableName);

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

    // get the datatype of the input file. 
    DataType dataType = NetCDFHDFSTools.getDataType(inputPath.toString(),
                                                    variableName,
                                                    conf);
    System.out.println("DataType is: " + dataType.toString() + 
                       " size: " + dataType.getSize());
    Utils.setDatatypeSize(conf, dataType.getSize());

    JobConf jobConf = new JobConf(conf);

    Job job = new Job(conf);
    job.setNumReduceTasks( numReducers );
    jobNameString += " with " + numReducers + 
                     " reducers ";


    job.setJarByClass(Median.class);

    if (DataType.INT == dataType) { 
      job.setMapperClass(MedianMapperInt.class);
      job.setReducerClass(MedianReducerInt.class);

	    // mapper output
	    job.setMapOutputKeyClass(ArraySpec.class);
	    job.setMapOutputValueClass(HolisticResultInt.class);

      // reducer output
      job.setOutputKeyClass(ArraySpec.class);
      job.setOutputValueClass(HolisticResultInt.class);
    } else if (DataType.SHORT == dataType) { 
      job.setMapperClass(MedianMapperShort.class);
      job.setReducerClass(MedianReducerShort.class);

	    // mapper output
	    job.setMapOutputKeyClass(ArraySpec.class);
	    job.setMapOutputValueClass(HolisticResultShort.class);

      // reducer output
      job.setOutputKeyClass(ArraySpec.class);
      job.setOutputValueClass(HolisticResultShort.class);
    }
	

    job.setInputFormatClass(NetCDFHDFSFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    PartitionerClass partitionerClass = Utils.getPartitionerClass(conf);
    if( PartitionerClass.hash == partitionerClass) { 
      //default, no action needed
      job.setPartitionerClass(HashPartitioner.class);
      System.out.println("Using HashPartitioner ");
    } else if( PartitionerClass.arrayspec == partitionerClass) { 
      job.setPartitionerClass(ArraySpecPartitioner.class);
      System.out.println("Using ArraySpecPartitioner");
    } else { 
      System.out.println("I don't understand the specified PartitionerClass. Bailing");
      return -1;
    }

    String partitionerType = job.getPartitionerClass().getCanonicalName();
    System.out.println("Partitioner: " + partitionerType);

		if ( Utils.useCombiner(conf) ) {
			jobNameString += " with combiner ";
      if (DataType.INT == dataType) { 
			  job.setCombinerClass(MedianCombinerInt.class);
      } else if (DataType.SHORT == dataType) { 
			  job.setCombinerClass(MedianCombinerShort.class);
      } else { 
        System.out.println("!!!! ERROR: a combiner was specified but one is not available for this data type");
      }
		}

    if( Utils.noScanEnabled(conf) ) 
      jobNameString += " with noscan ";

    if( Utils.queryDependantEnabled(conf) ) 
      jobNameString += " and query dependant";

    if( Utils.startReducerDynamically(conf) ) 
      jobNameString += " and dynamic reducer starts ";

    jobNameString += Utils.getPartModeString(conf) + ", " + 
                     Utils.getPlacementModeString(conf);

    job.setJobName(jobNameString);

    NetCDFHDFSFileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.waitForCompletion(true);

    return 0;
	}

	public static void main(String[] args) throws Exception {
    System.out.println("in netcdf_hdfs.Median.main()");
		int res = ToolRunner.run(new Configuration(), new Median(), args);
		System.exit(res);
	}
}
