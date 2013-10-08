package edu.ucsc.srl.damasc.hadoop.tools.netcdf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.ucsc.srl.damasc.hadoop.io.ArraySpec;
import edu.ucsc.srl.damasc.hadoop.io.input.ArrayBasedFileInputFormat;
import edu.ucsc.srl.damasc.hadoop.map.IdentityMapper;
import edu.ucsc.srl.damasc.hadoop.reduce.IdentityReducer;
import edu.ucsc.srl.damasc.hadoop.Utils;
import edu.ucsc.srl.damasc.hadoop.Utils.FSType;

public class Identity extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: identity <input> <output>");
			System.exit(2);
		}

		Configuration conf = getConf();
    String jobNameString = "";

    // get the buffer size
    int bufferSize = Utils.getBufferSize(conf);
    jobNameString += " buffersize: " + bufferSize + " ";

    jobNameString += " Identity ";

    String variableName = Utils.getVariableName(conf);
    if( variableName.equals(""))
      System.out.println("No variable specified");
    else { 
      System.out.println("Variable name: " + variableName);
    }

    String cephConfPath = Utils.getCephConfPath(conf);
    System.out.println("Ceph conf path: " + cephConfPath);

    FSType fsType = Utils.getFSType(conf);
    if( FSType.hdfs == fsType) { 
      System.out.println("FSType is hdfs");
    } 
    if( FSType.ceph == fsType) { 
      System.out.println("FSType is ceph");
    }
    if( FSType.unknown == fsType) { 
      System.out.println("FSType is unknown");
    }



    String inputFilePath = args[0];

    int[] variableShape = Utils.getVariableShape(conf);

    System.out.println("variable shape: " + Utils.arrayToString(variableShape)); 
    Utils.setVariableShape(conf, variableShape);
    Job job = new Job(conf);

    job.setJarByClass(Identity.class);
    job.setMapperClass(IdentityMapper.class);
	  job.setReducerClass(IdentityReducer.class);

	  // mapper output
	  job.setMapOutputKeyClass(ArraySpec.class);
	  job.setMapOutputValueClass(IntWritable.class);

	  // reducer output
	  job.setOutputKeyClass(ArraySpec.class);
	  job.setOutputValueClass(IntWritable.class);

    if( Utils.noScanEnabled(conf) ) 
      jobNameString += " with noscan ";

    if( Utils.queryDependantEnabled(conf) ) 
      jobNameString += " and query dependant";

    jobNameString += Utils.getPartModeString(conf) + ", " + 
                     Utils.getPlacementModeString(conf);
    jobNameString += " with " + Utils.getNumberReducers(conf) + 
                     " reducers ";

    job.setJobName(jobNameString);

    job.setInputFormatClass(ArrayBasedFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks( Utils.getNumberReducers(conf) );

    ArrayBasedFileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);

    return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Identity(), args);
		System.exit(res);
	}
}
