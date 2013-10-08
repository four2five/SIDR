package edu.ucsc.srl.damasc.hadoop.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.ucsc.srl.damasc.hadoop.io.ArraySpec;
//import edu.ucsc.srl.damasc.hadoop.io.IntArrayWritable;
//import org.apache.hadoop.mapreduce.TaskAttemptID;
//import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.io.DoubleWritable;


//import edu.ucsc.srl.damasc.hadoop.HDF5Utils;
//import edu.ucsc.srl.damasc.hadoop.Utils;
//import edu.ucsc.srl.damasc.hadoop.HadoopUtils;
//import edu.ucsc.srl.damasc.hadoop.io.GroupID;

/**
 * Reducer that simply iterates through the data it is passed
 */

 public class CoordVarFilterReducer
  extends Reducer<ArraySpec, DoubleWritable, ArraySpec, Text> { 

  public void reduce( ArraySpec as, Iterable<DoubleWritable> values, Context context) 
    throws IOException, InterruptedException { 

    DoubleWritable tempDW;
    //DoubleWritable[] tempIW;
    String outString = "";
    Text textOut = new Text();

    for( DoubleWritable val : values) { 
      tempDW = val; 
      //tempIW = tempIAW.get();
      /*
      for( int i=0; i<tempIW.length; i++) { 
        if( "" != outString ) { 
          outString += "," + tempIW[i].toString();
        } else { 
          outString += tempIW[i].toString();
        }

      }
      */
      outString += tempDW.toString();
      
    }
    if ("" != outString) { 
      textOut.set(outString);
      //context.write(as, textOut, 1); 
      context.write(as, textOut); 
    }
  }
}
