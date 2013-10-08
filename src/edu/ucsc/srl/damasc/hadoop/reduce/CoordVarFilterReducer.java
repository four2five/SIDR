package edu.ucsc.srl.damasc.hadoop.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.ucsc.srl.damasc.hadoop.io.ArraySpec;

/**
 * Reducer that simply iterates through the data it is passed
 */

 public class CoordVarFilterReducer
  extends Reducer<ArraySpec, DoubleWritable, ArraySpec, Text> { 

  public void reduce( ArraySpec as, Iterable<DoubleWritable> values, Context context) 
    throws IOException, InterruptedException { 

    DoubleWritable tempDW;
    String outString = "";
    Text textOut = new Text();

    for( DoubleWritable val : values) { 
      tempDW = val; 
      outString += tempDW.toString();
      
    }
    if ("" != outString) { 
      textOut.set(outString);
      context.write(as, textOut); 
    }
  }
}
