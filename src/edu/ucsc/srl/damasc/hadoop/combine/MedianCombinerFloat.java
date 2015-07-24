package edu.ucsc.srl.damasc.hadoop.combine;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.ucsc.srl.damasc.hadoop.io.ArraySpec;
import edu.ucsc.srl.damasc.hadoop.io.HolisticResultFloat;
import edu.ucsc.srl.damasc.hadoop.io.Result;
import edu.ucsc.srl.damasc.hadoop.Utils;

/**
 * Combiner class for the Median operator
 */
public class MedianCombinerFloat extends 
        Reducer<ArraySpec, HolisticResultFloat, ArraySpec, HolisticResultFloat> {

  private static final Log LOG = LogFactory.getLog(MedianCombinerFloat.class);
  static enum MedianCombinerFloatStatus { FULL, NOTFULL, MERGED }

  /**
   * Reduces values for a given key
   * @param key the Key for the given values being passed in
   * @param values a List of HolisticResultFloat objects to combine
   * @param context the Context object for the currently executing job
   */

  public void reduce(ArraySpec key, Iterable<HolisticResultFloat> values, 
                     Context context)
                     throws IOException, InterruptedException {


    // now we need to parse the variable dimensions out
    int[] variableShape = Utils.getVariableShape( context.getConfiguration());
    int[] extractionShape = 
        Utils.getExtractionShape(context.getConfiguration(), 
                                 variableShape.length);
    long neededValues = Utils.calcTotalSize(extractionShape);

    HolisticResultFloat holVal = new HolisticResultFloat();
    holVal.setNeededCount( (int)neededValues );

    for (HolisticResultFloat value : values) {
      if ( holVal.isFull() ) {
        LOG.warn("Adding an element to an already full HR. Key: " + 
                 key.toString() + 
                 " array size: " + holVal.getNeededCount() + 
                 " current elems: " + 
                 holVal.getCurrentCount() );
      }

      holVal.merge(value);
      context.getCounter(MedianCombinerFloatStatus.MERGED).increment(value.getCurrentCount());
    }

    // now, the remainig holistic result should be full. Check though
    if( holVal.isFull() ) {
      // apply whatever function you want, in this case we 
      // sort and then pull the median out
      holVal.sort();
      holVal.setFinal( holVal.getValues()[(holVal.getValues().length)/2] );
      context.getCounter(MedianCombinerFloatStatus.FULL).increment(1);
    } else {                                                                
      context.getCounter(MedianCombinerFloatStatus.NOTFULL).increment(1);
    }

    context.write(key, holVal);
  }
}
