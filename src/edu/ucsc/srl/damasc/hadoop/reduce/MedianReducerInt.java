package edu.ucsc.srl.damasc.hadoop.reduce;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;

import edu.ucsc.srl.damasc.hadoop.HadoopUtils;
import edu.ucsc.srl.damasc.hadoop.io.ArraySpec;
import edu.ucsc.srl.damasc.hadoop.io.HolisticResultInt;

/**
 * Reducer that simply iterates through the data it is passed
 */
public class MedianReducerInt extends 
        Reducer<ArraySpec, HolisticResultInt, ArraySpec, HolisticResultInt> {

  private static final Log LOG = LogFactory.getLog(MedianReducerInt.class);

  private int[] _extractionShape;
  private long _extShapeSize;

  public void setup(Context context) throws IOException, InterruptedException { 
    super.setup(context);

    TaskAttemptID attempt = context.getTaskAttemptID();
    TaskID task = attempt.getTaskID();
    Configuration conf = context.getConfiguration();
    if( null == conf)  {
      LOG.info("in MedianReducerInt.setup(), conf is non-existent");
    }

    LOG.info("in reduce().setup for task: " + task.getId());
     
    int[] outputCornerForThisReducer = 
      HadoopUtils.getOutputCornerForReducerN(task.getId(), conf );

    // if this happens, something is seriously wrong.
    // Basically, we should punt as we can't process
    // data correctly
    if (null == outputCornerForThisReducer) { 
      this._extractionShape = null;
      return;
    }

    int numReducers = HadoopUtils.getNumberReducers(conf);

    int[] outputShapeForThisReducer = 
      HadoopUtils.getReducerWriteShape( task.getId(), conf);

    int[] totalGlobalOutputSpace = HadoopUtils.getTotalOutputSpace(conf);

    this._extractionShape = 
      HadoopUtils.getExtractionShape(conf, outputCornerForThisReducer.length);

    this._extShapeSize = HadoopUtils.calcTotalSize(this._extractionShape);
    LOG.info("Reduce(): " + task.getId() + " of " + numReducers + 
                    " write corner: " +  Arrays.toString(outputCornerForThisReducer) + 
                    " shape: " + Arrays.toString(outputShapeForThisReducer) + 
                    " totalOutputSpace: " + Arrays.toString(totalGlobalOutputSpace));
  }


  /**
   * Iterates through the data it is passed, doing nothing to it. Outputs a 
   * Integer.MINIMUM_VALUE as the value for its key
   * @param key the flattened corner for this instance of the extraction shape 
   * in the global logical space
   * @param values an Iterable list of IntWritable objects that represent all the inputs
   * for this key
   * @param context the Context object for the executing program
   */
  public void reduce(ArraySpec key, Iterable<HolisticResultInt> values, 
                     Context context)
                     throws IOException, InterruptedException {

    // sanity test, bail if extraction shape is null
    if (null == this._extractionShape) { 
      return;
    }

    long timer = System.currentTimeMillis();

    HolisticResultInt holResult = null;

    try{ 
      holResult = new HolisticResultInt(HadoopUtils.calcTotalSize(this._extractionShape));
    } catch ( Exception e ) { 
      e.printStackTrace();
    }

    for (HolisticResultInt value : values) {
      LOG.debug("Merging in " + value.getCurrentCount() + " elements for key: " + key.toString());
      holResult.merge(value);
    }

    // test if we've filled out the holistic result
    if( holResult.isFinal() ) { 
      context.write(key, holResult, this._extShapeSize); 
    } else { 
      if( !holResult.isFull() ) { 
        LOG.info("key: " + key.toString() + " NOT set to final: ");
      } 
      holResult.shrinkValuesArray(); // shrink the array down to the current size
      holResult.sort();
      int medianValue = holResult.getValue(holResult.getCurrentCount()/2);
      holResult.setFinal(medianValue);
      context.write(key, holResult, holResult.getCurrentCount()); 
    }

    timer = System.currentTimeMillis() - timer;
  }
}
