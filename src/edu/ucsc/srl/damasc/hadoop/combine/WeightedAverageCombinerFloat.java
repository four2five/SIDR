package edu.ucsc.srl.damasc.hadoop.combine;

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
import edu.ucsc.srl.damasc.hadoop.io.WeightedAverageResultFloat;

/**
 * Combiner that simply iterates through the data it is passed
 */
public class WeightedAverageCombinerFloat extends 
        Reducer<ArraySpec, WeightedAverageResultFloat, ArraySpec, WeightedAverageResultFloat> {

  private static final Log LOG = LogFactory.getLog(WeightedAverageCombinerFloat.class);

  private int[] _extractionShape;
  private long _extShapeSize;

  public void setup(Context context) throws IOException, InterruptedException { 
    super.setup(context);

    TaskAttemptID attempt = context.getTaskAttemptID();
    TaskID task = attempt.getTaskID();
    Configuration conf = context.getConfiguration();

    LOG.info("in combiner().setup for task: " + task.getId());
     
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
    LOG.info("Combine(): " + task.getId() + " of " + numReducers + 
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
  public void reduce(ArraySpec key, Iterable<WeightedAverageResultFloat> values, 
                     Context context)
                     throws IOException, InterruptedException {

    // sanity test, bail if extraction shape is null
    if (null == this._extractionShape) { 
      return;
    }

    long timer = System.currentTimeMillis();
    long perGroupTotal = 0;
    int perGroupCount = 0;

    WeightedAverageResultFloat waRes = new WeightedAverageResultFloat();

    for (WeightedAverageResultFloat value : values) {
      System.out.println("in AvgRedFl, Merging in value: " + value.getCurrentValueFloat() + 
                         " count: " + value.getCurrentCount());
      System.out.println("\tcurrent value: " + waRes.getCurrentValueFloat() + 
                         " count: " + waRes.getCurrentCount());
      waRes.addWeightedAverageResultFloat(value);
      System.out.println("\t\tpost merge value: " + waRes.getCurrentValueFloat() +
                         " count: " + waRes.getCurrentCount());
    }

    System.out.println("context.write() key: " + key +
                       " value: " + waRes.getCurrentValue() +
                       " count: " + waRes.getCurrentCount());
    context.write(key, waRes, waRes.getCurrentCount()); 

    timer = System.currentTimeMillis() - timer;
  }
}
