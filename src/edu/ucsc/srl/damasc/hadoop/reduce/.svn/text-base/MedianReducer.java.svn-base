package edu.ucsc.srl.damasc.hadoop.reduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;

//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;

//import edu.ucsc.srl.damasc.hadoop.HDF5Utils;
//import edu.ucsc.srl.damasc.hadoop.Utils;
import edu.ucsc.srl.damasc.hadoop.HadoopUtils;
import edu.ucsc.srl.damasc.hadoop.io.ArraySpec;
import edu.ucsc.srl.damasc.hadoop.io.HolisticResult;

/**
 * Reducer that simply iterates through the data it is passed
 */
public class MedianReducer extends 
        Reducer<ArraySpec, HolisticResult, ArraySpec, HolisticResult> {

  private static final Log LOG = LogFactory.getLog(MedianReducer.class);

  private int[] _extractionShape;
  private long _extShapeSize;

  public void setup(Context context) throws IOException, InterruptedException { 
    super.setup(context);

  
    //int taskID = HadoopUtils.parseTaskID( context.getTaskAttemptID().toString()); 
    TaskAttemptID attempt = context.getTaskAttemptID();
    TaskID task = attempt.getTaskID();
    //System.out.println("in reducer.setup(), calling getMapTasksForReducer");
    Configuration conf = context.getConfiguration();
    if( null == conf)  {
      LOG.info("in MedianReducer.setup(), conf is non-existent");
    }

    LOG.info("in reduce().setup for task: " + task.getId());
     
    int[] outputCornerForThisReducer = 
      HadoopUtils.getOutputCornerForReducerN(task.getId(), conf );

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
    //int[] mapTasks = HDF5HadoopUtils.getMapTasksForReducer(taskID, context.getConfiguration());
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
  public void reduce(ArraySpec key, Iterable<HolisticResult> values, 
                     Context context)
                     throws IOException, InterruptedException {

    //System.out.println("in reducer, key: " + key.toString() );
    long timer = System.currentTimeMillis();

    // debug test
    //LongWritable maxVal = new LongWritable();
    //maxVal.set(Long.MIN_VALUE);

    //IntWritable intW = new IntWritable();
    HolisticResult holResult = null;

    try{ 
      holResult = new HolisticResult(HadoopUtils.calcTotalSize(this._extractionShape));
    } catch ( Exception e ) { 
      e.printStackTrace();
    }

    for (HolisticResult value : values) {
      LOG.debug("Merging in " + value.getCurrentCount() + " elements for key: " + key.toString());
      holResult.merge(value);
    }

    // test if we've filled out the holistic result
    if( holResult.isFinal() ) { 
      //LOG.info("key: " + key.toString() + " was already final.");
      //System.out.println("key: " + key.toString() + " was already final.");
      context.write(key, holResult, this._extShapeSize); 
    } else { 
      if( !holResult.isFull() ) { 
        LOG.info("key: " + key.toString() + " NOT set to final: ");
        //System.out.println("key: " + key.toString() + " NOT set to final: ");
        //LOG.info("key: " + key.toString() + " with " + 
        //System.out.println("key: " + key.toString() + " with " + 
         //                  holResult.getCurrentCount() + " / " + 
         //                  holResult.getNeededCount() );
        //LOG.info("results[" + (holResult.getCurrentCount() / 2 ) + "]: " +
        //System.out.println("results[" + (holResult.getCurrentCount() / 2 ) + "]: " +
        //                    (holResult.getValue(holResult.getCurrentCount()/2)) );
      } else { 
        //LOG.info("key: " + key.toString() + " is full.");
      }
      holResult.shrinkValuesArray(); // shrink the array down to the current size
      holResult.sort();
      int medianValue = holResult.getValue(holResult.getCurrentCount()/2);
      holResult.setFinal(medianValue);
      context.write(key, holResult, holResult.getCurrentCount()); 
    }

    //intW.set((int)(groupTotal/groupCount));

    timer = System.currentTimeMillis() - timer;
    //LOG.info("total reducer took: " + timer + " ms");
  }
}
