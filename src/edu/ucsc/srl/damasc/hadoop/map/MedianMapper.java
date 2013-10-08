package edu.ucsc.srl.damasc.hadoop.map;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;

import edu.ucsc.srl.damasc.hadoop.Utils;
import edu.ucsc.srl.damasc.hadoop.io.ArraySpec;
import edu.ucsc.srl.damasc.hadoop.io.DataIterator;
import edu.ucsc.srl.damasc.hadoop.io.HolisticResult;
import edu.ucsc.srl.damasc.hadoop.io.MultiVarData;
//import java.lang.Thread;
//import org.apache.hadoop.io.IntWritable;
//import edu.ucsc.srl.damasc.hadoop.io.HolisticResult2;

/*
import ucar.ma2.Array;
import ucar.ma2.ArrayInt;
*/

/**
 * Dummy mapper, just passed data through with a dummy key.
 * This is used for testing purposes
 */
public class MedianMapper extends Mapper<ArraySpec, MultiVarData, ArraySpec, HolisticResult> {

  private static int DATATYPESIZE = 4;
  @SuppressWarnings("unused")
private static final Log LOG = LogFactory.getLog(MedianMapper.class);

 /**
 * Reduces values for a given key
 * @param key ArraySpec representing the given Array being passed in
 * @param value an Array to process that corresponds to the given key 
 * @param context the Context object for the currently executing job
 */
  public void map(ArraySpec key, MultiVarData inMVD, Context context)
                  throws IOException, InterruptedException {

    TaskAttemptID attempt = context.getTaskAttemptID();
    TaskID task = attempt.getTaskID();

    try {

      long timer = System.currentTimeMillis();
     
      long elementCount = Utils.calcTotalSize(key.getShape());
      System.out.println("Array Spec has " + elementCount + " elements");

      int[] extractionShape = Utils.getExtractionShape(context.getConfiguration(),
                                                        key.getShape().length);

      int[] allOnes = new int[extractionShape.length];
      for( int i=0; i<allOnes.length; i++){
        allOnes[i] = 1;
      }

      ArraySpec arraySpec = new ArraySpec(key.getCorner(), "");
      //System.out.println("Creating a HolisticResult2 with capacity: " + 
        //Utils.calcTotalSize(extractionShape));
      int extShapeSize = Utils.calcTotalSize(extractionShape);
      HolisticResult result = new HolisticResult(extShapeSize);

      System.out.println("in mapper, corner is: " + 
                         Utils.arrayToString(key.getCorner()) + 
                         " shape: " + Utils.arrayToString(key.getShape())
                         + " extsize: " + extShapeSize + 
                         " extShape: " + Arrays.toString(extractionShape));
     
      Configuration conf = context.getConfiguration();
      String varName = Utils.getVariableName(conf);
      ByteBuffer inArray = inMVD.getVarDataByName(varName);

      DataIterator dataItr = new DataIterator(inArray, key.getCorner(),
                                                  key.getShape(), extractionShape,
                                                  DATATYPESIZE);
      int[] tempGroup;
      int[] tempArray = new int[extractionShape.length];
      long totalElements = 0;
      int medianValue = 0;
      int perGroupCount = 0;

      while( dataItr.hasMoreGroups() ) { 
        tempGroup = dataItr.getNextGroup();
        result.clear(); // reset the holistic result
        result.setNeededCount(extShapeSize);
        perGroupCount = 0;


        while( dataItr.groupHasMoreValues() ) { 
          result.setValue(dataItr.getNextValueInt());
          totalElements++;
          perGroupCount++;
        }

        if( result.isFull() ) {
          result.sort();
          medianValue = result.getValue(result.getCurrentCount()/2);
          result.setFinal(medianValue);
        } else {
        }

        arraySpec.setVariable(key.getVarName());
        Utils.mapToLocal(tempGroup, tempArray, arraySpec, extractionShape);
        //LOG.info("Key: " + groupID  + " keyCount " + perGroupCount);
        context.write(arraySpec, result, perGroupCount);

        //System.out.println("Just wrote " + perGroupCount + " for map task " + 
        //                  " with key2: " +
        //                  arraySpec.toString()); 
      }

      /*
      long totalElements = Utils.calcTotalSize( key.getShape() );
      for( int i=0; i<totalElements; i++) { 
        //System.out.println(i + ":" + Array.getLong(hiddenArray, i));
        longW.set(Array.getLong(hiddenArray,i));
        context.write(groupID, longW);
      }
      */

      
      timer = System.currentTimeMillis() - timer;

      /*
      System.out.println("Just wrote " + totalElements + " for map task " + 
                         task.getId() + " with key2: " +
                         arraySpec.toString() + 
                         " and it took " + timer + " ms"); 
      */
      //context.write(groupID, intW);
    } catch ( Exception e ) {
      System.out.println("Caught an exception in MedianMapper.map()" + e.toString() );
      e.printStackTrace();
    }
  }
}
