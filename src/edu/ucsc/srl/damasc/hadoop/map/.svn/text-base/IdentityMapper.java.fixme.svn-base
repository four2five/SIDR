package edu.ucsc.srl.damasc.hadoop.map;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import edu.ucsc.srl.damasc.hadoop.Utils;
import edu.ucsc.srl.damasc.hadoop.io.ArraySpec;
import edu.ucsc.srl.damasc.hadoop.io.DataIterator;
//import java.lang.Thread;
//import org.apache.hadoop.io.IntWritable;

/*
import ucar.ma2.Array;
import ucar.ma2.ArrayInt;
*/

/**
 * Dummy mapper, just passed data through with a dummy key.
 * This is used for testing purposes
 */
public class IdentityMapper extends Mapper<ArraySpec, ByteBuffer, ArraySpec, IntWritable> {

  private static int DATATYPESIZE = 4;
 /**
 * Reduces values for a given key
 * @param key ArraySpec representing the given Array being passed in
 * @param value an Array to process that corresponds to the given key 
 * @param context the Context object for the currently executing job
 */
  public void map(ArraySpec key, ByteBuffer inArray, Context context)
                  throws IOException, InterruptedException {

    String taskID = context.getTaskAttemptID().toString();
    //System.out.println("taskID: " + taskID);
    String[] splitTaskID = taskID.split("_");

    System.out.println("map task " + splitTaskID[4] + " using array keys");
    System.out.println("Speaker Ender");
    try {

    long timer = System.currentTimeMillis();
      //ArrayInt intArray = (ArrayInt)value;
      //E[] dataArray = value;
     
      long elementCount = Utils.calcTotalSize(key.getShape());
      System.out.println("Array Spec has " + elementCount + " elements");

      int[] extractionShape = Utils.getExtractionShape(context.getConfiguration(),
                                                        key.getShape().length);

      int[] allOnes = new int[extractionShape.length];
      for( int i=0; i<allOnes.length; i++){
        allOnes[i] = 1;
      }

      ArraySpec arraySpec = new ArraySpec(key.getCorner(), "");
      IntWritable intW = new IntWritable(Integer.MIN_VALUE);

      System.out.println("in mapper, corner is: " + 
                         Utils.arrayToString(key.getCorner()) + 
                         " shape: " + Utils.arrayToString(key.getShape()));
          //Utils.getVariableName(context.getConfiguration()) );

      //ByteBuffer hiddenArray = (ByteBuffer)inArray[0];
     
      DataIterator dataItr = new DataIterator(inArray, key.getCorner(),
                                                  key.getShape(), extractionShape,
                                                  DATATYPESIZE);
      int[] tempGroup;
      long totalElements = 0;
      while( dataItr.hasMoreGroups() ) { 
        tempGroup = dataItr.getNextGroup();

        if( tempGroup.length < 4) { 
          System.out.println("dataItr.getNextGroup just returned group: " + 
                             Utils.arrayToString(tempGroup));
        }
        //System.out.println("\tlooping over group " + Utils.arrayToString(tempGroup));
        arraySpec.setVariable(key.getVarName());
        arraySpec.setCorner(tempGroup);


        while( dataItr.groupHasMoreValues() ) { 
          intW.set(dataItr.getNextValueInt());

          /*
          if( arraySpec.getCorner().length < 4) { 
            System.out.println("in mapper, getGroupID.length is " + 
            		arraySpec.getCorner().length);
            Thread.dumpStack();
          }
          */

          context.write(arraySpec, intW, (long)1);
          totalElements++;
        }
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

      System.out.println("Just wrote " + totalElements + " for map task " + 
                         splitTaskID[4] + " with key2: " +
                         arraySpec.toString() + 
                         " and it took " + timer + " ms"); 

      //context.write(groupID, intW);
    } catch ( Exception e ) {
      System.out.println("Caught an exception in IdentityMapper.map()" + e.toString() );
      e.printStackTrace();
    }
  }
}
