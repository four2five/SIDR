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
import edu.ucsc.srl.damasc.hadoop.io.AverageResultFloat;
import edu.ucsc.srl.damasc.hadoop.io.MultiVarData;

/**
 * Dummy mapper, just passed data through with a dummy key.
 * This is used for testing purposes
 */
public class AverageMapperFloat extends Mapper<ArraySpec, MultiVarData, ArraySpec, AverageResultFloat> {

private static final Log LOG = LogFactory.getLog(AverageMapperFloat.class);

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
      System.out.println("Array Spec has corner " + Arrays.toString(key.getCorner()) + 
                         " and "  + elementCount + " elements");

      int[] extractionShape = Utils.getExtractionShape(context.getConfiguration(),
                                                        key.getShape().length);

      int[] allOnes = new int[extractionShape.length];
      for( int i=0; i<allOnes.length; i++){
        allOnes[i] = 1;
      }

      ArraySpec arraySpec = new ArraySpec(key.getCorner(), "");
      int extShapeSize = Utils.calcTotalSize(extractionShape);
      AverageResultFloat aRes = new AverageResultFloat();

    
      Configuration conf = context.getConfiguration();
      String varName = Utils.getVariableName(conf);
      ByteBuffer inArray = inMVD.getVarDataByName(varName);
      int datatypeSize = Utils.getDatatypeSize(conf);

      System.out.println("in mapper, corner is: " + 
                         Arrays.toString(key.getCorner()) + 
                         " shape: " + Arrays.toString(key.getShape()) + 
                         " extShape: " + Arrays.toString(extractionShape) + 
                         " extSize: " + extShapeSize + 
                         " datatypeSize: " + datatypeSize);


      DataIterator dataItr = new DataIterator(inArray, key.getCorner(),
                                                  key.getShape(), extractionShape,
                                                  datatypeSize);
      int[] tempGroup;
      int[] tempArray = new int[extractionShape.length];
      long totalElements = 0;
      long totalGroups = 0;
      //long perGroupTotal = 0;
      float runningPerGroupTotal = 0.0f;
      int perGroupCount = 0;

      while( dataItr.hasMoreGroups() ) { 
        tempGroup = dataItr.getNextGroup();
        //perGroupTotal = 0;
	runningPerGroupTotal = 0.0f;
        perGroupCount = 0;

        while( dataItr.groupHasMoreValues() ) { 
          perGroupCount++; // we increment first so that the next line never divides by zero

          // We compute the running average by added the diffence between the current value and the total,
          // dividing the different by the number of samples so that it has the appropriate affect on the average
          //runningPerGroupTotal += (dataItr.getNextValueFloat() - runningPerGroupTotal) / perGroupCount;
          
          // use the Class' logic for buffering floats
          aRes.addValue(dataItr.getNextValueFloat());
        }

        //aRes.setValue((float)perGroupTotal/perGroupCount, perGroupCount);
        //aRes.setValue(runningPerGroupTotal, perGroupCount);
        Utils.mapToLocal(tempGroup, tempArray, arraySpec, extractionShape);
      
        totalElements += perGroupCount;
        totalGroups++;

        arraySpec.setVariable(key.getVarName());
        context.write(arraySpec, aRes, perGroupCount);
        System.out.println("Group: " + arraySpec + " had " + perGroupCount +
                           " elements in this input split. Value: " + runningPerGroupTotal);
        System.out.println("ares value: " + aRes.getCurrentValueFloat() + " count: " + aRes.getCurrentCount());

        // reset the accumulator object
        aRes.clear();
      }

      timer = System.currentTimeMillis() - timer;

      System.out.println("Just wrote " + totalElements + " for map task " + 
                         task.getId() + " with key2: " +
                         arraySpec.toString() + 
                         " and it took " + timer + " ms"); 
      System.out.println("In total, wrote " + totalElements + " for "  + totalGroups + " groups");

    } catch ( Exception e ) {
      System.out.println("Caught an exception in AverageMapperFloat.map()" + e.toString() );
      e.printStackTrace();
    }
  }
}
