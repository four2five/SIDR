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
import edu.ucsc.srl.damasc.hadoop.io.HolisticResultFloat;
import edu.ucsc.srl.damasc.hadoop.io.MultiVarData;

/**
 */
public class MedianMapperFloat extends Mapper<ArraySpec, MultiVarData, ArraySpec, HolisticResultFloat> {

  @SuppressWarnings("unused")
private static final Log LOG = LogFactory.getLog(MedianMapperFloat.class);

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
      int extShapeSize = Utils.calcTotalSize(extractionShape);
      HolisticResultFloat result = new HolisticResultFloat(extShapeSize);

      Configuration conf = context.getConfiguration();
      String varName = Utils.getVariableName(conf);
      ByteBuffer inArray = inMVD.getVarDataByName(varName);
      int datatypeSize = Utils.getDatatypeSize(conf);

      System.out.println("in mapper, corner is: " + 
                         Arrays.toString(key.getCorner()) + 
                         " shape: " + Arrays.toString(key.getShape())
                         + " extsize: " + extShapeSize + 
                         " extShape: " + Arrays.toString(extractionShape) + 
                         " datatypeSize: " + datatypeSize);

      DataIterator dataItr = new DataIterator(inArray, key.getCorner(),
                                              key.getShape(), extractionShape,
                                              datatypeSize);
      int[] tempGroup;
      int[] tempArray = new int[extractionShape.length];
      long totalElements = 0;
      float medianValue = 0;
      int perGroupCount = 0;
      float tempVal = -1;
      int testGroup[] = {0,0,0,0};

      while( dataItr.hasMoreGroups() ) { 
        tempGroup = dataItr.getNextGroup();
        result.clear(); // reset the holistic result
        result.setNeededCount(extShapeSize);
        perGroupCount = 0;


        while( dataItr.groupHasMoreValues() ) { 
          tempVal = dataItr.getNextValueFloat();
          //result.setValue(dataItr.getNextValueFloat());
          if (Arrays.equals(tempGroup, testGroup)) { 
            LOG.info("  v: " + tempVal);
          }
          result.setValue(tempVal);
          totalElements++;
          perGroupCount++;
        }

        if( result.isFull() ) {
          result.sort();
          medianValue = result.getValue(result.getCurrentCount()/2);
          result.setFinal(medianValue);
        }

        arraySpec.setVariable(key.getVarName());
        Utils.mapToLocal(tempGroup, tempArray, arraySpec, extractionShape);
        context.write(arraySpec, result, perGroupCount);
      }

      timer = System.currentTimeMillis() - timer;
    } catch ( Exception e ) {
      System.out.println("Caught an exception in MedianMapperFloat.map()" + e.toString() );
      e.printStackTrace();
    }
  }
}