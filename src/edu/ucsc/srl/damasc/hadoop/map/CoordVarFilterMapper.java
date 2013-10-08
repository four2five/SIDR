package edu.ucsc.srl.damasc.hadoop.map;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Path;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;

import edu.ucsc.srl.damasc.hadoop.io.ArraySpec;
import edu.ucsc.srl.damasc.hadoop.io.DataIterator;
import edu.ucsc.srl.damasc.hadoop.io.MultiVarData;
import edu.ucsc.srl.damasc.hadoop.Utils;
import edu.ucsc.srl.damasc.hadoop.Utils.FilterCounters;

import org.apache.hadoop.io.DoubleWritable;

/**GroupID
 * Dummy mapper, just passed data through with a dummy key.
 * This is used for testing purposes
 */
public class CoordVarFilterMapper extends Mapper<ArraySpec, MultiVarData, ArraySpec, DoubleWritable> {

  private static int DATATYPESIZE = Double.SIZE / 8; // we need this as bytes, not bits
  private static final Log LOG = LogFactory.getLog(CoordVarFilterMapper.class);

 /**
 * Reduces values for a given key
 * @param key ArraySpec representing the given Array being passed in
 * @param value an Array to process that corresponds to the given key 
 * @param context the Context object for the currently executing job
 */
  public void map(ArraySpec key, MultiVarData inMVD, Context context)
                  throws IOException, InterruptedException {

    String taskID = context.getTaskAttemptID().toString();
    String[] splitTaskID = taskID.split("_");
    Configuration conf = context.getConfiguration();

    LOG.info("map task " + splitTaskID[4] + " using array keys");

    try {

      long timer = System.currentTimeMillis();

      long elementCount = Utils.calcTotalSize(key.getShape());
      LOG.info("Array Spec has " + elementCount + " elements");
      String cachedFilePath = conf.get(Utils.CACHED_COORD_FILE_NAME);
      LOG.info("cached file: " + cachedFilePath);

      int[] extractionShape = Utils.getExtractionShape(conf,
                                                        key.getShape().length);

      LOG.info("Extraction shape is: " + Arrays.toString(extractionShape));
      int[] allOnes = new int[extractionShape.length];
      for( int i=0; i<allOnes.length; i++){
        allOnes[i] = 1;
      }

      ArraySpec arraySpec = new ArraySpec(key.getCorner(), "");
      DoubleWritable doubleW = new DoubleWritable();
      double lowFilter = new Double(Utils.getLowThreshold(conf));
      double highFilter = new Double(Utils.getHighThreshold(conf));
      float equalFilterF = Utils.getEqualValue(conf);
      double equalFilter = new Double(equalFilterF);

      String varName = Utils.getVariableName(conf);       
      String[] coordVarNames = Utils.getCoordinateVariableName(conf).split(",");
      ByteBuffer xbb = inMVD.getVarDataByName("coordx");
      ByteBuffer bb = inMVD.getVarDataByName(varName);
      int coordVarDim = Utils.getCoordinateVariableDimension(conf);
      
      LOG.info("in CoordVarFilterMapper, corner is: " + 
               Arrays.toString(key.getCorner()) + 
               " shape: " + Arrays.toString(key.getShape()) + 
               " varName: " + varName + 
               " exShape: " + Arrays.toString(extractionShape) +
               " bb has capacity(): " + bb.capacity()); 
      LOG.info("coordvarname[0]: " + coordVarNames[0] + 
               " xbb has capacity(): " + xbb.capacity() +
               " and is dim: " + coordVarDim + " in the target variable"
              );
      LOG.info("low: " + lowFilter + " high: " + highFilter + 
               " equal: " + equalFilter + " " + equalFilterF); 

      DataIterator dataItr = new DataIterator(bb, key.getCorner(),
                                              key.getShape(), extractionShape,
                                              DATATYPESIZE);

      LOG.info("dataItr dump: ");
      dataItr.dumpMetadata();

      // find which dimension in the actual dataset is the coordinate variable dimension

      int[] tempGroup;
      int[] outGroup = new int[3];
      long totalElements = 0;
      long groupElements = 0;
      double tempDouble = Double.MIN_VALUE;
      long localHitCounter = 0;
      long localMissCounter = 0;
      long globalHitCounter = 0;
      long globalMissCounter = 0;
      int[] tempArray = new int[extractionShape.length];
      int[] curReadPos = new int[extractionShape.length];
      int[] coordValCoordinates = new int[3]; // time, y, z
      double coordVarVal = 0;
      float coordVarValF = 0;
      arraySpec.setFileName(key.getFileName());
      LOG.info("setting int data filename to " + key.getFileName());

      while( dataItr.hasMoreGroups() ) { 
        tempGroup = dataItr.getNextGroup();
        groupElements = 0;

        arraySpec.setVariable(key.getVarName());
        arraySpec.setCorner(tempGroup);

        while( dataItr.groupHasMoreValues() ) { 
          tempDouble = dataItr.getNextValueDouble();
          curReadPos = dataItr.getCurrentReadCoordinate();
          coordVarVal = xbb.getDouble(curReadPos[coordVarDim] * DATATYPESIZE);  // -jbuck
          coordVarValF = (float)coordVarVal;

          // we perfer using == over high / low fileters, so test that first
          if (equalFilterF != Float.MAX_VALUE) {
            if (equalFilterF == coordVarValF) { 
              doubleW.set(tempDouble); 
              localHitCounter++;
            } else { 
              localMissCounter++;
            }
          }  else { 
            if ( coordVarValF > lowFilter && coordVarValF < highFilter) { 
              doubleW.set(tempDouble); 
              localHitCounter++;
            } else { 
              localMissCounter++;
            }
          }

          totalElements++;
          groupElements++;
        }

        if (localHitCounter > 0) { 
          Utils.mapToLocal(tempGroup, tempArray, arraySpec, extractionShape);
          LOG.info("writing: " + arraySpec + ":" + doubleW);
          context.write(arraySpec, doubleW);
        } 

        globalHitCounter += localHitCounter;
        localHitCounter = 0;
        globalMissCounter += localMissCounter;
        localMissCounter = 0;
      }

      
      timer = System.currentTimeMillis() - timer;

      System.out.println("Just wrote " + totalElements + " for map task " + 
                         splitTaskID[4] + " with key2: " +
                         arraySpec.toString() + 
                         " and it took " + timer + " ms"); 
      context.getCounter(FilterCounters.THRESHOLD_HIT).increment(globalHitCounter);
      context.getCounter(FilterCounters.THRESHOLD_MISS).increment(globalMissCounter);
      LOG.info("global valid: " + globalHitCounter + " invalid: " + globalMissCounter);

    } catch ( Exception e ) {
      System.out.println("Caught an exception in CoordVarFilterMapper.map()" + e.toString() );
      e.printStackTrace();
    }
  }
}
