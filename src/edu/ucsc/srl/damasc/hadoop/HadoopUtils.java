package edu.ucsc.srl.damasc.hadoop;

import java.io.IOException;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;

import edu.ucsc.srl.damasc.hadoop.io.HadoopArraySpec;
import edu.ucsc.srl.damasc.hadoop.io.HadoopGroupID;

/**
 * A collection of methods that are used in various parts of the code to read / write
 * global configuration data as well as some small helper functions
 */
public class HadoopUtils {

    private static final Log LOG = LogFactory.getLog(HadoopUtils.class);
    public static final String PARTITIONER_CLASS = "damasc.partitioner_class";
    public static final String REDUCER_SHAPE_WEIGHT = "damasc.reducer_shape_weight";
    public static final String CEPH_DEFAULT_URI = "damasc.ceph_uri";
    public static final String VARIABLE_NAME = "damasc.variable_name";
    public static final String SAMPLE_RATIO = "damasc.sample_ratio";
    public static final String VARIABLE_SHAPE_PREFIX = "damasc.variable_shape";
    public static final String LOW_FILTER = "damasc.low_filter";
    public static final String HIGH_FILTER = "damasc.high_filter";
    public static final String PART_MODE = "damasc.partition_mode";
    public static final String PLACEMENT_MODE = "damasc.placement_mode";
    public static final String NO_SCAN = "damasc.noscan";
    public static final String USE_COMBINER = "damasc.use_combiner";
    public static final String HOLISTIC = "damasc.holistic";
    public static final String QUERY_DEPENDANT = "damasc.query_dependant";
    public static final String EXTRACTION_SHAPE = "damasc.extraction_shape";
    public static final String NUMBER_REDUCERS = "damasc.number_reducers";
    public static final String OPERATOR = "damasc.operator";
    public static final String DEBUG_LOG_FILE = "damasc.logfile";
    public static final String BUFFER_SIZE = "damasc.buffer_size";
    public static final String MULTI_FILE_MODE = "damasc.multi_file_mode";
    public static final String CEPH_CONF_PATH = "damasc.ceph_conf_path";
    public static final String FILE_NAME_ARRAY = "damasc.file_name_array";
    public static final String FS_TYPE = "damasc.fs_type";
    public static final String DEFAULT_BUFFER_SIZE = "1048576";
    public static final String REDUCER_KEY_LIMIT = 
      "damasc.reducer.key_limit";
    public static final String DEFAULT_OUTPUT_DATATYPE_SIZE = "1";
    public static final String OUTPUT_DATATYPE_SIZE = "damasc.output.datatype.size";
    public static final String DYNAMIC_REDUCER_START = "damasc.reducer.dynamic_start";

    private static boolean startReducerDynamically;
    private static float sampleRatio;
    private static float reducerShapeWeight;
    private static int[] variableShape;
    private static long validLow;
    private static long validHigh;
    private static Operator operator = Operator.optUnknown;
    private static PartitionerClass partitionerClass = PartitionerClass.arrayspec;
    private static PartMode partMode = PartMode.proportional;
    private static PlacementMode placementMode = PlacementMode.roundrobin;
    private static FSType fsType = FSType.hdfs;
    private static int[] extractionShape;
    private static boolean holisticEnabled = false;
    private static boolean queryDependantEnabled = false;
    private static boolean noScanEnabled = false;
    private static boolean useCombiner = false;
    private static int numberReducers = 1;
    private static String debugLogFile = "";
    private static int bufferSize = -1;
    private static MultiFileMode multiFileMode = MultiFileMode.combine;
    private static String cephConfPath = "";
    private static int outputDataTypeSize = -1;
    private static long reducerKeyLimit = -1; 


    public HadoopUtils() {
    }

    public static enum Operator { average, simpleMax, max, simpleMedian, median, nulltest, optUnknown }

    // partitioning scheme 
    public static enum PartMode{ proportional, record, calculated }

    public static enum PartitionerClass{ hash, arrayspec}

    public static enum PlacementMode{ roundrobin, sampling, implicit}

    public static enum MultiFileMode{ combine, concat}

    public static enum FSType { hdfs, ceph, unknown }
    /**
     * Adds a file name to the fileNameArray variable in conf if it doesn't exist in there already
     * @param fileName name of the file to add to fileNameArray
     * @param conf Configuration object for the currently executing program
     * @return returns the index of the fileName we just added
     */
    public static int addFileName(String fileName, Configuration conf) {
      ArrayList<String> fileNameArray = new ArrayList<String>(conf.getStringCollection(FILE_NAME_ARRAY));
      fileNameArray.add(fileName);
      int retVal = fileNameArray.indexOf(fileName);

      conf.setStrings(FILE_NAME_ARRAY, stringArrayToString(fileNameArray));

      return retVal;
    }

    /**
     * Helper function that converts a Collection<String> into a single comma-delimited  String
     * @param strings a Collection of String objects
     * @return a single String containing all the entries in the Collection passed in
     */
    public static String stringArrayToString(Collection<String> strings) {
      String retString = "";
      int numElements = 0;
      for ( String s : strings ) {
        if ( numElements > 0) {
          retString += "," + s;
        } else {
          retString = s;
        } 
        numElements++;
      }

      return retString;
    }

    /**
     * Sets the Variable Shape for the current job. 
     * @param conf Configuration object for the current program
     * @param variableShape a comma delimited string representing the shape of the variable being
     * processed by the current job
     */
    
    public static void setVariableShape(Configuration conf, int[] inVariableShape) {
      String inVariableAsString = Arrays.toString(inVariableShape);
      LOG.debug("in HadoopUtils, setting VariableShape to: " + inVariableAsString);
      conf.set( VARIABLE_SHAPE_PREFIX, inVariableAsString);

      variableShape = inVariableShape;
    }
    

    /**
     * Get the shape of the Variable as an n-dimensional array
     * @param conf Configuration object for the current program
     * @return the shape of the variable being processed by the current job as
     * an n-dimensional array
     */
    public static int[] getVariableShape(Configuration conf) {
      String dimString = conf.get(VARIABLE_SHAPE_PREFIX, "");
      // sanity checking
      if (dimString == "") { 
        return null;
      } else { 
        String openB = Pattern.quote("[");
        String closeB = Pattern.quote("]");
        dimString = dimString.replaceAll(openB,"");
        dimString = dimString.replaceAll(closeB,"");
        dimString = dimString.replaceAll("\\s+", "");
      }

      System.out.println("JB, here's that string" + dimString);
      String[] dimStrings = dimString.split(",");
      variableShape = new int[dimStrings.length];

      for( int i=0; i<variableShape.length; i++) {
        variableShape[i] = Integer.parseInt(dimStrings[i]);
      }

      return variableShape.clone();
    }

    /**
     * Get the name of the variable currently being processed
     * @param conf Configuration object for the current job
     * @return the name of the variable being processed by the current job
     */
    public static String getVariableName(Configuration conf) {
      String varString = conf.get(VARIABLE_NAME, "");
      return varString;
    }

    /**
     * Get the name of the log file that is being logged to. Potentially an 
     * empty String
     * @param conf Configuration object for the current job
     * @return the file path for the debug file. Possibly an empty string
     */
    public static String getDebugLogFileName(Configuration conf) {
      debugLogFile = conf.get(DEBUG_LOG_FILE, "");
        
      return debugLogFile;
    }

    /**
     * Return the configured buffer size.  
     * @param conf Configuration object for the current job
     * @return the configured buffer size for the currently executing program
     */
    public static int getBufferSize(Configuration conf) { 
      bufferSize = Integer.parseInt(conf.get(BUFFER_SIZE, DEFAULT_BUFFER_SIZE));

      return bufferSize;
    }

    
    public static void setOutputDataTypeSize( Configuration conf, int size ) { 
      conf.set(OUTPUT_DATATYPE_SIZE, Integer.toString(size));
      outputDataTypeSize = size;
    }
    

    public static int getOutputDataTypeSize(Configuration conf) { 
      outputDataTypeSize = Integer.parseInt(
        conf.get(OUTPUT_DATATYPE_SIZE, DEFAULT_OUTPUT_DATATYPE_SIZE));
      return outputDataTypeSize;
    }

    public static String getCephConfPath(Configuration conf) { 
      cephConfPath = getCephConfPathString(conf);

      return cephConfPath;
    }

    /**
     * Get the configured Multiple File mode for the current job
     * @param conf Configuration object for the current job
     * @return a MultiFileMode entry, specifing how to process multiple files
     */
    public static MultiFileMode getMultiFileMode(Configuration conf) { 
      String multiFileModeString = getMultiFileModeString(conf);
      multiFileMode = parseMultiFileMode( multiFileModeString );

      return multiFileMode;
    }

    public static boolean startReducerDynamically(Configuration conf) {
      startReducerDynamically = conf.getBoolean(DYNAMIC_REDUCER_START, false);

      return startReducerDynamically;
    }

    /**
     * Get the configured sampling ratio for the current program. 
     * Specifies what percentage of the given data set to sample
     * for placement.
     * @param conf Configuration object for the current job
     * @return the sample ratio as a float between 0 and 1 
     */
    public static float getSampleRatio(Configuration conf) {
      sampleRatio = conf.getFloat(SAMPLE_RATIO, (float)0.01);

      return sampleRatio;
    }

    public static float getReducerShapeWeight(Configuration conf) {
        int numReducers = getNumberReducers(conf);
        float defaultWeight = (float)1/ (numReducers * 2 ); // reasonable default?
        reducerShapeWeight = conf.getFloat(REDUCER_SHAPE_WEIGHT, defaultWeight);

        return reducerShapeWeight;
    }

    public static long getReducerKeyLimit(Configuration conf) {
      reducerKeyLimit = 
      conf.getLong(REDUCER_KEY_LIMIT, 
                   (long)-1);
      return reducerKeyLimit;
    }

    /**
     * Get the "low" value for filtering out data on the record dimension
     * @param conf Configuration object for the current job
     * @return the lowest valid value on the record dimension
     */
    public static long getValidLow(Configuration conf) {
      validLow = conf.getLong(HadoopUtils.LOW_FILTER, Long.MIN_VALUE);
      validHigh = conf.getLong(HadoopUtils.HIGH_FILTER, Long.MAX_VALUE);
      return validLow;
    }

    /**
     * Get the "high" value for filtering out data on the record dimension
     * @param conf Configuration object for the current job
     * @return the highest valid value on the record dimension
     */
    public static long getValidHigh(Configuration conf) {
      validLow = conf.getLong(HadoopUtils.LOW_FILTER, Long.MIN_VALUE);
      validHigh = conf.getLong(HadoopUtils.HIGH_FILTER, Long.MAX_VALUE);
      return validHigh;
    }

    /**
     * Get a String indiciating whether the No Scan feature is enabled
     * @param conf Configuration object for the current job
     * @return a String that is either "TRUE" or "FALSE, depending on if the
     * No Scan feature is enabled or not
     */
    public static String getNoScanString(Configuration conf) {
      return conf.get(HadoopUtils.NO_SCAN, "TRUE");  // default to true (no scan enabled)
    }

    /**
     *  Get a String indicating how many Reducers this job is configured for
     * @param conf Configuration object for the current job
     * @return a String containing the number of Reducers that this job is 
     * configured for
     */
    public static String getNumberReducersString(Configuration conf) {
      return conf.get(HadoopUtils.NUMBER_REDUCERS, "1"); // default to one reducer
    }

    /**
     * Get a String indicating whether a combiner should be employed for this job
     * @param conf Configuration object for the current job
     * @return a String with either "TRUE" or "FALSE", depending on if a combiner 
     * should be used for this job
     */
    public static String getUseCombinerString(Configuration conf) {
      return conf.get(HadoopUtils.USE_COMBINER, "TRUE");  // default to true (combiner enabled)
    }

    /**
     * Get a String indiciating whether the Query Dependent partitioning
     * feature should be used
     * @param conf Configuration object for the current job
     * @return a String with either "TRUE" or "FALSE", depending on whether
     * Query Aware partitioning should be used for this job
     */
    public static String getQueryDependantString(Configuration conf) {
      return conf.get(HadoopUtils.QUERY_DEPENDANT, "FALSE"); // default to false (not query dependent)
    }

    /**
     * Get a String indicating if the current job is a Holistic function 
     * @param conf Configuration object for the current job
     * @return a String with either "TRUE" or "FALSE" depending on whether
     * this program is applying a Holistic function
     */
    public static String getHolisticString(Configuration conf) {
      return conf.get(HadoopUtils.HOLISTIC, "FALSE"); // default to false (not holistic)
    }

    /**
     * Get a String indicating which Operator is being applied
     * @param conf Configuration object for the current job
     * @return a String with the name of the operator being applied by 
     * this program
     */
    public static String getOperatorString(Configuration conf) {
      return conf.get(HadoopUtils.OPERATOR, "");
    }

    /**
     * Get a String indicating which Mode is being used for 
     * partitioning
     * @param conf Configuration object for the current job
     * @return a String containing the Partitioning mode 
     */
    public static String getPartModeString(Configuration conf) {
      return conf.get(HadoopUtils.PART_MODE, "Record");
    }

    public static String getPartitionerClassString(Configuration conf) { 
      return conf.get(HadoopUtils.PARTITIONER_CLASS, "arrayspec");
    }

    public static String getFSTypeString(Configuration conf) {
      return conf.get(HadoopUtils.FS_TYPE, "unknown");
    }

    /**
     * Get a String indicating which Mode is being used for 
     * placement 
     * @param conf Configuration object for the current job
     * @return a String containing the Placement mode 
     */
    public static String getPlacementModeString(Configuration conf) {
      return conf.get(HadoopUtils.PLACEMENT_MODE, "Sampling");
    }

    /**
     * Get a String indicating which Mode is being used to
     * address dealing with multiple files in the input
     * @param conf Configuration object for the current job
     * @return a String containing the multiple file mode 
     */
    public static String getMultiFileModeString(Configuration conf) {
      return conf.get(HadoopUtils.MULTI_FILE_MODE, "concat");
    }
 
    // use /etc/ceph/ceph.conf as the default Ceph configuration file path
    public static String getCephConfPathString(Configuration conf) {
      LOG.debug("CEPH_CONF_PATH: " + HadoopUtils.CEPH_CONF_PATH);
      String foo = conf.get(HadoopUtils.CEPH_CONF_PATH, "/etc/ceph/ceph.conf");
      LOG.debug("foo: " + foo);
      return foo;
    }

    /**
     * Parse a String containing the number of Reducers for this job
     * @param numberReducersString a String containing the number of 
     * reducers for the current job
     * @return the number of reducers for the current job
     */
    public static int parseNumberReducersString( String numberReducersString) {
      int retVal = 1; // reasonable default

      try { 
        retVal = Integer.parseInt(numberReducersString);
      } catch ( NumberFormatException nfe) {
        LOG.debug("nfe caught in parseNumberReducersString on string " + 
                  numberReducersString + ". Using 1 as a default" );
        retVal = 1;
      }
      return retVal;
    }

    /**
     * Parse a String indicating if a combiner should be used for this job
     * @param useCombinerString indicating whether a combiner should be used
     * @return whether a combiner should be used
     */
    public static boolean parseUseCombinerString( String useCombinerString) {
      if ( 0 == useCombinerString.compareToIgnoreCase("True" ) ) {
        return true;
      } else {
        return false;
      }
    }

    /**
     * Parse a String indicating if the No Scan functionality 
     * should be used for this job
     * @param noScanString indicating whether No Scan should be used
     * @return whether No Scan functionality should be used
     */
    public static boolean parseNoScanString( String noScanString) {
      if ( 0 == noScanString.compareToIgnoreCase("True" ) ) {
        return true;
      } else {
        return false;
      }
    }

    /**
     * Parse a String indicating if Query Dependant partitioning 
     * should be used for this job
     * @param queryDependantString indicating whether Query Dependant partitioning
     * should be used
     * @return whether Query Depenedent partitioning should be used
     */
    public static boolean parseQueryDependantString( String queryDependantString) {
      if ( 0 == queryDependantString.compareToIgnoreCase("True" ) ) {
          return true;
        } else {
            return false;
        }
    }

    /**
     * Parse a String indicating if the current program is applying a holistic
     * function
     * @param holisticString indicating whether the current job is applying
     * a holistic function
     * @return whether the function being applied is holistic
     */
    public static boolean parseHolisticString( String holisticString) {
      if ( 0 == holisticString.compareToIgnoreCase("True" ) ) {
          return true;
        } else {
            return false;
        }
    }

    /**
     * Parse a String indicating how to process multiple file inputs
     * @param multiFileModeString indicates how to process input sets of 
     * multiple files
     * @return which MultiFileMode to use for this job
     */
    public static MultiFileMode parseMultiFileMode( String multiFileModeString ) {
        MultiFileMode multiFileMode = MultiFileMode.combine;

      if ( 0 == multiFileModeString.compareToIgnoreCase("Combine" ) ) {
          multiFileMode = MultiFileMode.combine;
      } else if ( 0 == multiFileModeString.compareToIgnoreCase("Concat")) {
          multiFileMode = MultiFileMode.concat;
      }

        return multiFileMode;
    }
    
    /**
     * Parse a String indicating how to process multiple file inputs
     * @param multiFileModeString indicates how to process input sets of 
     * multiple files
     * @return which MultiFileMode to use for this job
     */
    public static PlacementMode parsePlacementMode( String placementModeString ) {
        PlacementMode placementMode = PlacementMode.roundrobin;

      if ( 0 == placementModeString.compareToIgnoreCase("RoundRobin" ) ) {
          placementMode = PlacementMode.roundrobin;
      } else if ( 0 == placementModeString.compareToIgnoreCase("Sampling")) {
          placementMode = PlacementMode.sampling;
      } else if ( 0 == placementModeString.compareToIgnoreCase("Implicit")) {
          placementMode = PlacementMode.implicit;
      }

        return placementMode;
    }
   
    /**
     * Parse a String indicating which partitioning mode to use
     * @param partModeString indicates the partitioning mode to use for
     * this job
     * @return a PartMode object indicating which partitioning mode to use
     */ 
    public static PartMode parsePartMode( String partModeString ) {
        PartMode partMode = PartMode.proportional;

      if ( 0 == partModeString.compareToIgnoreCase("Proportional" ) ) {
          partMode = PartMode.proportional;
      } else if ( 0 == partModeString.compareToIgnoreCase("Record")) {
          partMode = PartMode.record;
      } else if ( 0 == partModeString.compareToIgnoreCase("Calculated")) {
          partMode = PartMode.calculated;
      } else {
          LOG.warn("Specified partition mode is not understood: " + partModeString + "\n" +
                   "Please specify one of the following: proportional, record, calculated" );

        }

        return partMode;
    }
    
    public static PartitionerClass parsePartitionerClass( String partitionerClassString) {
        PartitionerClass partitionerClass = PartitionerClass.arrayspec;

      if ( 0 == partitionerClassString.compareToIgnoreCase("hash" ) ) {
          partitionerClass = PartitionerClass.hash;
      } else if ( 0 == partitionerClassString.compareToIgnoreCase("arrayspec")) {
          partitionerClass = PartitionerClass.arrayspec;
      } else {
          LOG.warn("Specified Partitioner Class is not understood: " + 
                    partitionerClassString + "\n" +
                   "Please specify one of the following: arrayspec, hash" );
      }

        return partitionerClass;
    }
    
    public static FSType parseFSType( String fsTypeString ) {
        FSType fsType = FSType.unknown;

      if ( 0 == fsTypeString.compareToIgnoreCase("hdfs" ) ) {
          fsType = FSType.hdfs;
      } else if ( 0 == fsTypeString.compareToIgnoreCase("ceph")) {
          fsType = FSType.ceph;
      } else if ( 0 == fsTypeString.compareToIgnoreCase("unknown")) {
          fsType = FSType.unknown;
      } else {
          LOG.warn("Specified FileSystem is not understood: " + fsTypeString + "\n" +
                   "Please specify one of the following: hdfs, ceph" );
          fsType = FSType.unknown;
      }

        return fsType;
    }
    
    /**
     * Parse a String indicating which Operator this job is using
     * @param operatorString indicates which Operator this job is applying
     * @return an Operator object indicating which function this job is applying 
     */ 
    public static Operator parseOperator( String operatorString ) {
      Operator op;

      if ( 0 == operatorString.compareToIgnoreCase("Max")) {
          op = Operator.max;
      } else if ( 0 == operatorString.compareToIgnoreCase("SimpleMax")) {
          op = Operator.simpleMax;
      } else if ( 0 == operatorString.compareToIgnoreCase("Median")) {
          op = Operator.median;
      } else if ( 0 == operatorString.compareToIgnoreCase("SimpleMedian")) {
          op = Operator.simpleMedian;
      } else if ( 0 == operatorString.compareToIgnoreCase("NullTest")) {
          op = Operator.nulltest;
      } else if ( 0 == operatorString.compareToIgnoreCase("Average")) {
          op = Operator.average;
      } else {
          // redundant, given initialization of retVal to optUnknown but shooting for clarity
          op = Operator.optUnknown;
      }

        return op;
    }

    /**
     * Retrieve the partitioning mode for this job
     * @param conf the Configuration object for the current job
     * @return a PartMode object indicating which partitioning mode 
     * to use for this job
     */ 
    public static PartMode getPartMode( Configuration conf) {
      String partModeString = getPartModeString(conf);
      partMode = parsePartMode( partModeString );
      return partMode;
    }

    public static PartitionerClass getPartitionerClass( Configuration conf) {
      String partitionerClassString = getPartitionerClassString(conf);
      partitionerClass = parsePartitionerClass( partitionerClassString);
      return partitionerClass;
    }

    public static FSType getFSType( Configuration conf) {
      String fsTypeString = getFSTypeString(conf);
      fsType = parseFSType( fsTypeString );
      return fsType;
    }

    /**
     * Retrieve the placement mode for this job
     * @param conf the Configuration object for the current job
     * @return a PlacmenetMode object indicating which placement mode 
     * to use for this job
     */ 
    public static PlacementMode getPlacementMode( Configuration conf) {
      String placementModeString = getPlacementModeString(conf);
      placementMode = parsePlacementMode( placementModeString );
      return placementMode;
    }

    /**
     * Retrieve the placement mode for this job
     * @param conf the Configuration object for the current job
     * @return a PlacmenetMode object indicating which placement mode 
     * to use for this job
     */ 
    public static boolean useCombiner(Configuration conf) {
      String useCombinerString = getUseCombinerString(conf);
      useCombiner = parseUseCombinerString(useCombinerString);

      return useCombiner;  // default to false (no scan not enabled)
    }

    /**
     * Retrieve the number of reducers to use for this job
     * @param conf the Configuration object for the current job
     * @return the number of reducers to use for this job
     */ 
    public static int getNumberReducers(Configuration conf) {
      String numberReducersString = getNumberReducersString(conf);
      numberReducers = parseNumberReducersString(numberReducersString);
      return numberReducers;  // default to false (no scan not enabled)
    }

    public static void setNumberReducers(Configuration conf, int numReducers) {
        conf.set(HadoopUtils.NUMBER_REDUCERS, Integer.toString(numReducers)); 
        // do this to force the next call to getNumberReducers:w
        //numberReducersSet = false;
    }

    /**
     * Determine whether the No Scan functionality should be used
     * @param conf the Configuration object for the current job
     * @return whether to enable No Scan functionality for this job
     */ 
    public static boolean noScanEnabled(Configuration conf) {
      String noScanString = getNoScanString(conf);
      noScanEnabled = parseNoScanString(noScanString);
      return noScanEnabled;  // default to false (no scan not enabled)
    }

    /**
     * Determine whether the Query Dependant functionality should be used
     * @param conf the Configuration object for the current job
     * @return whether to enable Query Dependnet functionality for this job
     */ 
    public static boolean queryDependantEnabled(Configuration conf) {
      String queryDependantString = getQueryDependantString(conf);
      queryDependantEnabled = parseQueryDependantString(queryDependantString);
      return queryDependantEnabled;  // default to false (no scan not enabled)
    }

    /**
     * Determine whether the current function is holistic
     * @param conf the Configuration object for the current job
     * @return whether the current query is holistic
     */ 
    public static boolean holisticEnabled(Configuration conf) {
      String holisticString = getHolisticString(conf);
      holisticEnabled = parseHolisticString(holisticString);
      return holisticEnabled;  // default to false (no scan not enabled)
    }


    /**
     * Retrieve the current Operator
     * @param conf the Configuration object for the current job
     * @return an Operator object indicating which function is being applied
     * by the current program
     */ 
    public static Operator getOperator( Configuration conf) {
      String operatorString = getOperatorString(conf);
      operator = parseOperator( operatorString);
      return operator;
    }

    /**
     * Retrieve the extraction shape for the current job
     * @param conf the Configuration object for the current job
     * @return the extraction shape for this job as an n-dimensional array 
     */ 
    public static int[] getExtractionShape(Configuration conf, int size) {
      String extractionString = conf.get(EXTRACTION_SHAPE, "");

      if ( extractionString == "" ) {
        extractionShape = new int[size];

        for( int i=0; i<size; i++) {
          extractionShape[i] = 1;
        }
      } else {
        String[] extractionDims = extractionString.split(",");
        extractionShape = new int[extractionDims.length];

        for( int i=0; i < extractionShape.length; i++) {
          extractionShape[i] = Integer.parseInt(extractionDims[i]);
        }
      }

      return extractionShape;
    }

    /**
     * Determine whether a given cell is valid
     * @param globalCoord an n-dimensional array containing the coordinate 
     * to validate 
     * @param conf the Configuration object for the current job
     * @return whether the indicated cell is valid
     */ 
    public static boolean isValid(int[]globalCoord, Configuration conf){
      getValidLow(conf); // this will set high and low as a byproduct of requesting the value

      if(globalCoord[0] >= validLow && globalCoord[0] < validHigh ){
        return true;
      }else{
        return false;
      }
    }

    /**
     * Determine if a set of cordinates are at, or beyond, the last valid 
     * data coordinate
     * @param varShape the shape of the variable being processed
     * @param current the n-dimensional coordinate being validated
     * @return whether the array being passed in is at or past the 
     * end of the variable
     */
    public static boolean endOfVariable(int[] varShape, int[] current) {
      boolean retVal = true;

      if ( current[0] >= varShape[0] ) { 
        return retVal;
      } 

      for ( int i=0; i<current.length; i++) {
        if( current[i] < varShape[i] -1 ) { 
          retVal = false;
          return retVal;
        } 
      }

      return retVal;
    }

    /**
     * Determine if the current coordinate is a "full" record
     * @param corner the coordinate to check
     * @return whether the coordinate passed in is the start of a 
     * new record 
     */
    public static boolean atFullRecord(int[] corner ) {
      boolean retVal = true;

      for ( int i = 1; i < corner.length; i++ ) {
        if ( corner[i] != 0 ) { 
          retVal = false; 
        }   
      }   

      return retVal;
    } 

    /**
     * Increment an array in the context of an n-dimensional variable
     * @param varShape the variable being processed by the current program
     * @param current the coordinate to increment
     * @return the incremented n-dimensional array
     */
    public static int[] incrementArray( int[] varShape, int[] current ) {
      int curDim = current.length - 1;
      current[curDim]++;

      while ( current[curDim] >= varShape[curDim] && curDim > 0 ) {
        current[curDim] = 0;
        current[curDim - 1]++;
        curDim--;
      }
      return current;
    }
        

    /**
     * Caculates the total number of cells present in an n-dimensional array
     * NOTE: this will translate any zero values to 1 so that it doesn't 
     * return 0 as the total 
     * @param array the n-dimensional array for which to calculate the total size
     * @return the count of cells present in the array
     */
    public static int calcTotalSize( int[] array ) {
    int retVal = 1;

      for( int i=0; i<array.length; i++) {
        retVal *= (array[i] != 0 ? array[i] : 1);
      }
      return retVal;
    }

    public static long calcTotalSize( long[] array ) {
      long retVal = 1;

      for( int i=0; i<array.length; i++) {
        retVal *= array[i];
      }
      return retVal;
    }

    /**
     * Caculates the size of an n-dimensional array in bytes
     * @param array the n-dimensional array for which to calculate the total size
     * @param dataTypeSize the size of the data type, in bytes, 
     * stored in the array
     * @return the size of the n-dimensional array, in bytes
     */
    public static long calcArrayTotalSize( long[] array, int dataTypeSize ) { 
      long retVal = 1;

      for( int i=0; i<array.length; i++) {
        retVal *= array[i];
      }
      retVal *= dataTypeSize;
      return retVal;
    }
  
    /**
     * Compute the number of cells needed to increment each dimension
     * of an n-dimensional shape 
     * @param shape the shape of the variable being processed
     * @return an array of longs, indicating the number of cells needed, on 
     * each dimension, to increment a coordinate on that dimension
     */
    public static long[] computeStrides(long[] shape) throws IOException {
      long[] stride = new long[shape.length];
      long product = 1;
      for (int i = shape.length - 1; i >= 0; i--) {
        long dim = shape[i];
        if (dim < 0)
          throw new IOException("Negative array size");
        stride[i] = product;
        product *= dim;
      }
      return stride;
    }

    public static long[] computeStrides(int[] shape) throws IOException {
      long[] stride = new long[shape.length];
      long product = 1;
      for (int i = shape.length - 1; i >= 0; i--) {
        long dim = shape[i];
        if (dim < 0)
          throw new IOException("Negative array size");
        stride[i] = product;
        product *= dim;
      }
      return stride;
    }

    /**
     *  Expand a flattened n-dimensional array, using the variable it references
     * to calculate the said array
     * @param variableShape the shape of the variable the flattened coordinate 
     * corresponds to
     * @param element the flattened coordinate to expand
     * @return an n-dimensional coordinate
     */
    public static long[] inflate( long[] variableShape, long element ) 
      throws IOException {
      long[] retArray = new long[variableShape.length];
      long[] strides = computeStrides( variableShape );

      for ( int i = 0; i < variableShape.length; i++) {
        retArray[i] = (int)(element / strides[i]);
        element = element - (retArray[i] * strides[i] );    
      }
      return retArray;
    } 

    public static int[] inflate( int[] variableShape, long element ) 
      throws IOException {
      int[] retArray = new int[variableShape.length];
      LOG.debug("inflage: variableShape: " + Arrays.toString(variableShape) + " element: " + element);

      long[] strides = computeStrides( variableShape );
      LOG.debug("inflate: strides: " + Arrays.toString(strides));

      for ( int i = 0; i < variableShape.length; i++) {
        retArray[i] = (int)(element / strides[i]);
        element = element - (retArray[i] * strides[i]);    
      }
      LOG.debug("inflate returning: " + Arrays.toString(retArray));
      return retArray;
    } 

    /**
     * Flattens an n-dimensional coordinate into a single long value
     * @param variableShape the shape of the variable that the coordinate
     * references
     * @param currentElement the coordinate to flatten
     * @return a long value that is the equivalent of the currentElement arguement
     */
    public static long flatten( int[] variableShape, int[] currentElement ) 
      throws IOException {
      return calcLinearElementNumber( variableShape, currentElement );
    }

    public static long flatten( long[] variableShape, long[] currentElement ) 
                                throws IOException {
      return calcLinearElementNumber( variableShape, currentElement );
    }

    /**
     * Flattens an n-dimensional coordinate into a single long value
     * @param variableShape the shape of the variable that the coordinate
     * references
     * @param currentElement the coordinate to flatten
     * @return a long value that is the equivalent of the currentElement arguement
     */
    public static long calcLinearElementNumber( int[] variableShape, 
                                                int[] currentElement) 
                                                throws IOException {
        
      if( null == variableShape) {
        LOG.debug("calcLinearElementNumber, variableShape is null");
      } else if ( null == currentElement) {
        LOG.debug("calcLinearElementNumber, currentElement is null");
      }
      
	    long[] strides = computeStrides( variableShape);
	    long retVal = 0;
	
	    if( Arrays.equals(variableShape, currentElement)) { 
	    	retVal = strides[0] * variableShape[0];
	    } else { 
		    for( int i=0; i<currentElement.length; i++ ) {
		        retVal += ( strides[i] * currentElement[i] );
		    }
	    }
      return retVal;
    }

    public static long calcLinearElementNumber(long[] variableShape, 
                                               long[] currentElement)
                                                throws IOException {
        
      long[] strides = computeStrides( variableShape);
      long retVal = 0;

      for( long i=0; i<currentElement.length; i++ ) {
        retVal += ( strides[(int)i] * currentElement[(int)i] );
      }

      return retVal;
    }

    /**
     * Determines whether an array is sorted. Probably superfluous
     * @param array the array to determine whether it's sorted
     * @return whether the array is sorted
     */
    public static boolean isSorted( int[] array ){
      if ( array.length <= 1) { 
        return true;
      }

      for ( int i=0; i<array.length - 1; i++) {
        if ( array[i] > array[ i+1]) {
          return false;
        }
      }

      return true;
    }

    /**
     * Determines whether an array is sorted. Probably superfluous
     * @param array the array to determine whether it's sorted
     * @return whether the array is sorted
     */
    public static boolean isSorted( long[] array ){
      if ( array.length <= 1) { 
        return true;
      }

      for ( int i=0; i<array.length - 1; i++) {
        if ( array[i] > array[ i+1]) {
          return false;
        }
      }

      return true;
    }

    /**
     * This method maps an offset to a BlockLocation
     * @param blocks A lits of BlockLocations that represent the file 
     * containing the variable in question
     * @param offset An offset in the byte-stream
     * @return the BlockLocation which contains the offset passed into 
     * this method
     */
    public static BlockLocation offsetToBlock(BlockLocation[] blocks, long offset) {
      long start;
      long end;

      for (BlockLocation block : blocks) {
        start = block.getOffset();
        end = start + block.getLength();
        if (start <= offset && offset < end)
            return block;
        }
        return null;
    }

    /**
     * Maps a local coordinate into the global space
     * @param currentCounter the current ID to map into the global space
     * @param corner the anchoring corner of the current data set
     * @param globalCoordinate return value for this function
     */
    public static int[] mapToGlobal( int[] currentCounter, int[] corner,
                               int[] globalCoordinate) {
      for ( int i=0; i < currentCounter.length; i++) {
        globalCoordinate[i] = currentCounter[i] + corner[i];
      }
      return globalCoordinate;
    }

    public static void adjustGIDForLogicalOffset( HadoopGroupID gid, 
                                                  int[] logicalStartOffset, 
                                                  int[] extractionShape ) { 
      int[] groupID = gid.getHadoopGroupID();
      for( int i=0; i<logicalStartOffset.length; i++) { 
        groupID[i] += (logicalStartOffset[i] / extractionShape[i]);
      }
      gid.setHadoopGroupID(groupID);
    }

    /**
     * Map a global coordinate to a local coordinate by using the 
     * variable shape and extraction shape.
     * @param globalCoord the global coordinate to map into the local
     * space
     * @param groupIDArray memory that is allocated to hold the temporary
     * result
     * @param outHadoopGroupID the HadoopGroupID object that will be returned with
     * the local coordinate result
     * @param extractionShape extraction shape used to map from global
     * to local
     * @return a HadoopGroupID object containing the local coordinate result
     */
    public static HadoopGroupID mapToLocal( int[] globalCoord, int[] groupIDArray,
                                      HadoopGroupID outHadoopGroupID,
                                      int[] extractionShape ) {
      //short circuit out in case extraction shape is not set
      if ( extractionShape.length == 0 ) {
        outHadoopGroupID.setHadoopGroupID(groupIDArray);
        return outHadoopGroupID;
      }

      for ( int i=0; i < groupIDArray.length; i++ ) {
        groupIDArray[i] = globalCoord[i] / extractionShape[i];
      }

      outHadoopGroupID.setHadoopGroupID(groupIDArray);
      return outHadoopGroupID;
    }

    /**
     * Helper function used for debugging when needed
     * @param corner a corner coordiate
     * @param currentCounter another n-dimensional coordinate
     * @param globalCoordinate the globalcoordinate for the current coordinate
     * @param myHadoopGroupID the group ID for this coordinate
     * @param val1 value to print out
     * @param val2 value to print out
     * @return a String-ified version of all the arguements passed in 
     */
	  public static String giantFormattedPrint(int[] corner, int[] currentCounter,
	                                     int[] globalCoordinate, int[] myHadoopGroupID, 
	                                     int val1, int val2, String retString) {
	    // formatted string from hell
	    retString = String.format(
	            "gl co:%03l,%03l,%03l,%03l " +
	            "co: %03l,%03l,%03l,%03l " +
	            "ctr: %03l,%03l,%03l,%03l " +
	            "grp id: %03l,%03l,%03l,%03l " +
	            "v1: %010d " +
	            "v2: %010d" + 
	            "\n",
	            globalCoordinate[0], globalCoordinate[1], 
	            globalCoordinate[2], globalCoordinate[3],
	            corner[0], corner[1], corner[2], corner[3],
	            currentCounter[0], currentCounter[1], 
	            currentCounter[2], currentCounter[3],
	            myHadoopGroupID[0], myHadoopGroupID[1], myHadoopGroupID[2], myHadoopGroupID[3],
	            val1,
	            val2);
	
	    return retString;
    }

  public static String getCephDefaultURI(Configuration conf) { 
    String retString = conf.get(CEPH_DEFAULT_URI, "ceph://null");
    return retString;
  }

  public static int[] getTotalOutputSpace(Configuration conf) { 
    int[] variableShape = getVariableShape(conf);
    // sanity checking
    if (variableShape == null) { 
      return null;
    }
    int[] extractionShape = getExtractionShape(conf, variableShape.length);
    int[] outputSpace = new int[variableShape.length];

    for( int i=0; i<outputSpace.length; i++) { 
      outputSpace[i] = (int)((double)variableShape[i] / extractionShape[i]); 
    }

    return outputSpace;
  }

  public static int stepSizeForShape( int recordDim,
                                      int numSplits,
                                      int[] shape,
                                      long maxReducerKeyCount) 
  {
    int totalSteps= 1;
    for( int i=0; i<=recordDim; i++) { 
      totalSteps*= shape[i];
    } 
    
    long cellsPerStep = 1;
    for( int i=shape.length-1; i>recordDim; i--) {
      cellsPerStep *= shape[i];
    }
    
    int retVal = (int)Math.ceil( totalSteps / (float)numSplits);

    // correct for reducerMaxKeySize
    if( maxReducerKeyCount > 0) { 
      while( (retVal * cellsPerStep) > maxReducerKeyCount) { 
        retVal--;
      }
    }
    

    LOG.debug("in stepSizeForShape: total steps in output data: " + totalSteps + 
             " cells per step: " + cellsPerStep + 
             " numSplits: " + numSplits + 
             " retval: " + retVal);
    
    return retVal;
  }

  public static int determineNumberOfReducers(Configuration conf) {  

    int[] totalOutputSpace = getTotalOutputSpace(conf);
    long maxReducerKeyCount = getReducerKeyLimit(conf);

    // total size in terms of keys
    long totalSize = calcTotalSize(totalOutputSpace);
    LOG.debug("totalOS: " + Arrays.toString(totalOutputSpace) + 
                       " maxRedKeys: " + maxReducerKeyCount);
    int numReducers = (int)(Math.ceil((double)totalSize / maxReducerKeyCount));

    return numReducers;
  }

  public static int determineRecordDimension(Configuration conf) { 
    int[] outputSpace = getTotalOutputSpace(conf);
    int numReducers = getNumberReducers(conf);
    long maxKeysPerReducer = getReducerKeyLimit(conf);
    float weight = getReducerShapeWeight(conf); 

    return determineRecordDimensionWeighted(outputSpace, numReducers, 
                                            weight, maxKeysPerReducer);
  }

  public static int determineRecordDimensionWeighted(int[] totalOutputShape, 
                                    int numReducers, double weight,
                                    long maxReducerKeyCount) { 
    // reasonable default value
    int recordDimension = totalOutputShape.length - 1;
    long sizeSoFar = 1;
    long totalSize = HadoopUtils.calcTotalSize(totalOutputShape);

    int[] stepSize = new int[totalOutputShape.length];

    for( int i=0; i<stepSize.length; i++) { 
      stepSize[i] = totalOutputShape[i];

    }

    LOG.debug("total shape: " + Arrays.toString(totalOutputShape) + " total size: " + 
             totalSize);    

    // dimension lenth 1 means no length at all, unless it's the 
    // highest dimension, then it's a single cell's length of data
    for( int i=0; i<totalOutputShape.length; i++) { 
      stepSize[i] = 1;
      sizeSoFar = HadoopUtils.calcTotalSize(stepSize);

      // make sure that the last step won't vary too much from all early steps

      double tempVal = (double)sizeSoFar / totalSize;
      //LOG.debug("i: " + i + " sizeSoFar " + sizeSoFar + " total: " + totalSize + 
      //                   " weight: " + weight + " tempVal: " + tempVal);
      if( (tempVal < weight) & 
        ((maxReducerKeyCount < 0) || (sizeSoFar <= maxReducerKeyCount)) 
      // &  (sizeSoFar > numReducers)
      ){ 
        recordDimension = i;
        break;
      }
    }

    return recordDimension;
  }

  // this calculates the write shape per reducer
  public static int[] getReducerWriteCorner( int reducerNum,
                                            Configuration conf ) { 

    int[] totalOutputShape = getTotalOutputSpace(conf);
    if( null == totalOutputShape) { 
    	LOG.warn("getReducerWriteCorner: totalOutputShape is NULL");
      return null;
    }

    int[] returnCorner = new int[totalOutputShape.length];

    int numReducers = getNumberReducers(conf);
    long maxReducerKeyCount = getReducerKeyLimit(conf);

    int recordDim = HadoopUtils.determineRecordDimension(conf);

    LOG.debug("In getReducerWriteCorner, recordDim: " + recordDim);

    int recordStepSize = stepSizeForShape( recordDim, numReducers, 
                                           totalOutputShape, maxReducerKeyCount);

    LOG.debug("In getReducerWriteCorner, recordStepSize: " + recordStepSize);

    for( int i=0; i<recordDim; i++) { 
      returnCorner[i] = 0;
    }
    returnCorner[recordDim] = recordStepSize * reducerNum;
    for( int i=recordDim+1; i<returnCorner.length; i++) { 
      returnCorner[i] = 0;
    }

    returnCorner = roundArrayShape(returnCorner, totalOutputShape);
    LOG.debug("Rounded corner: " + Arrays.toString(returnCorner));

    return returnCorner;
  }

  // this calculates the write shape per reducer
  public static int[] getReducerWriteShape( int reducerNum,
                                            Configuration conf ) throws IOException { 

    int[] totalOutputSpace = getTotalOutputSpace(conf);
    // sanity check
    if (totalOutputSpace == null) { 
      return null;
    }
    int[] returnShape = Arrays.copyOf(totalOutputSpace , totalOutputSpace.length);

    int numReducers = getNumberReducers(conf);
    long maxReducerKeyCount = getReducerKeyLimit(conf);

    int recordDim = HadoopUtils.determineRecordDimension(conf);


    LOG.debug("In getReducerWriteShape, recordDim: " + recordDim);

    int recordStepSize = stepSizeForShape( recordDim, numReducers, 
                                           totalOutputSpace, maxReducerKeyCount);

    LOG.debug("In getReducerWriteShape, recordStepSize: " + recordStepSize);

    for( int i=0; i<recordDim; i++) { 
      returnShape[i] = 0;
    }

    long[] strides = computeStrides(totalOutputSpace);
    returnShape[recordDim] = recordStepSize;

    // we may need to do some adjustment for the last reducer, since it may not
    // write as much as previous reducers
    
    if( (numReducers-1) == reducerNum) { 
      int[] returnCorner = getReducerWriteCorner(reducerNum, conf);
      returnShape[recordDim] = totalOutputSpace[recordDim] - returnCorner[recordDim];
    }

    LOG.info("JB, debug print3: " + Arrays.toString(returnShape));
    // round the shape
    returnShape = roundArrayShape(returnShape, totalOutputSpace);

    return returnShape;
  }

  /*
   * This will take a shape with potentially an over-full dimension and round said dimension up accordingly
  */
  public static int[] roundArrayShape( int[] arrayShape, 
                                       int[] totalOutputShape) { 

    LOG.debug("top of roundArrayShape, arrayShape: " + Arrays.toString(arrayShape) + 
           " totalOutputShape: " + Arrays.toString(totalOutputShape));
  	if( null == arrayShape ){
  		LOG.warn("roundArrayShape: arrayShape is NULL");
  	}
  	
  	if( null == totalOutputShape ) { 
  		LOG.warn("roundArrayShape: totalOutputShape is NULL");
  	}

    for( int i=totalOutputShape.length-1; i > 0; i--) { 
      if( arrayShape[i] > totalOutputShape[i] ) { 
        arrayShape[i-1] += arrayShape[i] / totalOutputShape[i];
        arrayShape[i] = arrayShape[i] % totalOutputShape[i];
      }
    }

    if (arrayShape[0] > totalOutputShape[0]) { 
      arrayShape[0] = totalOutputShape[0];
    }
    
    // if this is true, then we have issues. return array of -1 to indicate this
    if( arrayShape[0] > totalOutputShape[0] )  { 
      LOG.debug("in roundArrayShape, arrayShape[0] > totalOutputShape[0]. This is bad");
      LOG.debug("ArrayShape: " + Arrays.toString(arrayShape) +
                         " tos: " + Arrays.toString(totalOutputShape));
      arrayShape = null;
    }

    LOG.debug("bottom of roundArrayShape, arrayShape: " + Arrays.toString(arrayShape) + 
            " totalOutputShape: " + Arrays.toString(totalOutputShape));
    return arrayShape;

  }
  

  public static int[] correctArray( int[] corner,
                                    int[] shape,
                                    Configuration conf) 
  {
    int[] totalOutputShape = getTotalOutputSpace(conf);
    if( null == totalOutputShape) { 
      LOG.debug("totalOutputShape is null");
     }
    return correctArray(corner, shape, totalOutputShape);
  }


  public static int[] correctArray( int[] corner,
                                    int[] shape,
                                    int[] totalShape)
  {
    if( null == corner) { 
        LOG.debug("in correctArray, corner is NULL");
    } 
    if( null == shape) { 
        LOG.debug("in correctArray, shape is NULL");
    } 
    if( null == totalShape) { 
        LOG.debug("in correctArray, totalShape is NULL");
    }

    int[] fakeArray = Arrays.copyOf(shape, shape.length);
    fakeArray = HadoopUtils.roundArrayShape(fakeArray, totalShape);
    LOG.info("fakeArray: " + Arrays.toString(fakeArray) + " total shape: " + Arrays.toString(totalShape));
    LOG.info("pre-correct: " + 
      "\tcorner: " + Arrays.toString(corner) + 
      "\tshape: " + Arrays.toString(shape)
    );
    // convert everything to flattened values, subtract, and then assigned
    // the difference to the shape
    long shapeL = 0;
    try { 
      long cornerL = HadoopUtils.flatten(totalShape, corner);
      shapeL = HadoopUtils.flatten(totalShape, shape);
      long totalShapeL = HadoopUtils.flatten(totalShape, totalShape);
      long[] strides = computeStrides(totalShape);

      LOG.info("shapeL: " + shapeL + " totalShapeL: " + totalShapeL + " cornerL: " + cornerL);
      LOG.info("strides: " + Arrays.toString(strides));
      // test
      shapeL = totalShapeL - cornerL - 1; // subtract 1 to get the actual last element, not the next one

      shape = HadoopUtils.inflate(totalShape, shapeL);
    } catch( IOException e ) { 
      e.printStackTrace();
    }

    LOG.debug("post-correct: " + 
      "\tcorner: " + Arrays.toString(corner) + 
      "\tshape: " + Arrays.toString(shape)
    );

    return shape;
  }

  public static int[] incrementArray( int[] varShape,
                                    int[] currentShape,
                                    int[] incrementShape )
  {
    // increment the current shape
    for( int i = 0; i<varShape.length; i++) {
      if( (i < (varShape.length - 1)) && (incrementShape[i] > 1))
        currentShape[i] += incrementShape[i];
    }

    // now correct for full dimensions
    for( int i=varShape.length-1; i >= 0; i-- ) {
      if( (currentShape[i] >= varShape[i]) & (i > 0) ) {
        currentShape[i-1] += (currentShape[i] / varShape[i]);
        currentShape[i] = currentShape[i] % varShape[i];
      }
    }

    return currentShape;
  }

  public static int[] getOutputCornerForReducerN(int taskID, 
                                                 Configuration conf) { 

    int[] totalOutputSpace = getTotalOutputSpace(conf);
    int numReducers = getNumberReducers(conf);
    LOG.debug("NumReducers: " + numReducers);
    int[] outputWriteShape;
    try {
	    outputWriteShape = getReducerWriteShape(numReducers, conf);
    } catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	    outputWriteShape = null;
    }

    if (outputWriteShape == null) { 
      return null;
    }

    long maxKeysPerReducer = getReducerKeyLimit(conf);
    int recordDim;

    float weight = getReducerShapeWeight(conf); 
    long maxReducerKeyCount = getReducerKeyLimit(conf);
    recordDim = determineRecordDimensionWeighted( outputWriteShape, numReducers,
                                                  weight, maxReducerKeyCount);

    LOG.debug("total output shape: " + Arrays.toString(totalOutputSpace) + 
                       " outputWriteShape: " + Arrays.toString(outputWriteShape) + 
                       " record dim: " + recordDim );

    int[] retArray = new int[outputWriteShape.length];
    int[] stepArray = new int[outputWriteShape.length];
    
    for( int i=0; i<retArray.length; i++) { 
      retArray[i] = 0;
      stepArray[i] = 0;
    }

    stepArray[recordDim] = outputWriteShape[recordDim];

    for( int i=0; i<taskID; i++) { 
      //retArray[recordDim] += outputWriteShape[recordDim];
      retArray = incrementArray(totalOutputSpace, retArray, stepArray);
    }
   
    return retArray;
  }

  public static String stripURIInfo(String fullPath, String myURI){
    if( fullPath.startsWith(myURI) ) {
      return fullPath.substring(myURI.length());
    } else {
      return new String("");
    }
  }

  public static long multiplyAndSumArrays( int[] array, long[] strides) {
    long result = 0;
    for( int i=0; i<array.length; i++) {
      result += (array[i] * strides[i]);
    }
    
    return result;
  }
}
