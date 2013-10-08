package edu.ucsc.srl.damasc.hadoop.io.input;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.net.URI;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import ncsa.hdf.hdf5lib.H5;
import ncsa.hdf.hdf5lib.HDF5Constants;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;
import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import edu.ucsc.srl.damasc.hadoop.io.ArraySpec;
import edu.ucsc.srl.damasc.hadoop.io.SHFileStatus;
//import edu.ucsc.srl.damasc.hadoop.HDF5Utils;
import edu.ucsc.srl.damasc.hadoop.Utils;
import edu.ucsc.srl.damasc.hadoop.Utils.PartMode;
import edu.ucsc.srl.damasc.hadoop.Utils.PlacementMode;
import edu.ucsc.srl.damasc.hadoop.Utils.MultiFileMode;


/**
 * FileInputFormat class that represents HDF5 files
 * Format specific code goes here
 */
public class HDF5FileInputFormat 
    extends ArrayBasedFileInputFormat {

  private static final Log LOG = 
      LogFactory.getLog(HDF5FileInputFormat.class);

  // Use the constructor to do the silly .so loading trick os that
  // the hdf5 library will work correctly later

  public HDF5FileInputFormat(){
    super();
    System.out.println("in the constructor");
  }

  /**
   * Use FileInputFormat for file globbing capabilities, and filter out any
   * files that cannot be opened by NetcdfFile.open().
   * @param jobC Context for the job being executed
   * @return a List<FileStatus> where each entry represents a valid file
   */
  @Override
  protected List<FileStatus> listStatus(JobContext jobC) throws IOException{

    List<FileStatus> files = super.listStatus(jobC);
    ArrayList<FileStatus> rfiles = new ArrayList<FileStatus>();
    Configuration conf = jobC.getConfiguration();

    int hdf5FileID = -1;
    int fapl = -1;

    /*
    String ldString = System.getenv("LD_LIBRARY_PATH");
    System.out.println("LD_Library path: " + ldString + "\n\n");
    String ldPreString = System.getenv("LD_PRELOAD");
    System.out.println("LD_PRELOAD path: " + ldPreString + "\n\n");
    String cpString = System.getenv("CLASSPATH");
    System.out.println("classpath: " + cpString + "\n\n");
    String userString = System.getProperty("user.name");
    System.out.println("username: " + userString);
    String javaLPString = System.getProperty("java.library.path");
    System.out.println("java.library.path: " + javaLPString + "\n\n");
    System.out.println("bitness of jvm is " + System.getProperty("sun.arch.data.model") );
    */

    try { 
      fapl = H5.H5Pcreate(HDF5Constants.H5P_FILE_ACCESS);
      //int error = H5.H5Pset_fapl_ceph(fapl, );
    } catch (HDF5LibraryException le) { 
      le.printStackTrace();
    }

    for (FileStatus file: files) {

      LOG.warn("file: " + file.getPath().toString());
      try {
        //String fileToOpen = Utils.stripURIInfo(file.getPath().toString(), 
       //                        jobC.getConfiguration().get("fs.default.name", ""));
        Path filePath = Utils.convertToMountPath(file.getPath(), conf); 

        System.out.println("trying to H5Fopen path:" + filePath.toString()); 

        hdf5FileID = H5.H5Fopen(filePath.toString(), HDF5Constants.H5F_ACC_RDONLY,
                             fapl);
      } catch (Exception e) {
        LOG.warn("Skipping input: " + file.getPath());
        rfiles.add(file);
      } finally {
        try { 
          H5.H5Fclose(hdf5FileID);
 
        } catch (HDF5LibraryException le) { 
          le.printStackTrace();
        }
      }

    }

    try { 
      H5.H5Pclose(fapl);
    } catch (Exception e) { 
      LOG.error("H5Pclose errored, returned " + e.toString());
    }
    files.removeAll(rfiles);
    return files;
  }

  /** 
   * This is somewhat of a utility function in that it 
   * figures out what is the current, highest numbered dimension
   * that is not "full". This function is exclusively used by 
   * the method nextCalculatedPartition().
   * @param cellsLeft how many cells that still need to be covered by the
   * shape being generated
   * @param strides How many cells it takes to iterate a given dimension,
   * The lengths correspond to the given dimension (strides[0] = number
   * of cells needed to iterate on dimension zero, etc.)
   * @param current the current position, in the logical space
   * @param varShape the shape of the variable being processed
   * @return the dimension that should next be iterated on when creating
   * partition shapes
   */
   /*
  public static int calcCurrentDim( long cellsLeft, long[] strides, 
                                    int[] current, int[] varShape ) {
    int retDim = -1;

    // first, see if we need to fill out any dimensions above zero
    for ( int i = current.length - 1; i > 0; i-- ) { 
      if (current[i] == 0)   
      {
        continue;   // no need to increment this level, it's full
      } else if ( cellsLeft > strides[i] ) { 
        // if this dim is non-zero, non-full and there are 
        // sufficient cells left, it's out winner
        retDim = i;
        return retDim;
      }
    }

    // if we're still in this fucntion, 
    //it's time to start filling in, from dim-0 down,
    // as free cells permit
    for( int i=0; i < current.length; i++ ) {
      if ( strides[i] <= cellsLeft ) { 
        retDim = i;
        return retDim;
      }
    }

    // if we get here, something went super wrong. Return -1 to indicate such
      retDim = -1;
      return retDim;
  }
  */

  /**
   * This method iteratively creates partition shapes. It relies heavily on the
   * function calcCurrentDim().
   * @param var Variable object for the current NetCDF variable that we're 
   * generating partitions for
   * @param varShape the shape of the variable, by dimension, that we're 
   * creating partitions for
   * @param currentStart where to start creating a partition from
   * @param blockSize the HDFS block size (in bytes)
   * @param dataTypeSize the size, in bytes, of the data type stored in
   * the variable that partitions are being generated for
   * @param allOnes a helper structure. It's simply an array that is the same 
   * length as varshape that contains all "1"s 
   * @param strides The number of cells require to iterate a given dimension
   * @param fileName The name of the file that contains the variable that 
   * partitions are being generated for
   * @param blockToAS a HashMap that stores the mapping of ArraySpecs to
   * file system blocks
   * @param startOffset deprecated
   * @param conf Configuration object for the currently executing MR job
   *
   */
  public void nextCalculatedPartition( 
                      int datasetID, int[] varShape, 
                      int[] currentStart, 
                      long blockSize, 
                      int dataTypeSize, 
                      int[] allOnes, int[] strides, 
                      String fileName,
                      HashMap<BlockLocation, ArrayList<ArraySpec>> blockToAS,
                      int[] startOffset,
                      Configuration conf ) 
                      throws IOException, Exception {
    

    if ( Utils.endOfVariable(varShape, currentStart) ) { 
      return;
    }

    // get the byte-offset of the current starting point
    //ArrayLong offsets = var.getLocalityInformation(
    //                    currentStart, 
    //                    allOnes);
    
    long[][] selectionArray = new long[1][varShape.length];
    long[] offsetArray = new long[1];
    long[] longStrides = new long[strides.length];
    for (int i=0; i<longStrides.length; i++) { 
      longStrides[i] = (long)strides[i];
    }

    int dataspace_id = -1;
    int datatype = -1;
    int error = -1;
    long curLocation = -1;
    try { 
      dataspace_id = H5.H5Dget_space(datasetID);
      if (dataspace_id < 0) { 
        LOG.error("H5Dget_space error, returned " + dataspace_id);
      }

      datatype = H5.H5Dget_type(datasetID);
      if (datatype < 0) { 
        LOG.error("H5Dget_type error, returned " + datatype);
      }

      curLocation = getSingleCellOffset(Utils.convertIntArrayToLongArray(currentStart), 
                                        dataspace_id, datasetID, 
                                        datatype, selectionArray, offsetArray);  
    } catch (Exception e) { 
      LOG.error("Exception caught: " + e.toString());
    } 

    long bytesLeft = blockSize - (curLocation % blockSize);
    long cellsLeft = bytesLeft / dataTypeSize;
    long currentBlock = curLocation / blockSize;


    // determine the highest, non-full block
    int curDim = calcCurrentDim( cellsLeft, longStrides, currentStart, varShape);

    if ( curDim < 0) {
      System.out.println("DANGER:\n" + 
      "\t curDim: " + curDim + 
      "\tcells left: " + cellsLeft + "\n" + 
      "\tstrides: " + Utils.arrayToString(strides) );
    }

    int steps = (int)Math.min( varShape[curDim] - currentStart[curDim], 
                               cellsLeft /strides[curDim] );

    int[] readShape = new int[currentStart.length];

    // readShape[curDim] will be updated but I wanted to init it properly
    for ( int i = 0; i < currentStart.length; i++) { 
      readShape[i] = 1;
    }

    // if we're stepping the record dim, we need to be 
    // careful due to interleaved variables
    if ( curDim == 0 ) {     
      long[] tempShape = new long[currentStart.length];
      for ( int j = 0; j < tempShape.length; j++) {
        if ( j > 0 ) {
          tempShape[j] = 1;
        } else {
          tempShape[j] = currentStart[j];
        }
      }
             
      // get the current block so we know if the next 
      // record is on a differnet block
      //offsets = var.getLocalityInformation(
      //                    currentStart, 
      //                    allOnes);
      //curLocation = offsets.getLong(0);
      curLocation = getSingleCellOffset(Utils.convertIntArrayToLongArray(currentStart), 
                                        dataspace_id, datasetID, 
                                        datatype, selectionArray, offsetArray);  
      currentBlock = curLocation / blockSize;


      // j starts at 1, since we know a read at 
      // corner: currentStart[0], 1 ... will be on the current block
      for ( int j = 1; j < steps; j++) {
        tempShape[curDim]++; //  look ahead a record
        //offsets = var.getLocalityInformation(
        //                  tempShape, 
        //                  allOnes);
        //curLocation = offsets.getLong(0);
        curLocation = getSingleCellOffset(tempShape, dataspace_id, datasetID, 
                                        datatype, selectionArray, offsetArray);  

        bytesLeft = blockSize - (curLocation % blockSize);
        cellsLeft = bytesLeft / dataTypeSize;

        long tempBlock = curLocation / blockSize;
        // make sure this record isn't on a different block
        // and that there are enough free cells to fill it
        if ( (tempBlock != currentBlock) || 
             (cellsLeft < strides[curDim]) ){
          break;
        }
        readShape[curDim]++;
      }
    } else { // if it's not dimension 0, life is a lot easier
      readShape[curDim] = steps;
    }

    // fill in the remaining dimensions of readShape, if needed
    for ( int i = curDim + 1; i < currentStart.length; i++) {
      readShape[i] = varShape[i];
    } 


    // if noscan is on, check if this ArraySpec belongs in the MapReduce
    if ( Utils.noScanEnabled(conf)) {
      if( Utils.isValid(currentStart, conf)) {  
        Utils.insertNewAs( blockToAS, currentBlock * blockSize, 
                     new ArraySpec(currentStart, readShape, 
                     Utils.getVariableName(conf), fileName, varShape) ); // here
      }
    } else {
      Utils.insertNewAs( blockToAS, currentBlock * blockSize, 
                   new ArraySpec(currentStart, readShape, 
                                 Utils.getVariableName(conf), fileName, varShape) );
    }

    // increment currentDim
    currentStart[curDim] += readShape[curDim];
    
    // zero out the higher dimensions
    for ( int i = curDim+ 1; i < currentStart.length; i++ ) { 
      currentStart[i] = 0;
    }

    for ( int i = currentStart.length - 1; i > 0; i-- ) {
      if( currentStart[i] == varShape[i] ) {
        currentStart[i - 1]++;
        currentStart[i] = 0;
      }
    }

    return;
  }

  private long getSingleCellOffset(long[] currentStart, int dataspace_id,
                                   int datasetID, int datatype, long[][]selectionArray, 
                                   long offsetArray[]) { 

      //selectionArray[0] = Utils.convertIntArrayToLongArray(currentStart);
      selectionArray[0] = currentStart;
      try { 
        int error = H5.H5Sselect_elements(dataspace_id, HDF5Constants.H5S_SELECT_SET,
                                    1, selectionArray);
        if (error < 0) { 
          LOG.error("H5Sselect_elements error, returned " + error);
        }
        error = H5.H5Dget_offsets(datasetID, datatype, dataspace_id, 
                        HDF5Constants.H5P_DEFAULT, offsetArray);
        if (error < 0) { 
          LOG.error("H5Dget_offsets error, returned " + error);
        }
      } catch (HDF5Exception e) { 
        LOG.error("HDF5 exception: " + e.getMessage());
      }

      return offsetArray[0];
  }

  /**
   * Map an ArraySpec to a file system block. This is file format 
   * specific because it uses the sampling facility exposed by the
   * file format library. Once the correct block is determined,
   * that BlockLocation is returned.
   * @param variable The variable that partitions are being generated for
   * @param array The ArraySpec that needs to be matched to a BlockLocation
   * @param blocks A list of BlockLocations that represent the current file
   * @param conf Configuration object for the currently running MapReduce
   * program
   * @return a BlockLocation object representing the block that this 
   * ArraySpec should be assigned to
   */
  private BlockLocation arrayToBlock(int datasetID, ArraySpec array,
                                     BlockLocation[] blocks, 
                                     Configuration conf)
                                     throws IOException, HDF5LibraryException, HDF5Exception {

    /*
     * For each block an integer is used to accumulate the number of samples in
     * the logical space that mapped to offsets in the block.
     */
    HashMap<BlockLocation, Integer> blockHits = 
      new HashMap<BlockLocation, Integer>();

    
    int dataspaceID = H5.H5Dget_space(datasetID);
    int numDims = H5.H5Sget_simple_extent_ndims(dataspaceID);
    int[] dims = new int[numDims];
    int[] maxDims = new int[numDims];

    //long arraySize = Utils.calcTotalSize(dims);
    long arraySize = array.getSize();

    int sampleSize = (int) (arraySize * Utils.getSampleRatio(conf));
    //System.out.println("\t\tAbout to take " + sampleSize + " samples");

    int[] shape = array.getShape();
    int rank = array.getRank();
    long[] stride = Utils.computeStrides(shape);

    /*
     * Holds the position we will query for byte offset
     */

     // this needs to be a long to appease HDF5
    long[] sampleIndex = new long[rank];
    sampleIndex[0] = array.getCorner()[0];

    /*
     * Helper array (sampleIndex = corner, ones = shape)
     */
    int[] ones = new int[rank];
    for (int i = 0; i < ones.length; i++)
        ones[i] = 1;

    /*
     * Generate sampleSize positions
     *
     * FIXME:
     *   - This should be uniform without replacement
     *   - This linearization should be moved into ArraySpec
     */
    Random rand = new Random();
    long[] tempResult = new long[1];
    long[] totalResults = new long[sampleSize];

    // this needs to be long[][], because that's what HDF5 requires
    long[][] tempArray = new long[1][rank];

    // create one massive selection
    for (int i = 0; i < sampleSize; i++) {
      //System.out.println("top of loop, interation " + i );
      int samplePos = rand.nextInt((int)arraySize);
      int holdSamplePos = samplePos;
      /* Convert from linearized space to coordinates */
      for (int j = 0; j < rank; j++) {
        if ( j == 0 ) {  
          sampleIndex[j] = array.getCorner()[j];
          sampleIndex[j] += samplePos / stride[j];
          samplePos -= ( sampleIndex[j] - array.getCorner()[j]) * stride[j];
        } else  { 
          sampleIndex[j] = (int)(samplePos / stride[j]);
          samplePos -= sampleIndex[j] * stride[j];
        }
      }

      tempArray[0] = sampleIndex;

      //System.out.println("add " + Utils.arrayToString(tempArray[0]) + 
      //                   " to the selection via SELECT");
      totalResults[i] = getSingleCellOffset(sampleIndex, dataspaceID, datasetID, 
                                       H5.H5Dget_type(datasetID), tempArray, tempResult);  

      //H5.H5Sselect_elements(dataspaceID, HDF5Constants.H5S_SELECT_SET,
      //                      1, tempArray);
      // get the actual offsets

      //H5.H5Dget_offsets(datasetID, H5.H5Dget_type(datasetID), 
      //                  dataspaceID,
      //                  HDF5Constants.H5P_DEFAULT, tempResult);
      //totalResults[i] = tempResult[0];

      /* 
       * Use Joe's netCDF extension to lookup the offset for this sample
       * coordinate.
       */
      /*
      Object[] offsetArray;

      try {

        offsetArray = variable.getLocalityInformation(sampleIndex, ones);
      } catch (Exception e) {
        throw new IOException(e.toString() + "\n corner: " + 
                              Utils.arrayToString(array.getCorner()) + 
                              " shape: " + 
                              Utils.arrayToString(array.getShape()) + 
                              " sample coord: " + 
                              Utils.arrayToString(sampleIndex));
      }

      long offset = offsetArray.getLong(0);
      */

    }


    for (int i = 0; i < sampleSize; i++) {
      /* Find block that contains this offset */
      BlockLocation block = Utils.offsetToBlock(blocks, totalResults[i]);

      /* Update block-to-hits mapping */
      if (!blockHits.containsKey(block))
        blockHits.put(block, new Integer(0));

      Integer curHits = blockHits.get(block);
      blockHits.put(block, new Integer(curHits.intValue() + 1));
    }

    /* Iterator over each block-to-hits mapping entry */
    Iterator<Map.Entry<BlockLocation, Integer>> blockHitsIter;
    blockHitsIter = blockHits.entrySet().iterator();

    /* Holders for final results */
    Integer maxHits = null;
    BlockLocation maxBlock = null;

    /* Find the block with the most hits */
    while (blockHitsIter.hasNext()) {
      Map.Entry<BlockLocation, Integer> entry = blockHitsIter.next();
      BlockLocation block = entry.getKey();
      Integer count = entry.getValue();

      if (maxHits == null || count > maxHits) {
        maxHits = count;
        maxBlock = block;
      }
    }

    /* The winner */
    //System.out.println("Winning block is " + maxBlock.toString() );
    return maxBlock;
  }

  /**
   * Map partitions to inputSplits via sampling. This is file format specifc
   * as it uses the specific library to map a logical coordinate to an offset
   * in the byte-stream as part of the sampling function.
   * @param var Variable object that partitions are being created for
   * @param records Array of records to be mapped to blocks
   * @param blocks A list of BlockLocations that represents that file
   * that contains the variable that partitions are being generated for
   * @param blockToArrays A hash map that maps ArraySpecs to Blocks
   * @param totalArraySpecCount A count of ArraySpec objects placed
   * so far
   * @param fileName The path of the file that contains the variable 
   * that partitions are being generated for
   * @param conf Configuration object for the program currently running
   * @return  the running total of ArraySpecs seen so far
   */
  private long samplingPlacement(
                  int datasetID, ArraySpec[] records, BlockLocation[] blocks,
                  HashMap<BlockLocation, ArrayList<ArraySpec>> blockToArrays, 
                  long totalArraySpecCount, String fileName,
                  Configuration conf ) 
                  throws IOException, HDF5LibraryException, HDF5Exception{

    long blocksPlacedSoFar = 0; 
    int recCounter = 0;
    for (ArraySpec record : records) {
      if (recCounter % 50 == 0) { 
        System.out.println("on record: " + recCounter);
      } 
      recCounter++;

      BlockLocation block = arrayToBlock(datasetID, record, blocks, conf);
      blockToArrays.get(block).add(record);
      blocksPlacedSoFar++;
    }

    totalArraySpecCount++;

    return totalArraySpecCount;
  }

  /**
   * Create paritions via calculating the shape that the given scientific 
   * file format (NetCDF v3 in this case) would likely have generated, given
   * the shape of the data.
   * @param var Variable object representing the variable that partitions are
   * being generated for
   * @param blockToArrays a HashMap storing maps between BlockLocation and 
   * ArraySpecs
   * @param blockSize size of HDFS blocks, in bytes
   * @param dataTypeSize size of a single cell, in bytes
   * @param fileName name of the file containing the variable for which
   * partitions are being generated
   * @param startOffset the location in the logical space to begin generating
   * a partition for
   * @param conf Configuration object for the currently executing program
   */
  private void genCalculatedPartitions( 
                  int datasetID, 
                  HashMap<BlockLocation, ArrayList<ArraySpec>> blockToArrays,
                  long blockSize, int dataTypeSize, 
                  String fileName,
                  int[] startOffset,
                  Configuration conf ) 
                  throws IOException, Exception {

    /*
    LOG.info("In genCalculatedPartitions");
    // geta list of all the dimensions
    List<Dimension> dims = var.getDimensions();

    int shape[] = new int[dims.size()];
    int allOnes[] = new int[dims.size()];
    int lastCorner[] = new int[dims.size()];

    for( int i=0; i<dims.size(); i++) {
      shape[i] = dims.get(i).getLength();
      allOnes[i] = 1;
      lastCorner[i] = 0;
    }

    long[] strides = Utils.computeStrides(shape);

    while ( !Utils.endOfVariable( shape, lastCorner)) {
      nextCalculatedPartition( var, shape, lastCorner, blockSize,
      dataTypeSize, allOnes, strides, fileName, blockToArrays, startOffset, conf);
    }
    */
  }

  /**
   * Extract a string, containing a comma-delimited list of 
   * dimension lengths, and set it in the Configuration object
   * @param variable The current variable that partitions are 
   * being generated for
   * @param conf Configuration object for the currently executing job
   */
  //private void setVarDimString( Variable variable, Configuration conf ) {
    /*
  private void setVarDimString( int dataset, Configuration conf ) 
    throws HDF5LibraryException {
 
    if (!Utils.variableShapeSet() ) {

      int dataspace = H5.H5Dget_space(dataset);
      int numDims = H5.H5Sget_simple_extent_ndims(dataspace);
      long dims[] = new long[numDims];
      long maxDims[] = new long[numDims];
      H5.H5Sget_simple_extent_dims(dataspace, dims, maxDims);

      // -jbuck buck TODO: stash variable shape into conf here
      String varDimsString = "";

      for ( int i=0; i<dims.length; i++) {
        if ( i > 0 )
          varDimsString += ",";

        varDimsString += dims[i];
      }

      Utils.setVariableShape(conf, varDimsString);

      System.out.println("var dim string: " + varDimsString);
    }
  }
  */

  /**
   * This method is called by the super class to generate
   * splits via the proscribed method. File format specific
   * calls are made from here
   * @param job Context object for the currently executing job
   * @param shFileStatus A SHFileStatus object that represents 
   * the file that splits are currently being generated for
   * @param splits A list of InputSplits that will have splits 
   * added to it as the new splits are generated.
   * @param partMode The partitioning mode to use when generating splits
   * @param placementMode the placement mode that will map splits to 
   * BlockLocations
   */
  private void genFileSplits(JobContext job, SHFileStatus shFileStatus,
                             List<ArrayBasedFileSplit> splits, PartMode partMode, 
                             PlacementMode placementMode ) throws IOException, HDF5Exception {

    FileStatus fileStatus = shFileStatus.getFileStatus();
    Path path  = fileStatus.getPath();
    long fileLen = fileStatus.getLen();
    Configuration conf = job.getConfiguration();

    FileSystem fs = Utils.getFS(conf);

    LOG.info("Uri: " + fs.getUri());

    long blockSize = fileStatus.getBlockSize();
    LOG.debug("File " + fileStatus.getPath() + " has blocksize: " + 
              blockSize + " and file size: " + fileStatus.getLen() );

    long totalArraySpecCount = 0;

    // Create and initialize the block-to-array mapping
    BlockLocation[] blocks = fs.getFileBlockLocations(fileStatus, 0, fileLen);
    long numBlocks = blocks.length;

    // Create the HashMap for the result and then initialize the elements
    HashMap<BlockLocation, ArrayList<ArraySpec>> blockToArrays =
      new HashMap<BlockLocation, ArrayList<ArraySpec>>();

    for (BlockLocation block : blocks) {
      blockToArrays.put(block, new ArrayList<ArraySpec>());
    }

    Path filePath = Utils.convertToMountPath(fileStatus.getPath(), conf);

    int fapl = -1;
    int hdf5FileID = -1;
    int error = -1;
    try { 
      fapl = H5.H5Pcreate(HDF5Constants.H5P_FILE_ACCESS);

      hdf5FileID = H5.H5Fopen(filePath.toString(), HDF5Constants.H5F_ACC_RDONLY,
                              fapl);
    } catch (Exception e) { 
      LOG.error("exception raised: " + e.toString());
    }

    // Pull the dimensions out of the variable being processed
    int datasetID = getDataset(hdf5FileID, job.getConfiguration());
    int dims[] = getDims(datasetID);

    System.out.println("Dataset dims: " +  Arrays.toString(dims) );

    // close the file handle
    try { 
      error = H5.H5Fclose(hdf5FileID);
      if (error < 0) { 
        LOG.error("H5Fclose error, returned " + error);
      }
      error = H5.H5Pclose(fapl);
      if (error < 0) { 
        LOG.error("H5Pclose error, returned " + error);
      }
    } catch (Exception e) { 
      LOG.error("exception raised: " + e.toString());
    }


    //setVarDimString( datasetID, job.getConfiguration() );
    //ArrayList<Dimension> dimList =
    //    new ArrayList<Dimension>(variable.getDimensions());

    //int[] dims = new int[dimList.size()];
    //for( Dimension dim : dimList ) {
    //}

    //for ( int i=0; i<dimList.size(); i++) { 
      //dims[i] = dimList.get(i).getLength();
    //}


    int dataTypeSize = H5.H5Dget_type(datasetID);

    String configuredDatasetName = Utils.getVariableName(job.getConfiguration());

    LOG.info("Partition mode: " + 
             Utils.getPartModeString(job.getConfiguration() ) + 
             " placement mode: " + 
             Utils.getPlacementModeString(job.getConfiguration() )
            );
    // first do the partitioning
    LOG.info("\t!!!! starting to partition for variable " + 
             configuredDatasetName + " !!!!!");

    ArraySpec[] records = new ArraySpec[0]; // this will hold the results

    // generate splits for the data
    switch ( partMode) { 

      case proportional: 
        System.out.println("calling porportionalPartition with: " + 
          " dims: " + Utils.arrayToString(dims) + 
          " variable name: " + configuredDatasetName + 
          " blocksize: " + blockSize + 
          " numBlocks: " + numBlocks + 
          " fileLen: " + fileLen + 
          " dataTypeSize: " + dataTypeSize + 
          " file: " + fileStatus.getPath().toString() + 
          " startOffset: " + shFileStatus.getStartOffset());
          
        records = proportionalPartition( dims, configuredDatasetName, 
                                         blockSize, numBlocks, fileLen, 
                                         dataTypeSize, fileStatus.getPath().toString(),
                                         shFileStatus.getStartOffset(),
                                         job.getConfiguration());
          System.out.println("returned " + records.length + " records");
        break;

        case record:
        System.out.println("calling recordPartition with: " + 
          " dims: " + Utils.arrayToString(dims) + 
          " variable name: " + configuredDatasetName + 
          " blocksize: " + blockSize + 
          " numBlocks: " + numBlocks + 
          " fileLen: " + fileLen + 
          " dataTypeSize: " + dataTypeSize + 
          " file: " + fileStatus.getPath().toString() + 
          " startOffset: " + shFileStatus.getStartOffset());

          records = recordBasedPartition( dims, configuredDatasetName,
                                          fileStatus.getPath().toString(),
                                          partMode,
                                          shFileStatus.getStartOffset(),
                                          job.getConfiguration());
          System.out.println("returned " + records.length + " records");
          break;


        case calculated:
          try {
            genCalculatedPartitions( datasetID, blockToArrays, blockSize, 
                                     dataTypeSize, fileStatus.getPath().toString(),
                                     shFileStatus.getStartOffset(),
                                     job.getConfiguration());
          } catch (IOException ioe ) {
            System.out.println("Caught an ioe in genCalculatedPartitions\n" + 
                               ioe.toString() );
          } catch (Exception e ) {
            System.out.println("Caught an e in genCalculatedPartitions\n" + 
                               e.toString() );
          }
          break;
     }

     LOG.info("\t!!!! starting placement for variable " + 
              configuredDatasetName + " !!!!!");

      // now place the splits you just generated
      switch( placementMode ) {
        case roundrobin:
          if ( (records != null) && (records.length > 0) )
            totalArraySpecCount =  roundRobinPlacement(records, blocks, 
                                                       blockToArrays, 
                                                       totalArraySpecCount, 
                                                       fileStatus.getPath().toString(),
                                                       job.getConfiguration());
          break;
  
        case sampling:
          if ( (records != null) && (records.length > 0) )
            totalArraySpecCount =  samplingPlacement(datasetID, records, 
                                                     blocks, 
                                                     blockToArrays, 
                                                     totalArraySpecCount,
                                                     fileStatus.getPath().toString(),
                                                     job.getConfiguration());
          break;
  
        case implicit: 
        // this is a no-op, just putting this here for symmetry's sake
          break;
    }

    LOG.info("\t!!!! done with placement for variable " + 
             configuredDatasetName  + " !!!!!");

     // Remove any blocks from the list with empty logical spaces
    Iterator<Map.Entry<BlockLocation, ArrayList<ArraySpec>>> blockMapIter;
    blockMapIter = blockToArrays.entrySet().iterator();

    while (blockMapIter.hasNext()) {
      Map.Entry<BlockLocation, ArrayList<ArraySpec>> entry = 
          blockMapIter.next();
      ArrayList<ArraySpec> arrays = entry.getValue();
      if (arrays.isEmpty()) {
        blockMapIter.remove();
      }
    }

    
     // Create a split for each block, and assign to this split the ArraySpec
     // instances that are associated with the block.
     // 
     // This is fundamentally our logical space decomposition that is aware of
     // the physical layout.
    blockMapIter = blockToArrays.entrySet().iterator();

    while (blockMapIter.hasNext()) {
      Map.Entry<BlockLocation, ArrayList<ArraySpec>> entry = 
          blockMapIter.next();
      ArrayList<ArraySpec> arrays = entry.getValue();
      ArrayBasedFileSplit split = 
          new ArrayBasedFileSplit(path, arrays, entry.getKey().getHosts());
      splits.add(split);
    }
  }

    /** 
     * Open the file represented by FileStatus and return a Variable
     * object for the variable specified in the job arguements
     * @param file FileStatus object representing the file from which the 
     * variable should be extracted
     * @param conf Configuration object for the currently running job
     * @return a Variable obejct representing the variable, for this job,
     * from the file specified by the file arguement
    */
    public int getDataset( int hdf5FileID, Configuration conf) { 
	    int retInt = -1;

      try { 
	      //Path path = file.getPath();

        String configuredDatasetName = Utils.getVariableName(conf);

        //int fapl = H5.H5Pcreate(HDF5Constants.H5P_FILE_ACCESS);
        //int error = H5.H5Pset_fapl_ceph(fapl, cephConfPath);
        //String fileToOpen = Utils.stripURIInfo(path.toString(), 
        //                       conf.get("fs.default.name", ""));

        //int hdf5FileID = H5.H5Fopen(fileToOpen, HDF5Constants.H5F_ACC_RDONLY,
         //                             fapl);
        retInt = H5.H5Dopen(hdf5FileID, configuredDatasetName, 
                            HDF5Constants.H5P_DEFAULT);

        //long dataType = H5.H5Dget_type(retInt);
        //System.out.println("H5Dget_type returned " + dataType);
        //Utils.setOutputDataTypeSize(dataType);
        //H5.H5Pclose(fapl);
	    } catch (Exception e) {
	      System.out.println("Caught an exception in ArrayBasedFileInputFormat.getVariable()" + 
	                         e.toString() );
	    }
	
	    return retInt;
    }
 
  /**
   * Order a set of files and validate that they are valid files for this
   * program.
   * @param files List of FileStatus objects that represent the files 
   * in the input space
   * @param conf Configuration object for the currently executing MR program
   * @return A list of SHFileStatus objects representing the order the files
   * should have splits generated in
   */
  public List<SHFileStatus> orderMultiFileInput( List<FileStatus> files,
                                                 Configuration conf ) 
                                                 throws HDF5LibraryException{ 
    List<SHFileStatus> retList = new ArrayList<SHFileStatus>();

    // first, sort the files in alphanumeric order
    Collections.sort(files);

    int fapl = -1;
    int hdf5FileID = -1;
    int error = -1;
    int[] startOffset = null;
    // now go through them, in order
    for (FileStatus file: files) {

      Path filePath = Utils.convertToMountPath(file.getPath(), conf);
      try { 
        fapl = H5.H5Pcreate(HDF5Constants.H5P_FILE_ACCESS);

      } catch (Exception e) { 
        LOG.error("exception raised in orderMultiFileInput, H5Pcreate(): " + e.toString());
      }

      try { 
        hdf5FileID = H5.H5Fopen(filePath.toString(), HDF5Constants.H5F_ACC_RDONLY,
                                fapl);
      } catch (Exception e) { 
        LOG.error("exception raised in orderMultiFileInput, H5Fopen: " + e.toString() + 
                  " file path: " + filePath.toString());
      }

      int dataset = getDataset(hdf5FileID, conf); 

      int dataspace = H5.H5Dget_space(dataset);
      int numDims = H5.H5Sget_simple_extent_ndims(dataspace);

      // these need to be long[] to appease HDF5
      long[] dims = new long[numDims];
      long [] maxDims = new long[numDims];
      H5.H5Sget_simple_extent_dims(dataspace, dims, maxDims);

      int[] retDims = new int[dims.length];
      for( int i=0; i<retDims.length; i++) { 
        retDims[i] = (int)dims[i];
      }

      if ( startOffset == null ){
        startOffset = new int[numDims];
        for( int i=0; i<startOffset.length; i++) {
          startOffset[i] = 0;
        }
      }

      if ( Utils.getMultiFileMode(conf) == MultiFileMode.combine ) {
        retList.add(new SHFileStatus(file, startOffset) );
      }  else { // default to concat mode
        retList.add(new SHFileStatus(file, startOffset) );
        // add the length of *the* variable in this file to the 
        // start of the next file
        startOffset[0] += retDims[0];
      }

      // close the file handle
      try { 
        error = H5.H5Fclose(hdf5FileID);
        if (error < 0) { 
          LOG.error("H5Fclose error, returned " + error);
        }
        error = H5.H5Pclose(fapl);
        if (error < 0) { 
          LOG.error("H5Pclose error, returned " + error);
        }
      } catch (Exception e) { 
        LOG.error("exception raised: " + e.toString());
      }
    }

    return retList;
  }

  @Override
  /**
   * this over rides the getSplits method in ArrayBasedFileInputFormat.
   * This is the method that actually returns the splits to be scheduled and 
   * executed by Hadoop.
   * @param job The JobContext object for the currently executing job
   * @return A List of InputSplit objects to be scheduled and run by Hadoop
   */
  public List<InputSplit> getSplits(JobContext job) throws IOException {

    /*
    try { 
      H5.H5loadGlobalLibraries();
    } catch( Exception e) {
      e.printStackTrace();
    }
    */

    ArrayList<ArrayBasedFileSplit> splits; 
    List<InputSplit> retSplits = new ArrayList<InputSplit>();
    List<FileStatus> files = listStatus(job);
    List<SHFileStatus> shFiles = new ArrayList<SHFileStatus>();

    splits = new ArrayList<ArrayBasedFileSplit>();
    /*
    HashMap<Long, ArrayList<ArraySpec>> blockToSlab =
      new HashMap<Long, ArrayList<ArraySpec>>();

    LOG.info("\tDebug Print");
    for (FileStatus file: files) {
      LOG.info("\t" + file.getPath().toUri().toString());
    }



    FileStatus fileStatus = null;


    for (FileStatus file: files) {
      if (fileStatus == null) {
        fileStatus = file;
        LOG.info("Using input: " + file.getPath().toString());
      } else {
        LOG.warn("Skipping input: " + file.getPath().toString());
      }
    }

    if (ncfileStatus == null)
      return splits;
*/

    PartMode partMode = Utils.getPartMode(job.getConfiguration());
    PlacementMode placementMode = 
        Utils.getPlacementMode(job.getConfiguration());

    /*
    if (Utils.getMultiFileMode(job.getConfiguration()) == MultiFileMode.concat) {
      orderMultiFileInput( files, shFiles);
    }
    */

    try { 
      // set the starting offset for each file (depends on damasc.multi_file_mode 
      shFiles = orderMultiFileInput( files, job.getConfiguration() );
    } catch (HDF5LibraryException hle) { 
      LOG.error("HDF5LibraryException in getSplits(): " + hle.getMessage());
    }
  
    for (SHFileStatus shFile: shFiles) {
      LOG.info("Parsing file: " + shFile.getFileStatus().getPath().toString());
      Utils.addFileName(shFile.getFileStatus().getPath().toString(), job.getConfiguration());
      try { 
        genFileSplits(job, shFile, splits, partMode, placementMode);
      } catch( HDF5Exception e) { 
        LOG.error("HDF5Exception: " + e.getMessage());
      }
    }

    Collections.sort(splits);
    
    // debug: log splits to a file if the debug log files is set
    String debugFileName = Utils.getDebugLogFileName(job.getConfiguration());
    if ( "" != debugFileName ) {  
      LOG.info("Trying to log to " + debugFileName);
      File outputFile = new File( debugFileName );
      BufferedWriter writer = new BufferedWriter( new FileWriter(outputFile));

      int i = 0;
      for (InputSplit split : splits) {
        ArrayBasedFileSplit tempSplit = (ArrayBasedFileSplit)split;
        //LOG.info("Split " + i);
        writer.write("Splits " + i);
        writer.newLine();
        for ( ArraySpec spec : tempSplit.getArraySpecList() ) {
          writer.write("File: " + spec.getFileName() + 
                       "\tvar: " + spec.getVarName()); 
          writer.write( "\tcorner: " + Utils.arrayToString( spec.getCorner())); 
          writer.write( "\t shape: " + Utils.arrayToString( spec.getShape())); 
          writer.write("\t startOffset: " + Utils.arrayToString( spec.getLogicalStartOffset()) );
          writer.newLine();
        }
        i++;
      }
      writer.close();
    } else {
      LOG.info("No debugFileName set");
    }

    for( int i=0; i<splits.size(); i++) { 
        retSplits.add((InputSplit)(splits.get(i)));
    }

    return retSplits;
  }

    //long dims[] = getDims(dataset);
  private int[] getDims(int datasetID) throws HDF5LibraryException{ 
    int dataspaceID = H5.H5Dget_space(datasetID);
    int numDims = H5.H5Sget_simple_extent_ndims(dataspaceID);

    long dims[] = new long[numDims];
    long maxDims[] = new long[numDims];
    H5.H5Sget_simple_extent_dims(dataspaceID, dims, maxDims);
    int[] retDims = new int[dims.length];
    for( int i=0; i<retDims.length; i++) { 
      retDims[i] = (int)dims[i];
    }

    return retDims;
  }

  @Override
  /**
   * Creates a RecordReader for NetCDF files
   * @param split The split that this record will be processing
   * @param context A TaskAttemptContext for the task that will be using
   * the returned RecordReader
   * @return A NetCDFRecordReacer 
   */
  public RecordReader<ArraySpec, ByteBuffer> createRecordReader( InputSplit split, 
                                                TaskAttemptContext context )
                                                throws IOException { 
    HDF5RecordReader reader = 
        new HDF5RecordReader();
    reader.initialize( (ArrayBasedFileSplit) split, context);

    return reader;
  }

}
