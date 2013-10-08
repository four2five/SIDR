package edu.ucsc.srl.damasc.hadoop.io.input;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;

import java.lang.reflect.Array;

import ncsa.hdf.hdf5lib.H5;
import ncsa.hdf.hdf5lib.HDF5Constants;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;
import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.ucsc.srl.damasc.hadoop.io.ArraySpec;
import edu.ucsc.srl.damasc.hadoop.io.input.ArrayBasedFileSplit;
import edu.ucsc.srl.damasc.hadoop.Utils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * HDF5 specific code for reading data from HDF5 files.
 * This class is used by Map tasks to read the data assigned to them
 * from HDF5 files. 
 * TODO: don't copy data out of an InputSplit and the store it internally.
 * Rather, keep the split around and just access the data as needed
 */
public class HDF5RecordReader 
      extends RecordReader<edu.ucsc.srl.damasc.hadoop.io.ArraySpec, ByteBuffer> {

  private static final Log LOG = LogFactory.getLog(HDF5RecordReader.class);
  private long _timer;
  private int _numArraySpecs;

  //this will cause the library to use its default size
  private int _bufferSize = -1; 

  private int _hdf5FileID = -1;
  private int _dataspaceID = -1; // ID for the dataspace
  private int _datasetID = -1; // ID for the dataspace
  private int _fapl = -1;
  private int _hdf5DataType = -1;
  private int _dataTypeSize = -1;

  //private <T extends Class>T _javaClass = null;
  //private Class _javaClass = null;
  //private int _facl = -1;

  private String _datasetName; // name of the dataset
  private String _curFileName;

  // how many data elements were read the last step 
  private long _totalDataElements = 1; 

  // how many data elements have been read so far (used to track work done)
  private long _elementsSeenSoFar = 0; 

  private ArrayList<ArraySpec> _arraySpecArrayList = null;

  private ArraySpec _currentArraySpec = null; // this also serves as key
  private ByteBuffer _value = null;
  private int _currentArraySpecIndex = 0;
  private long _onesArray[] = null; 

  /**
   * Resets a RecordReader each time it is passed a new InputSplit to read
   * @param genericSplit an InputSplit (really an ArrayBasedFileSplit) that
   * needs its data read
   * @param context TaskAttemptContext for the currently executing progrma
  */
  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext context) 
                         throws IOException {

    LOG.info("In initialize for HDF5RecordReader");
      
    this._timer = System.currentTimeMillis();
    ArrayBasedFileSplit split = (ArrayBasedFileSplit)genericSplit;
    this._numArraySpecs = split.getArraySpecList().size();
    Configuration conf = context.getConfiguration();

    Path path = split.getPath();

    this._arraySpecArrayList = split.getArraySpecList();

    // calculate the total data elements in this split
    this._totalDataElements = 0;

    for ( int j=0; j < this._arraySpecArrayList.size(); j++) {
      this._totalDataElements += this._arraySpecArrayList.get(j).getSize();
    }

    // get the buffer size
    this._bufferSize = Utils.getBufferSize(conf);

    //this._raf = new NcHdfsRaf(fs.getFileStatus(path), job, this._bufferSize);
    //this._raf = new NcCephRaf(cfs.getFileStatus(path), job, this._bufferSize);

    try{ 
      this._fapl = H5.H5Pcreate(HDF5Constants.H5P_FILE_ACCESS);

      // pull the cephConfPath out of the Configuration file
     // String cephConfPath = Utils.getCephConfPath(job);

      //LOG.info("in HDF5RecordReader, setting fapl with confpath: " + cephConfPath);
      //H5.H5Pset_fapl_ceph(this._fapl, cephConfPath);

      //String fileToRead = Utils.stripURIInfo(path.toString(), 
         //context.getConfiguration().get("fs.default.name", ""));
      Path filePath = Utils.convertToMountPath(path, conf);

      LOG.info("In HDF5RecordReader, opening file" + filePath.toString());
      this._hdf5FileID = H5.H5Fopen(filePath.toString(), HDF5Constants.H5F_ACC_RDONLY,
                                    this._fapl);
    } catch ( HDF5LibraryException le){
      le.printStackTrace();
    }

  }

  /** this is called to load the next key/value in. The actual data is retrieved
   * via getCurrent[Key|Value] calls
   * @return a boolean that is true if there is more data to be read, 
   * false otherwise
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if ( !this._arraySpecArrayList.isEmpty() ) {
      // set the current element
      this._currentArraySpec = this._arraySpecArrayList.get(0);

      // then delete it from the ArrayList
      this._arraySpecArrayList.remove(0);

      // fixing an entirely random bug -jbuck TODO FIXME
      if ( this._currentArraySpec.getCorner().length <= 1 ) {
        return this.nextKeyValue();
      }

      // transfer the data
      loadDataFromFile(); 

      return true;
    } else {
      this._timer = System.currentTimeMillis() - this._timer;
      LOG.debug("from init() to nextKeyValue() returning false, " +
                "this record reader took: " + this._timer + 
                " ms. It had " + this._numArraySpecs + 
                " ArraySpecs to process" );

      return false;
    }
  }

  /**
   * Load data into the value element from disk.
   * Currently this only supports IntWritable. Extend this 
   * to support other data types TODO
   */
  private void loadDataFromFile() throws IOException {
    try { 

      // reuse the open variable if it's the correct one
      // ArraySpec.getVarName() is used as DataSet name for HDF5
      //private String _dataSetName; // name of the dataset

      if ( this._datasetName == null ||  
          0 != (this._currentArraySpec.getVarName()).compareTo(this._datasetName)){

        LOG.debug("calling H5Dopen on " + this._currentArraySpec.getVarName() );

        this._datasetID = 
            H5.H5Dopen( this._hdf5FileID, this._currentArraySpec.getVarName(),
                        HDF5Constants.H5P_DEFAULT);

        this._dataspaceID = H5.H5Dget_space(this._datasetID); 
        this._hdf5DataType = H5.H5Tcopy(this._datasetID);
        this._dataTypeSize = H5.H5Tget_size(this._hdf5DataType);
            //findVariable(this._currentArraySpec.getVarName());
      }
            

      if ( this._datasetID < 0) {
        LOG.warn("this._datasetID is < 0. BAD NEWS");
        LOG.warn( "file: " + this._currentArraySpec.getFileName() + 
            "corner: " +   
            Arrays.toString(this._currentArraySpec.getCorner() ) + 
            " shape: " + Arrays.toString(this._currentArraySpec.getShape() ) );
      }

      LOG.warn( " File: " + this._currentArraySpec.getFileName() + 
                " startOffset: " + 
                Utils.arrayToString(this._currentArraySpec.getLogicalStartOffset()) + 
                " corner: " + 
                Arrays.toString(this._currentArraySpec.getCorner()) + 
                " shape: " + 
                Arrays.toString(this._currentArraySpec.getShape()));

      // this next bit is to be able to set the dimensions of the variable
      // for this ArraySpec. Needed for flattening the groupID to a long

      /*
      ArrayList<Dimension> varDims = 
          new ArrayList<Dimension>(this._curVar.getDimensions());
      */
      int numDims = H5.H5Sget_simple_extent_ndims(this._dataspaceID);

      // setup the ones array if not already set 
      if( null == this._onesArray) { 
        this._onesArray = new long[numDims];
        for( int i=0; i<this._onesArray.length; i++) { 
          this._onesArray[i] = 1;
        }
      }

      long[] dimLengths = new long[numDims];
      long[] maxLengths = new long[numDims];
      int returnedDims = H5.H5Sget_simple_extent_dims(this._dataspaceID,
                         dimLengths, maxLengths);
      if( returnedDims != numDims ) { 
        System.out.println("Dataspace said it had " + numDims + 
                           " dimensions but then only returned " + 
                           returnedDims + " dimension lengths");
      }

      int[] intDimLengths = new int[dimLengths.length];
      for( int i=0; i<intDimLengths.length; i++) {
       intDimLengths[i] = (int)dimLengths[i];
      }

      this._currentArraySpec.setVariableShape(intDimLengths);
            
      long timerA = System.currentTimeMillis();
      /*
      this._value = this._curVar.read( this._currentArraySpec.getCorner(), 
                                       this._currentArraySpec.getShape()
                                     );
      */

      // get the java class to store the data
      this._hdf5DataType = H5.H5Dget_type( this._datasetID);
      //this._javaClass = 
          //HDF5Utils.convertHDF5DataTypeToJavaClass(this._hdf5DataType);

      // sort out the total number of elements that will be read
      long totalReadSize = Utils.calcTotalSize(this._currentArraySpec.getShape());

      // create the array to store the data
      //this._value = Array.newInstance(this._javaClass, (int)totalReadSize); 

      this._value = ByteBuffer.allocate( this._dataTypeSize * (int)totalReadSize);
      this._value.order(ByteOrder.LITTLE_ENDIAN);

      // create the selection for the read
      int memspace_id = -1;

      long[] hdf5Shape = new long[this._currentArraySpec.getShape().length];
      long[] hdf5Corner = new long[this._currentArraySpec.getCorner().length];
      for( int i=0; i<hdf5Shape.length; i++) { 
        hdf5Shape[i] = (long)this._currentArraySpec.getShape()[i];
        hdf5Corner[i] = (long)this._currentArraySpec.getCorner()[i];
      }

      memspace_id = H5.H5Screate_simple(numDims, hdf5Shape, hdf5Shape);

      //long[][] selectionArray = 
      //    new long[totalReadSize][this._currentArraySpec.getShape().length];
      
      //int error = H5.H5Sselect_hyperslab(memspace_id, HDF5Constants.H5S_SELECT_SET,
                                         
                                        
      int error = H5.H5Sselect_hyperslab(this._dataspaceID, 
                                         HDF5Constants.H5S_SELECT_SET,
                                         //this._currentArraySpec.getCorner(), 
                                         hdf5Corner, 
                                         this._onesArray,
                                         //this._currentArraySpec.getShape(),
                                         hdf5Shape,
                                         this._onesArray
                                        );

      // do the actual read
      H5.H5Dread(this._datasetID, this._hdf5DataType, memspace_id, this._dataspaceID,
                 HDF5Constants.H5P_DEFAULT, this._value.array());

      // close the memspace as we won't need it again
      H5.H5Sclose(memspace_id);

      long timerB = System.currentTimeMillis();
      LOG.info("IO time: " + (timerB - timerA) + " for " + 
                this._value.capacity() + 
               " bytes");
    
    } catch (HDF5LibraryException le) {
      le.printStackTrace();
      /*
      throw new IOException("HDF5LibraryException caught in " + 
                            "HDF5RecordReader.loadDataFromFile()" + 
                            "corner: " + 
                           Arrays.toString(this._currentArraySpec.getCorner()) +
                            " shape: " + 
                            Arrays.toString(this._currentArraySpec.getShape()));
      */
    } catch (HDF5Exception e) {
      e.printStackTrace();
    /*
      throw new IOException("HDF5Exception caught in " + 
                            "HDF5RecordReader.loadDataFromFile()" + 
                            "corner: " + 
                           Arrays.toString(this._currentArraySpec.getCorner()) +
                            " shape: " + 
                            Arrays.toString(this._currentArraySpec.getShape()));
    */
    }
    
  }
  
  /**  
   * Update _elementsSeenSoFar so that getProgress() is correct(-ish).
   * This is used to track task (and job) progress.
   */
  private void updateProgress() {
    this._elementsSeenSoFar += this._currentArraySpec.getSize();
  }

  /**
   * Returns the current key
   * @return An ArraySpec object indicating the current key
   */
  @Override
  public ArraySpec getCurrentKey() {
    // updates the counter for work done so far
    updateProgress();

    // extract the cell coordinate pointed to by the current key
    return this._currentArraySpec;
  }

  /**
   * return the current Value, in the Key/Value sense.
   * @return an Array object containing the values that correspond 
   * to the ArraySpec that is the current Key
   */
  @Override
  public ByteBuffer getCurrentValue() {
    return this._value;
  }

  /**
   * Returns this jobs progress to the JobTracker so that the GUIs
   * can be updated.
   * @return a float representing the percent of this job that is
   * compeleted (between 0 and 1)
   */
  @Override
  public float getProgress() throws IOException, InterruptedException {
    return (float)(this._elementsSeenSoFar / this._totalDataElements);
  }

  /**
   * Close files that were opened by the Record Reader and do general cleanup
   */
  @Override
  public void close() throws IOException {
    try {
      /*
      if (this._ncfile != null) {
        this._ncfile.close();
      }
      */
      if( this._datasetID >= 0)
        H5.H5Dclose(this._datasetID);

      if( this._dataspaceID >= 0)
        H5.H5Sclose(this._dataspaceID);

      if( this._hdf5FileID >= 0)
        H5.H5Fclose(this._hdf5FileID);

      if( this._fapl >= 0)
        H5.H5Pclose(this._fapl);

    } catch (HDF5LibraryException le) {
      LOG.warn("le thrown in HDF5RecordReader.close()\n");
    }
  }
}
