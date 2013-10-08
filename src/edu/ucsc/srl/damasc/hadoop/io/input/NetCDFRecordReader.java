package edu.ucsc.srl.damasc.hadoop.io.input;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import ucar.ma2.InvalidRangeException;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import ucar.unidata.io.RandomAccessFile;
import edu.ucsc.srl.damasc.hadoop.Utils;
import edu.ucsc.srl.damasc.hadoop.io.ArraySpec;
import edu.ucsc.srl.damasc.hadoop.io.NetCDFTools;


/**
 * NetCDF specific code for reading data from NetCDF files.
 * This class is used by Map tasks to read the data assigned to them
 * from NetCDF files. 
 * TODO: don't copy data out of an InputSplit and the store it internally.
 * Rather, keep the split around and just access the data as needed
 */
public class NetCDFRecordReader
      extends RecordReader<ArraySpec,ByteBuffer> {

  private static final Log LOG = LogFactory.getLog(NetCDFRecordReader.class);
  private long _timer;
  private int _numArraySpecs;

  //this will cause the library to use its default size
  //private int _bufferSize = -1; 

  private NetcdfFile _ncfile = null;
  //private NcHdfsRaf _raf = null;
  //private NcCephRaf _raf = null;

  //private RandomAccessFile _raf = null;

  private Variable _curVar; // actual Variable object
  private String _curVarName; // name of the current variable that is open
  // how many data elements were read the last step 
  private long _totalDataElements = 1; 

  // how many data elements have been read so far (used to track work done)
  private long _elementsSeenSoFar = 0; 

  private ArrayList<ArraySpec> _arraySpecArrayList = null;

  private ArraySpec _currentArraySpec = null; // this also serves as key
  private ByteBuffer _value = null;
  private Configuration _conf = null;

  /**
   * Resets a RecordReader each time it is passed a new InputSplit to read
   * @param genericSplit an InputSplit (really an ArrayBasedFileSplit) that
   * needs its data read
   * @param context TaskAttemptContext for the currently executing progrma
  */
  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext context) 
                         throws IOException {
      
    this._timer = System.currentTimeMillis();
    ArrayBasedFileSplit split = (ArrayBasedFileSplit)genericSplit;
    this._numArraySpecs = split.getArraySpecList().size();
    this._conf = context.getConfiguration();

    Path path = split.getPath();

    //this._raf = NetCDFTools.getRAF(context.getConfiguration(), path.toString());

    this._arraySpecArrayList = split.getArraySpecList();

    // calculate the total data elements in this split
    this._totalDataElements = 0;

    for ( int j=0; j < this._arraySpecArrayList.size(); j++) {
      this._totalDataElements += this._arraySpecArrayList.get(j).getSize();
    }


    //this._raf = new NcHdfsRaf(fs.getFileStatus(path), job, this._bufferSize);
    //this._raf = new NcCephRaf(cfs.getFileStatus(path), job, this._bufferSize);
    //this._ncfile = NetcdfFile.open(this._raf, path.toString());

    // rip off the Ceph URI
    String newFilePath = stripCephURI(path.toString());
    LOG.info("newFilePath: " + newFilePath);

    File filePath = 
      new File(Utils.getCephMountPoint(this._conf), newFilePath);
    LOG.info("JB, opening file " + filePath.toString());
    this._ncfile = NetcdfFile.open(filePath.toString());
     
    // try to compact the specified arraySpecs into larger groups
    //compactArraySpecList( this._arraySpecArrayList);
    int listSize = this._arraySpecArrayList.size();
    ArraySpec.compactList( this._arraySpecArrayList);
    LOG.info("ArraySpec.compactList went from " + listSize + " to " + 
      this._arraySpecArrayList.size() + " elements");
  }

  /*
  private void compactArraySpecList( ArrayList<ArraySpec> arraySpecList) { 
    int previousSize= arraySpecList.size();
    for( int i=0; i<arraySpecList.size; i++ ) { 
      
    }
  }
  */

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
      if ( this._curVarName == null ||  
          0 != (this._currentArraySpec.getVarName()).compareTo(this._curVarName)){
        LOG.debug("calling getVar on " + this._currentArraySpec.getVarName() );
        this._curVar = 
            this._ncfile.findVariable(this._currentArraySpec.getVarName());
      }
            

      if ( this._curVar ==null ) {
        LOG.warn("this._curVar is null. BAD NEWS");
        LOG.warn( "file: " + this._currentArraySpec.getFileName() + 
            "corner: " +   
            Arrays.toString(this._currentArraySpec.getCorner() ) + 
            " shape: " + Arrays.toString(this._currentArraySpec.getShape() ) );
      }

      LOG.warn( " File: " + this._currentArraySpec.getFileName() + 
                " startOffset: " + Utils.arrayToString(this._currentArraySpec.getLogicalStartOffset()) + 
               "corner: " + 
               Arrays.toString(this._currentArraySpec.getCorner()) + 
               " shape: " + 
               Arrays.toString(this._currentArraySpec.getShape()));

      // this next bit is to be able to set the dimensions of the variable
      // for this ArraySpec. Needed for flattening the groupID to a long
      ArrayList<Dimension> varDims = 
          new ArrayList<Dimension>(this._curVar.getDimensions());
      int[] varDimLengths = new int[varDims.size()];

      for( int i=0; i<varDims.size(); i++) {
        varDimLengths[i] = varDims.get(i).getLength();
      }
                
      this._currentArraySpec.setVariableShape(varDimLengths);
            
      long timerA = System.currentTimeMillis();

      LOG.info("Reading in " + Utils.calcTotalSize(this._currentArraySpec.getShape()) );

      // read the NetCDF Array object from the variable and instantly turn
      // it into a ByteBuffer (the desired format)
      this._value = (
                     (this._curVar.read(this._currentArraySpec.getCorner(), 
                                                this._currentArraySpec.getShape()
                    ))
                    ).getDataAsByteBuffer();

      long timerB = System.currentTimeMillis();
      LOG.info("IO time: " + (timerB - timerA) + " for " + 
               this._value.capacity() + " bytes");
    } catch (InvalidRangeException ire) {
    // convert the InvalidRangeException (netcdf specific) 
    // to an IOException (more general)
      throw new IOException("InvalidRangeException caught in " + 
                            "NetCDFRecordReader.loadDataFromFile()" + 
                            "corner: " + 
                           Arrays.toString(this._currentArraySpec.getCorner()) +
                            " shape: " + 
                            Arrays.toString(this._currentArraySpec.getShape()));
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
      if (this._ncfile != null) {
        this._ncfile.close();
      }
    } catch (IOException ioe) {
      LOG.warn("ioe thrown in NetCDFRecordReader.close()\n");
    }
  }

  private String stripCephURI(String filePath ) { 
    String cephURI = "ceph:/null/";
    return filePath.substring(cephURI.length());

  }
}
