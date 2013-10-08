package edu.ucsc.srl.damasc.hadoop.io.output;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

//import java.lang.reflect.Array;

import java.util.concurrent.atomic.AtomicLong;

import ncsa.hdf.hdf5lib.H5;
import ncsa.hdf.hdf5lib.HDF5Constants;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;
import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

import org.apache.hadoop.io.IntWritable;


import edu.ucsc.srl.damasc.hadoop.HadoopUtils;
import edu.ucsc.srl.damasc.hadoop.HDF5Utils;
import edu.ucsc.srl.damasc.hadoop.io.ArraySpec;
//import edu.ucsc.srl.damasc.hadoop.io.GroupID;
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
public class HDF5FileOutputFormat<K, V> extends FileOutputFormat<K, V> {

	private static final Log LOG = LogFactory.getLog(HDF5FileOutputFormat.class);

  protected static class HDF5Writer<K, V> extends RecordWriter<K, V> { 

	  private int _hdf5FileID = -1;
    private Configuration _conf;
    private ByteBuffer _outBuffer;
    private int _dataTypeSize;
	  private int[] _onesArray = null; 
	  private long[] _onesArrayLong = null; 
	  private long[] _zeroesArrayLong = null; 
    private long[] _totalOutputShape;
    private long[] _reducerWriteCorner;
    private long[] _reducerWriteShape;
    private long[] _writeCorner;
    private int _dummyCounter;
	  //private long _timer;
	
	  //this will cause the library to use its default size
	  private int _bufferSize = -1; 
	  private int _dataspaceID = -1; // ID for the dataspace
	  private int _datasetID = -1; // ID for the dataspace
    private int _memspaceID = -1;
    private int DATATYPE = HDF5Constants.H5T_NATIVE_INT;
	
	  //private String _datasetName; // name of the dataset
	  //private String _curFileName;
	
    //HashMap map = new HashMap();

	  // how many data elements were read the last step 
	  //private long _totalDataElements = 1; 

    //private HashMap<Long, Object> _dataHashMap = null;
    //private HashMap<Long, AtomicLong> _counterHashMap = null;
    //private long _writeSize = -1;

    //private GroupID _tempGroupID;
    //private Long _tempLong;
    //private int _recordDim;
    //private boolean _writeTriggered = false;
    //private long _writesAfterFileWrite = 0;
    //private int[] _writeShape;
    private long[] _strides;
   // private long[] _hdf5Shape; 
    //private long[] _hdf5Corner;


	  public HDF5Writer(int fileID, int dataspaceID, int datasetID, TaskAttemptContext context) 
	                         throws IOException {
	      
      int reducerID = context.getTaskAttemptID().getTaskID().getId();
      this._hdf5FileID = fileID;	
      this._dataspaceID = dataspaceID;
      this._datasetID = datasetID;
      this._conf = context.getConfiguration();
      this._dataTypeSize = Utils.getDataTypeSize(context.getOutputValueClass());
      int[] totalOutputShape = Utils.getTotalOutputSpace( this._conf);
      this._reducerWriteCorner = 
        Utils.convertIntArrayToLongArray(HadoopUtils.getReducerWriteCorner(reducerID, this._conf));
      this._reducerWriteShape = 
        Utils.convertIntArrayToLongArray(HadoopUtils.getReducerWriteShape(reducerID, this._conf));
      this._totalOutputShape = new long[totalOutputShape.length];
      //int[] variableDimension = Utils.getVariableShape(this._conf);
      this._outBuffer = ByteBuffer.allocate(this._dataTypeSize);
      int[] extractionShape = Utils.getExtractionShape(this._conf, totalOutputShape.length);
      this._strides = Utils.computeStrides(totalOutputShape);
      //this._reducerWriteCorner = new long[totalOutputShape.length];
      //this._reducerWriteShape = new long[totalOutputShape.length];
      this._onesArray = new int[totalOutputShape.length];
      this._onesArrayLong = new long[totalOutputShape.length];
      this._writeCorner = new long[totalOutputShape.length];
      this._dummyCounter = 0;
      this._zeroesArrayLong = new long[totalOutputShape.length];

      LOG.info("Datatype Size: " + this._dataTypeSize);

      for( int i=0; i<totalOutputShape.length; i++) { 
        this._totalOutputShape[i]  = (long)totalOutputShape[i];
        this._onesArray[i] = 1;
        this._onesArrayLong[i] = (long)1;
        this._zeroesArrayLong[i] = (long)0;
      }
      /*
      try { 


        /*
        this._dataspaceID = H5.H5Screate_simple(RANK, hdf5Lengths, hdf5Lengths);
      
        this._datasetID = H5.H5Dcreate(this._hdf5FileID, "fake_dataset",
                                       DATATYPE, 
                                     this._dataspaceID,
                                     HDF5Constants.H5P_DEFAULT,
                                     HDF5Constants.H5P_DEFAULT,
                                     HDF5Constants.H5P_DEFAULT);
       } catch( HDF5LibraryException le) { 
          throw new IOException("Caught an HDF5LibraryException in HDF5Writer:" + 
          le.getMessage());
       } catch( HDF5Exception e) { 
          throw new IOException("Caught an HDF5LibraryException in HDF5Writer:" + 
          e.getMessage());
       }
        */

       //this._dataHashMap = new HashMap<Long, Object>();
       //this._counterHashMap = new HashMap<Long, AtomicLong>();

       //calculate the size of a "record"

       LOG.info("Strides: " + Utils.arrayToString(this._strides));

      
       ////this._writeShape = this._onesArray.clone();
       //this._writeSize = Utils.calcTotalSize(extractionShape) * this._dataTypeSize;

       this._outBuffer.order(ByteOrder.LITTLE_ENDIAN);

       //this._hdf5Shape = new long[this._writeShape.length];
       //this._hdf5Corner = new long[this._writeShape.length];
       //for( int i=0; i<this._hdf5Shape.length; i++) {
        //this._hdf5Shape[i] = 1;
       //}

       try{ 
          //this._memspaceID = H5.H5Screate_simple(RANK, this._hdf5Shape, this._hdf5Shape);
          this._memspaceID = H5.H5Screate_simple(this._onesArrayLong.length, 
                                                 this._onesArrayLong, 
                                                 this._onesArrayLong);
          H5.H5Sselect_hyperslab(this._memspaceID, HDF5Constants.H5S_SELECT_SET,  this._zeroesArrayLong, null, this._onesArrayLong, null);
       } catch ( HDF5Exception he) { 
        he.printStackTrace();
      }

       // this bit is hacky, just for testing at the moment
       //this._writeSize *= 6;

       //LOG.info("Write size: " + this._writeSize + 
                          //" elements with shape: " + Arrays.ToString(this._onesArrayLong));
	  }

    public synchronized void write(K key, V value, long recordsRepresented)
      throws IOException { 

        for (int i=0; i<this._writeCorner.length; i++) { 
          this._writeCorner[i] = (long)((ArraySpec)key).getCorner()[i];
        }

        this._writeCorner = 
          Utils.subtractArrayFromAnother(this._writeCorner, this._reducerWriteCorner);
        //LOG.info("JB, write at " + Arrays.toString(this._writeCorner));
        //this._tempGroupID = (GroupID)key;  
        //ByteBuffer data = (ByteBuffer)value;

        LOG.info("JB, writing " + this._dummyCounter + " at " + 
                 Arrays.toString(this._writeCorner));
        this._outBuffer.putInt( 0, ((IntWritable)value).get());
        //this._outBuffer.putInt(0, this._dummyCounter);
        this._dummyCounter++;

      try{ 

        // so we need to project global offset into the local memory space

        writeHDF5( this._outBuffer, 
                   DATATYPE, 
                   //this._tempGroupID.getGroupID(),
                   this._writeCorner,
                   //this._writeShape
                   this._onesArrayLong
                 );

      } catch ( IOException e) { 
        e.printStackTrace();
        throw new IOException("exception in write()");
      }
    }

    public synchronized void writeHDF5( ByteBuffer data, int DataType, long[] corner,
                                        long[] shape) throws IOException {

      if( corner.length != shape.length) { 
        LOG.info("Corner has len: " + corner.length + " while shape has len: " + 
                            shape.length + ". They should match and they do not.");
        Thread.dumpStack();

      }


      try {
        // select the hyperspace in the file to use
        int error = H5.H5Sselect_hyperslab(this._dataspaceID, HDF5Constants.H5S_SELECT_SET,
                                          corner, null, this._onesArrayLong, null);
      } catch (HDF5LibraryException le) { 
        le.printStackTrace();
        throw new IOException("Caught an HDF5LibraryException in writeHDF5, H5Sselect_hyperslab:" + 
          le.getMessage());
      }

      try{ 
        H5.H5Dwrite(this._datasetID, DATATYPE,
                    this._memspaceID, this._dataspaceID,
                    //HDF5Constants.H5S_ALL, this._dataspaceID,
                    HDF5Constants.H5P_DEFAULT, data.array());
      } catch (HDF5LibraryException le) { 
        le.printStackTrace();
        throw new IOException("Caught an HDF5LibraryException in writeHDF5, H5Dwrite:" + 
          le.getMessage());
      }

    }
	
	  /**
	   * Close files that were opened by the Record Reader and do general cleanup
	   */
	  @Override
	  public synchronized void close(TaskAttemptContext context) throws IOException {
      /*
      for( Long recordCorner : this._counterHashMap.keySet() ) {
        LOG.info("final count for key " + recordCorner + ":" + 
                           this._counterHashMap.get(recordCorner) );
      }
      */

	    try {

        if( this._memspaceID >= 0)
          H5.H5Sclose(this._memspaceID);
	      if( this._dataspaceID >= 0)
	        H5.H5Sclose(this._dataspaceID);
	      if( this._datasetID >= 0)
	        H5.H5Dclose(this._datasetID);
	      if( this._hdf5FileID >= 0)
	        H5.H5Fclose(this._hdf5FileID);
	
	    } catch (HDF5LibraryException le) {
	      LOG.warn("le thrown in HDF5Writer.close()\n");
	    }
	  }
	}

  public RecordWriter<K, V> getRecordWriter( TaskAttemptContext job )
    throws IOException, InterruptedException  {

    int fapl = -1;
    int facl = -1;
    int error = -1;
    int fileID = -1;
    int dataspaceID = -1;
    int datasetID = -1;

    String extension = ".h5";
    Configuration conf = job.getConfiguration();
    Path path = getDefaultWorkFile(job, extension);
    Path fileToWrite = Utils.convertToMountPath(path, conf);
    //FileSystem cfs = file.getFileSystem(conf);
    FileSystem fs = Utils.getFS(conf);
    LOG.warn("Reducer file path: " + fileToWrite.toString());
    int reducerID = job.getTaskAttemptID().getTaskID().getId();
    int DATATYPE = HDF5Constants.H5T_NATIVE_INT;

    int[] totalOutputShape = Utils.getTotalOutputSpace(conf);
    int[] reducerWriteCorner = HadoopUtils.getReducerWriteCorner(reducerID, conf);
    long[] reducerWriteShape = 
      Utils.convertIntArrayToLongArray(HadoopUtils.getReducerWriteShape(reducerID, conf));
    LOG.info("write corner: " + Arrays.toString(reducerWriteCorner) + " shape: " + 
             Arrays.toString(reducerWriteShape));

    LOG.debug("path: " + fileToWrite.toString());
    LOG.debug("parent: " + fileToWrite.getParent().toString());
    LOG.debug("file: " + fileToWrite.getName().toString());

    boolean retVal = fs.mkdirs(path.getParent(), new FsPermission("777"));
    LOG.info("mkdirs retVal: " + retVal);

    FileStatus[] files = fs.listStatus(path.getParent());

    if (null == files) { 
      LOG.warn("path: " + path.toString() + " is not a dir / returned null");
    } else { 
      LOG.warn("Dir: " + path.getParent().toString() + " has files: " );
      for (FileStatus file : files ) { 
        LOG.warn("\t[ " + file.getPath().toString() + " ]");
      }
    }

    //stat the directory to ensure that it exists
    FileStatus statedDir = fs.getFileStatus( path.getParent());

    if (statedDir.isDir()) { 
      LOG.warn("path: " + statedDir.getPath().toString() + " is a dir"); 
    } else { 
      LOG.warn("path: " + statedDir.getPath().toString() + " is NOT a dir"); 
    }
    LOG.info("in HDF5FileOutputFormat, creating file: " + fileToWrite.toString());

    try{

      // create the HDF5 file
      LOG.info("creating fapl");
      fapl = H5.H5Pcreate(HDF5Constants.H5P_FILE_ACCESS);

      LOG.info("creating facl");
      facl = H5.H5Pcreate(HDF5Constants.H5P_FILE_CREATE);

      LOG.info("creating HDF5 file: " + fileToWrite.toString());
      fileID = H5.H5Fcreate(fileToWrite.toString(), HDF5Constants.H5F_ACC_TRUNC, facl, fapl);

      LOG.info("creating HDF5 dataspace: ");
      dataspaceID = 
        H5.H5Screate_simple(reducerWriteShape.length, reducerWriteShape, reducerWriteShape);

      datasetID = H5.H5Dcreate(fileID, "fake_dataset", DATATYPE, dataspaceID,
                                     HDF5Constants.H5P_DEFAULT,
                                     HDF5Constants.H5P_DEFAULT,
                                     HDF5Constants.H5P_DEFAULT);

      H5.H5Pclose(fapl);
      H5.H5Pclose(facl);

    } catch ( HDF5LibraryException le) { 
      LOG.error(le.toString());
      le.printStackTrace();
      throw new IOException("Caught an HDF5LibraryException in getRecordWriter:" + 
        le.getMessage());
    } catch (HDF5Exception he) { 
      he.printStackTrace();
      throw new IOException("Caught an HDF5Exception in getRecordWriter:" + 
        he.getMessage());
    }

    return new HDF5Writer<K, V>(fileID, dataspaceID, datasetID, job);
  }
}
