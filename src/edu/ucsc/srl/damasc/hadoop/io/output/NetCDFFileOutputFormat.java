package edu.ucsc.srl.damasc.hadoop.io.output;

import java.io.IOException;
import java.io.File;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFileWriteable;
import ucar.nc2.Variable;

import edu.ucsc.srl.damasc.hadoop.io.DoubleTypedResult;
import edu.ucsc.srl.damasc.hadoop.io.FloatTypedResult;
import edu.ucsc.srl.damasc.hadoop.io.IntTypedResult;
import edu.ucsc.srl.damasc.hadoop.io.ShortTypedResult;
import edu.ucsc.srl.damasc.hadoop.io.TypedResult;
import edu.ucsc.srl.damasc.hadoop.io.HolisticResultInt;

import edu.ucsc.srl.damasc.hadoop.io.NetCDFHDFSTools;
import edu.ucsc.srl.damasc.hadoop.Utils;
import edu.ucsc.srl.damasc.hadoop.io.ArraySpec;
//import edu.ucsc.srl.damasc.hadoop.io.NetCDFTools;

import edu.ucsc.srl.damasc.hadoop.HadoopUtils;

public class NetCDFFileOutputFormat<K, V> extends FileOutputFormat<K, V> { 

  private static final Log LOG = 
      LogFactory.getLog(NetCDFFileOutputFormat.class);

  private static final String LOCAL_HDFS_TMP_PATH_PREFIX = "/tmp";

  protected static class NetCDFWriter<K, V> extends RecordWriter<K,V> { 
	  private NetcdfFileWriteable _ncwFile;
	  private Configuration _conf;
	  private ByteBuffer _outBuffer;
	  private int _dataTypeSize;
	  private int[] _onesArray;
	  private int[] _totalOutputShape;
    private int[] _reducerWriteCorner;
    private int[] _reducerWriteShape;
    private int[] _writeCorner;
    private Variable _var;
    private Array _writeArray;
	
	  public NetCDFWriter( NetcdfFileWriteable ncwFile, Variable var, 
                         TaskAttemptContext context) 
	    throws IOException { 
	
	    this._ncwFile = ncwFile;
	    this._conf = context.getConfiguration();
	    this._dataTypeSize = Utils.getDataTypeSize(context.getOutputValueClass()); 
      LOG.info("_outBuffer is " + this._dataTypeSize + " bytes");
	    this._outBuffer = ByteBuffer.allocate(this._dataTypeSize);
	    this._totalOutputShape = Utils.getTotalOutputSpace( this._conf);
      this._reducerWriteCorner = new int[this._totalOutputShape.length];
      this._reducerWriteShape = new int[this._totalOutputShape.length];
      this._var = var;
	    this._onesArray = new int[this._totalOutputShape.length];
	    this._writeCorner = new int[this._totalOutputShape.length];

	
	    for( int i=0; i<this._onesArray.length; i++) { 
	      this._onesArray[i] = 1;
	    }
	
	
	  }

    public void setReducerCorner( int[] corner ) { 
      LOG.info("in setReducercorner, shape: " + Arrays.toString(corner));
      this._reducerWriteCorner = Arrays.copyOf(corner, corner.length);
    }
	
	  public synchronized void close(TaskAttemptContext context) throws IOException { 
	    this._ncwFile.close();

      // now we need to copy the local file into HDFS. 
      // This works since we just doing  'cp' and will not be attempting to 
      // overwrite any bytes

      copyLocalFileToHDFS(this._ncwFile);
	  } 

    private void copyLocalFileToHDFS(NetcdfFileWriteable nfw) { 
      // Sort out the source and destination paths
      String localPath = nfw.getLocation();
      String hdfsPath = localPath.substring(LOCAL_HDFS_TMP_PATH_PREFIX.length());
      LOG.info("local path: " + nfw.getLocation() + " hdfs path: " + hdfsPath.toString());

      try { 
        // do the actual copy
        FileSystem fs = FileSystem.get(this._conf);
        fs.copyFromLocalFile(new Path(localPath), 
                             new Path(hdfsPath));
        LOG.info("Successfully copied local NetCDF file into HDFS");
      } catch (IOException ioe) { 
        LOG.error("Caught an IOException while trying to copy the local NetCDF file into HDFS", ioe);
      }
    }
	
    /**
     * Internal method for writing out DoubleArray s
    */
    private synchronized void writedoubleArray(ByteBuffer bb, DoubleTypedResult value) throws IOException { 
      double[] values = value.getValues();
      //double toPrint = -1;
      LOG.info("in writedoubleArray, values.length: " + values.length);
      if (values.length > 1) 
      { 
        System.err.println("More than one value returned. Not good");
        System.err.println("\tsize is: " + values.length);
        System.err.println("\tisFull: " + value.isFull());
       // toPrint = values[0];
      } else 
      {
        //toPrint = values[0];
      }
      if (values.length > 0) 
      { 
	      _outBuffer.putDouble(0, values[0]);
      } else { 
	      _outBuffer.putDouble(0, (double)-1);
      }
    }

    /**
     * Internal method for writing out DoubleArray s
    */
    private synchronized void writeshortArray(ByteBuffer bb, ShortTypedResult value) throws IOException { 
      short[] values = value.getValues();
      //double toPrint = -1;
      LOG.info("in writeshortArray, values.length: " + values.length);
      if (values.length > 1) 
      { 
        System.err.println("More than one value returned. Not good");
        System.err.println("\tsize is: " + values.length);
        System.err.println("\tisFull: " + value.isFull());
       // toPrint = values[0];
      } else 
      {
        //toPrint = values[0];
      }
      if (values.length > 0) 
      { 
	      _outBuffer.putShort(0, values[0]);
      } else { 
	      _outBuffer.putShort(0, (short)-1);
      }
    }

    /**
     * Internal method for writing out FloatArray s
    */
    private synchronized void writefloatArray(ByteBuffer bb, FloatTypedResult value) throws IOException { 
      float[] values = value.getValues();
      //double toPrint = -1;
      LOG.info("in writefloatArray, values.length: " + values.length);
      if (values.length > 1) 
      { 
        System.err.println("More than one value returned. Not good");
        System.err.println("\tsize is: " + values.length);
        System.err.println("\tisFull: " + value.isFull());
        //System.err.println("\tisFinal: " + value.isFinal());
       // toPrint = values[0];
      } else 
      {
        //toPrint = values[0];
      }
      LOG.info("size of _outBuffer: " + _outBuffer.capacity());
      if (values.length > 0) 
      { 
	      _outBuffer.putFloat(0, values[0]);
      } else { 
        System.err.println("No values in the array. This is bad");
	      _outBuffer.putFloat(0, (float)-1);
      }
    }

    /**
     * Internal method for writing out IntArray s
    */
    private synchronized void writeintArray(ByteBuffer bb, IntTypedResult value) throws IOException { 
      int[] values = value.getValues();
      //double toPrint = -1;
      LOG.info("in writeintArray, values.length: " + values.length);
      if (values.length > 1) 
      { 
        System.err.println("More than one value returned. Not good");
        System.err.println("\tsize is: " + values.length);
        System.err.println("\tisFull: " + value.isFull());
       // toPrint = values[0];
      } else 
      {
        //toPrint = values[0];
      }
      if (values.length > 0) 
      { 
	      _outBuffer.putInt(0, values[0]);
      } else { 
	      _outBuffer.putInt(0, -1);
      }
    }

	  public synchronized void write( K key, V value, long recordsRepresented) throws IOException { 
      this._writeCorner = ((ArraySpec)key).getCorner();
      this._writeCorner = 
        Utils.subtractArrayFromAnother(this._writeCorner, this._reducerWriteCorner);
	    LOG.info("JB, write at  " + Arrays.toString(this._writeCorner));  

      /*
      Class wrappedType = null;
      if (value instanceof TypedResult) { 
        wrappedType = ((TypedResult)value).getWrappedValueClass();
        if (null != wrappedType) { 
          LOG.info("Typed result is: " + wrappedType.toString());
        } else { 
          LOG.info("This is a TypedResult, but getWrappedValueClass() is returning null");
        }
      } else { 
        LOG.info("\tERROR, this value does not implement TypedResult.");
      }
      */

      if (value instanceof DoubleTypedResult) 
      { 
        LOG.info("calling writedoubleArray()");
        writedoubleArray(_outBuffer, (DoubleTypedResult)value);
        _writeArray = Array.factory(DataType.DOUBLE, _onesArray, _outBuffer);
      } else if (value instanceof IntTypedResult) 
      { 
        LOG.info("calling writeintArray()");
        writeintArray(_outBuffer, (IntTypedResult)value);
        _writeArray = Array.factory(DataType.INT, _onesArray, _outBuffer);
      } else if (value instanceof ShortTypedResult) 
      { 
        LOG.info("calling writeshortArray()");
        writeshortArray(_outBuffer, (ShortTypedResult)value);
        _writeArray = Array.factory(DataType.SHORT, _onesArray, _outBuffer);
      } else if (value instanceof FloatTypedResult) 
      { 
        LOG.info("calling writefloatArray()");
        writefloatArray(_outBuffer, (FloatTypedResult)value);
        _writeArray = Array.factory(DataType.FLOAT, _onesArray, _outBuffer);
      } else { 
        LOG.info("not calling writeXXXXXArray()");
      }

      /*
      int[] values = ((HolisticResultInt)value).getValues();
      int toPrint = -1;
      if (values.length > 1) 
      { 
        System.err.println("More than one value returned. Not good");
        System.err.println("\tsize is: " + values.length);
        //System.err.println("\tisFinal: " + ((HolisticResultInt)value).isFinal());
        System.err.println("\tisFull: " + ((HolisticResultInt)value).isFull());
        toPrint = values[0];
      } else 
      {
        toPrint = values[0];
      }
	    this._outBuffer.putInt(0, toPrint);
      */
	
      /*
	    writeNetCDF( this._outBuffer,
	                 this._dataTypeSize,
	                 this._writeCorner,
	                 this._onesArray
	               );
      */

      try{ 
        _ncwFile.write(this._var.getName(), _writeCorner, this._writeArray);
      } catch (IOException ioe) { 
        LOG.info("Caught an ioe in NetCDFFileOutputFormat.writeNetCDF()" + 
                           " for file: " + this._ncwFile.getLocation());
        ioe.printStackTrace();
      } catch ( InvalidRangeException ire ) { 
        LOG.info("Caught an ire in NetCDFFileOutputFormat.writeNetCDF()" + 
                           " for file: " + this._ncwFile.getLocation());
        ire.printStackTrace();
      }
	  }
  }

  public RecordWriter<K, V> getRecordWriter( TaskAttemptContext job) 
    throws IOException, InterruptedException { 

    NetcdfFileWriteable  ncwFile; 
    String extension = ".nc";
    Configuration conf = job.getConfiguration();
    Path path = getDefaultWorkFile(job, extension); 
    LOG.warn("Default work file: " + path.toString());
    //FileSystem fs = Utils.getFS(conf);
    int reducerID = job.getTaskAttemptID().getTaskID().getId();
    //LOG.info("task attempt id: " + job.getTaskAttemptID() + " id to use: " + reducerID);

	  int[] totalOutputShape = Utils.getTotalOutputSpace(conf);
    int[] reducerWriteCorner = HadoopUtils.getReducerWriteCorner(reducerID, conf);
    int[] reducerWriteShape = HadoopUtils.getReducerWriteShape(reducerID, conf);
    LOG.info("write corner: " + Arrays.toString(reducerWriteCorner) + " shape: " + 
             Arrays.toString(reducerWriteShape));

    //Path fileToRead = Utils.convertToMountPath(path, conf);

    LOG.info("in NetCDFFileOutputFormat, final file path: " + path.toString() );
    URI myUri = path.toUri();
    String scheme = myUri.getScheme();
    String part = myUri.getSchemeSpecificPart();
    String fragment = myUri.getFragment();
    String pathFromUri = myUri.getPath();

    LOG.error("scheme: " + scheme + "\npart: " + part + "\nfragment: " + fragment + 
              "\nuriPath: " + pathFromUri);

    //LOG.info("in NetCDFFileOutputFormat, shorter file path: " + Path.getPathWithoutSchemeAndAuthority(path).toString());

    //LOG.debug("path: " + fileToRead.toString());
    //LOG.debug("parent: " + fileToRead.getParent().toString());
    //LOG.debug("file: " + fileToRead.getName().toString());

    // create the path, but in tmp
    //Path localPath = new Path("/tmp", myUri.getPath().toString());

    //boolean retVal =  fs.mkdirs(path.getParent(), new FsPermission("777"));
    //LOG.info("mkdirs retVal: " + retVal);

    //FileStatus[] files = fs.listStatus(path.getParent());

    /*
    File localFile = new File(localPath.toString());

    if (localFile.createNewFile()) { 
      LOG.info("path: " + localFile.getName() + " was created successfully");
    } else { 
      LOG.warn("path: " + localFile.getName() + " was NOT created");
    }
    */ 
    //stat the directory to ensure that it exists
    //FileStatus statedDir = fs.getFileStatus( path.getParent());

    /*
    if (statedDir.isDir()) { 
      LOG.warn("path: " + statedDir.getPath().toString() + " is a dir"); 
    } else { 
      LOG.warn("path: " + statedDir.getPath().toString() + " is NOT a dir"); 
    }
    */

    Path localPath = new Path(LOCAL_HDFS_TMP_PATH_PREFIX + File.separator + myUri.getPath().toString());
    LOG.error("path in tmp: " + localPath);
    System.err.println("path in tmp: " + localPath);
    // create the file in the file system. This will create any missing directories
    // in the path
    File localFile = new File(localPath.toString());
    localFile.getParentFile().mkdirs();
    LOG.warn("creating file: " + localPath.toString());
    ncwFile = NetcdfFileWriteable.createNew(localPath.toString(), false);

    ArrayList<Dimension> dims = new ArrayList<Dimension>();
    for( int i=0; i<reducerWriteShape.length; i++) { 
      dims.add( (ncwFile.addDimension("dim_" + Integer.toString(i), reducerWriteShape[i])));
    }
    Dimension[] arrayOfDims = dims.toArray(new Dimension[dims.size()]);
    Variable var = ncwFile.addVariable("var1", DataType.INT, arrayOfDims);
    ncwFile.create();

    NetCDFWriter<K, V> retWriter = new NetCDFWriter<K, V>(ncwFile, var, job);
    retWriter.setReducerCorner(reducerWriteCorner);

    return retWriter;
  }

}
