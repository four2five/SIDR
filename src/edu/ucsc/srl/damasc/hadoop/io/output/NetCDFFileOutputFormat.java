package edu.ucsc.srl.damasc.hadoop.io.output;

import java.io.IOException;
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

import edu.ucsc.srl.damasc.hadoop.Utils;
import edu.ucsc.srl.damasc.hadoop.io.ArraySpec;
import edu.ucsc.srl.damasc.hadoop.io.NetCDFTools;

import edu.ucsc.srl.damasc.hadoop.HadoopUtils;

public class NetCDFFileOutputFormat<K, V> extends FileOutputFormat<K, V> { 

  private static final Log LOG = 
      LogFactory.getLog(NetCDFFileOutputFormat.class);

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
	  } 
	
	  public synchronized void write( K key, V value, long recordsRepresented) throws IOException { 
      this._writeCorner = ((ArraySpec)key).getCorner();
      this._writeCorner = 
        Utils.subtractArrayFromAnother(this._writeCorner, this._reducerWriteCorner);
	    LOG.info("JB, write at  " + Arrays.toString(this._writeCorner));  
	    this._outBuffer.putInt(0, ((IntWritable)value).get());
	
	    writeNetCDF( this._outBuffer,
	                 this._dataTypeSize,
	                 this._writeCorner,
	                 this._onesArray
	               );
	  }
	
	  // implement this
	  public synchronized void writeNetCDF( ByteBuffer data, int dataTypeSize, 
	                                        int[] corner, int[] shape )  {
      this._writeArray = Array.factory(DataType.INT, shape, data);

      try{ 
        this._ncwFile.write(this._var.getName(), corner, this._writeArray);
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
    FileSystem fs = Utils.getFS(conf);
    int reducerID = job.getTaskAttemptID().getTaskID().getId();
    //LOG.info("task attempt id: " + job.getTaskAttemptID() + " id to use: " + reducerID);

	  int[] totalOutputShape = Utils.getTotalOutputSpace(conf);
    int[] reducerWriteCorner = HadoopUtils.getReducerWriteCorner(reducerID, conf);
    int[] reducerWriteShape = HadoopUtils.getReducerWriteShape(reducerID, conf);
    LOG.info("write corner: " + Arrays.toString(reducerWriteCorner) + " shape: " + 
             Arrays.toString(reducerWriteShape));

    Path fileToRead = Utils.convertToMountPath(path, conf);

    LOG.info("in NetCDFFileOutputFormat, creating file: " + fileToRead);

    LOG.debug("path: " + fileToRead.toString());
    LOG.debug("parent: " + fileToRead.getParent().toString());
    LOG.debug("file: " + fileToRead.getName().toString());

    // create intervening directories
    boolean retVal =  fs.mkdirs(path.getParent(), new FsPermission("777"));
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

    // create the file in the file system. This will create any missing directories
    // in the path
    LOG.warn("creating file: " + fileToRead.toString());
    ncwFile = NetcdfFileWriteable.createNew(fileToRead.toString(), false);

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
