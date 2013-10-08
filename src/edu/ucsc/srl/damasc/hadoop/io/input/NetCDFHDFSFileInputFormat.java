package edu.ucsc.srl.damasc.hadoop.io.input;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import ucar.nc2.Dimension;
import ucar.ma2.Array;
import ucar.ma2.ArrayLong;
import ucar.ma2.Index;
import ucar.ma2.InvalidRangeException;

import ucar.unidata.io.RandomAccessFile;

import edu.ucsc.srl.damasc.hadoop.io.ArraySpec;
//import edu.ucsc.srl.damasc.hadoop.io.NcHdfsRaf;
//import edu.ucsc.srl.damasc.hadoop.io.NcCephRaf;
import edu.ucsc.srl.damasc.hadoop.io.MultiVarData;
import edu.ucsc.srl.damasc.hadoop.io.NetCDFHDFSTools;
import edu.ucsc.srl.damasc.hadoop.io.SHFileStatus;
import edu.ucsc.srl.damasc.hadoop.Utils;
import edu.ucsc.srl.damasc.hadoop.Utils.FSType;
import edu.ucsc.srl.damasc.hadoop.Utils.PartMode;
import edu.ucsc.srl.damasc.hadoop.Utils.PlacementMode;
import edu.ucsc.srl.damasc.hadoop.Utils.MultiFileMode;


/**
 * FileInputFormat class that represents NetCDF files (specifically NetCDF v3).
 * Extend the existing NetCDFHDFS FileInputFormat as we're only altering the Record Reader.
 */
public class NetCDFHDFSFileInputFormat
    extends NetCDFHDFSCoreFileInputFormat {

  private static final Log LOG = 
      LogFactory.getLog(NetCDFHDFSFileInputFormat.class);

  @Override
  /**
   * Creates a RecordReader for NetCDF files
   * @param split The split that this record will be processing
   * @param context A TaskAttemptContext for the task that will be using
   * the returned RecordReader
   * @return A NetCDFRecordReader 
   */
  public RecordReader<ArraySpec, MultiVarData> createRecordReader( InputSplit split, 
  //public RecordReader<ArraySpec,CoordinateVarData> createRecordReader( InputSplit split, 
                                                                       TaskAttemptContext context )
                                                                       throws IOException { 
    NetCDFHDFSRecordReader reader = 
        new NetCDFHDFSRecordReader();
    reader.initialize( (ArrayBasedFileSplit) split, context);

    return reader;
  }
}
