package edu.ucsc.srl.damasc.hadoop.io;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileReader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.ucsc.srl.damasc.hadoop.io.NcHdfsRaf;
import edu.ucsc.srl.damasc.hadoop.Utils.FSType;
import edu.ucsc.srl.damasc.hadoop.Utils;

import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import ucar.nc2.Dimension;
import ucar.ma2.Array;
import ucar.ma2.IndexIterator;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;


import ucar.unidata.io.RandomAccessFile;

import java.io.IOException;

/**
 * Utility methods that are specific to NetCDF files / data
 */
public class NetCDFHDFSTools{ 

  public static int getNDims( String cephConfPath, String inputFilePath, 
                              String variableName, Configuration conf) { 

	  int nDims = -1;
	  NetcdfFile file = null; 
    RandomAccessFile raf = null;

    try { 
      System.out.println(" getNDims, path: " + inputFilePath);
  
      raf = getRAF( conf, inputFilePath);
      if( null == raf) { 
        System.out.println("In getNDims. raf is NULL");
        return -1;
      }

	    file = NetcdfFile.open(raf, inputFilePath); 
	
	    List<Variable> vars = file.getVariables();
	    Iterator<Variable> iter = vars.listIterator();
	
	    Variable ourVar = null;
	
	    while( iter.hasNext() ) { 
	      ourVar = iter.next();
	      System.out.println("Comparing " + ourVar.getShortName() + " to " + 
	                         variableName);
	      if( ourVar.getShortName().compareTo(variableName) == 0) { 
	        System.out.println("Found the matching variable");
	        break;
	      } else {
	        ourVar = null;
	      }
	    }
	
	    // if this is true, our variable is not in this file. Bail.
	    if( null == ourVar ) {
	    } else { 
	      nDims = ourVar.getDimensions().size();
	    }
    } catch ( IOException ioe ) { 
      ioe.printStackTrace();
    } finally { 
      if( null != file) { 
        try{ 
          file.close();
          if( null != raf)
            raf.close();
        } catch( IOException ioe) { 
          System.out.println("Caught while closing file: " + inputFilePath);
          ioe.printStackTrace();
        }
      }
    }

    return nDims;
  }

  public static int[] getVariableShape( String inputFilePath, 
                                        String variableName, Configuration conf) { 
   
	  int[] dims = {};
	  NetcdfFile file = null; 
    RandomAccessFile raf = null;

    try { 
      System.out.println(" getVariableShape, path: " + inputFilePath);

      raf = getRAF( conf, inputFilePath);
      if( null == raf) { 
        System.out.println("In getVariableShape . raf is NULL");
        return new int[0];
      }

	    file = NetcdfFile.open(raf, inputFilePath); 
	
	    List<Variable> vars = file.getVariables();
	    Iterator<Variable> iter = vars.listIterator();
	
	    Variable ourVar = null;
	
	    while( iter.hasNext() ) { 
	      ourVar = iter.next();
	      System.out.println("Comparing " + ourVar.getShortName() + " to " + 
	                         variableName);
	      if( ourVar.getShortName().compareTo(variableName) == 0) { 
	        System.out.println("Found the matching variable");
	        break;
	      } else {
	        ourVar = null;
	      }
	    }
	
	
	    // if this is true, our variable is not in this file. Bail.
	    if( null == ourVar ) {
	    } else { 
	      List<Dimension> readDims = ourVar.getDimensions();
        dims = new int[readDims.size()];
        for( int i=0; i<dims.length; i++) { 
          dims[i] = readDims.get(i).getLength();
        }
	    }
    } catch ( IOException ioe ) { 
      ioe.printStackTrace();
    } finally { 
      if( null != file) { 
        try{ 
          file.close();
          if( null != raf)
            raf.close();
        } catch( IOException ioe) { 
          System.out.println("Caught while closing file: " + inputFilePath);
          ioe.printStackTrace();
        }
      }
    }

    System.out.println("dims: " + Arrays.toString(dims));
    return dims;
  }

  public static int getDataTypeSize(String cephConfFile, String inputFilePath,
                                    String variableName, Configuration conf) {

    // hard-coded for now, since we're only working with ints
    return 4;

    /*
    int dataTypeSize = -1;

	  NetcdfFile file = null; 

    NcCephRaf raf = null;
    CephFileSystem cfs = null;

    try { 

	    cfs = new CephFileSystem();
	    cfs.initialize(URI.create("ceph://null/"), conf);
	    int bufferSize = Utils.getBufferSize(conf);

      //String fileToRead = Utils.stripURIInfo(inputFilePath, conf.get("fs.default.name", ""));
      //System.out.println(" getVariableShape, path: " + "ceph://null" + fileToRead);
      System.out.println(" getDataTypeSize, path: " + "ceph://null" + inputFilePath);
      System.out.println("buffersize: " + bufferSize);

	    raf = new NcCephRaf( 
	                          cfs.getFileStatus(
	                            new Path( URI.create("ceph://null/" + inputFilePath))), 
	                            conf, bufferSize);
	    file = NetcdfFile.open(raf, inputFilePath); 
	
	    List<Variable> vars = file.getVariables();
	    Iterator iter = vars.listIterator();
	
	    Variable ourVar = null;
	
	    while( iter.hasNext() ) { 
	      ourVar = (Variable)iter.next();
	      System.out.println("Comparing " + ourVar.getName() + " to " + 
	                         variableName);
	      if( ourVar.getName().compareTo(variableName) == 0) { 
	        System.out.println("Found the matching variable");
	        break;
	      } else {
	        ourVar = null;
	      }
	    }
	
	
	    // if this is true, our variable is not in this file. Bail.
	    if( null == ourVar ) {
	    } else { 
	      //List<Dimension> readDims = ourVar.getDimensions();
        dataTypeSize = ourVar.getDataType().getSize();
	    }
    } catch ( IOException ioe ) { 
      ioe.printStackTrace();
    } finally { 
      if( null != file) { 
        try{ 
          file.close();
          if( raf != null)
            raf.close();
          if( cfs != null)
            cfs.close();

        } catch( IOException ioe) { 
          System.out.println("Caught while closing file: " + inputFilePath);
          ioe.printStackTrace();
        }
      }
    }

    System.out.println("data type size: " + dataTypeSize);
    return dataTypeSize;
  */
  }

  public static RandomAccessFile getRAF( Configuration conf, FileStatus fstat ) 
                                            throws IOException {
    int bufferSize = Utils.getBufferSize(conf);
    return getRAF( conf, fstat, bufferSize);

  }

  public static FileSystem getFS( Configuration conf ) throws IOException {
    FileSystem fs = null;
    fs = new DistributedFileSystem();

    String hdfsNamenodeString = conf.get("fs.default.name");
    fs.initialize(URI.create(hdfsNamenodeString), conf);

    return fs;
  }

  public static RandomAccessFile getRAF( Configuration conf, String fileString)
                                          throws IOException {
    FileStatus fstat = null;

    FileSystem fs = null;
    fs = getFS(conf);

    if( null == fs) {
      System.out.println("\t\tFS is null");
      return null;
    }

    Path tempPath = new Path(fileString);

    fstat = fs.getFileStatus(tempPath);

    URI tempURI = URI.create(fileString);
    tempPath = new Path(tempURI);
    fs = new DistributedFileSystem();

    System.out.println("fs.default.name:" + conf.get("fs.default.name"));
    String hdfsNamenodeString = conf.get("fs.default.name");
    fs.initialize(URI.create(hdfsNamenodeString), conf);

    fstat = fs.getFileStatus(tempPath);
    fs.close();

    if( null == fstat ){
      System.out.println("\t\tfstat is NULL in getRAF(conf, fielstring)");
      return null;
    } else { 
      return getRAF(conf, fstat);
    }
  }

  public static RandomAccessFile getRAF( Configuration conf, FileStatus fstat,
                                         int bufferSize ) throws IOException {
    RandomAccessFile raf = null;
    raf = new NcHdfsRaf(fstat, conf, bufferSize);
    return raf;
  } 

  public static ByteBuffer extractCoordinateVarsToFile(Configuration conf, String filePath, 
                                                             String[] coordVarNames) { 

    // build upthe file name from all the Variable name
    String outFileName = "";
    for (String varName : coordVarNames) { 
      if (outFileName != "") { 
        outFileName += "_" + varName;
      } else { 
        outFileName += varName;
      }
    }

    return extractCoordinateVarsToFile(conf, filePath, coordVarNames, outFileName);
  }

  public static ByteBuffer extractCoordinateVarsToFile(Configuration conf, String filePath, 
                                                String[] coordVarNames, String outFileName) { 
    RandomAccessFile inRAF = null;
    NetcdfFile inNCFile = null;
    RandomAccessFile outRAF = null;
    NetcdfFile outNCFile = null;
    String _curFileName;
    String _curVarName; // name of the current variable that is open
    Variable _curVar; // actual Variable object
    ByteBuffer _value = null;
    Array _data = null;
    try { 
      inRAF = NetCDFHDFSTools.getRAF(conf, filePath);
      inNCFile = NetcdfFile.open(inRAF, filePath);
      List<Variable> vars = inNCFile.getVariables();

      // get a file system
      FileSystem fs = FileSystem.get(conf);
      Path outPath = new Path(outFileName);
      FSDataOutputStream outStream = null;

      if (fs.exists(outPath)) { 
        System.out.println("file: " + outPath.toString() + " already exists. Deleting it");
        fs.delete(outPath, false);
      }

      outStream = fs.create(outPath);

      // Stash the number of variables in total
      outStream.writeInt(coordVarNames.length);

      // now write out each variable
      CoordVariable cv = new CoordVariable();
      cv.setFileName(outPath.toString());

      System.out.println("Writing out coordVarNames");
      for (String coordVar : coordVarNames) { 
        _curVar = inNCFile.findVariable(coordVar);
        if( null == _curVar) { 
          System.out.println("could not find coordinate variable: " + coordVar + 
                             " in file " + filePath);
          outStream.flush();
          outStream.close();
          System.out.println("bailing");
          return null;
        } else { 
          System.out.println("\tFound variable: " + _curVar.getShortName());
        }

        // check that there is only one dimension
        ArrayList<Dimension> varDims = new ArrayList<Dimension>(_curVar.getDimensions());
        if (varDims.size() != 1) { 
          System.out.println("Variable: " + _curVar.getShortName() + 
                             " is not a coordinate variable, has " + 
                             varDims.size() + " dimensions");
          outStream.flush();
          outStream.close();
          System.out.println("bailing");
          return null;
        }

        _data = _curVar.read();
        System.out.println("Read " + _data.getSize() + " elements from Var: " + _curVar.getShortName() + 
                           " that was in file: " + filePath);

        cv.setVarName(coordVar);
        ByteBuffer tempBB = _data.getDataAsByteBuffer();
        System.out.println("JB, capacity is: " + tempBB.capacity());
        byte[] realArray = new byte[tempBB.capacity()];
        tempBB.get(realArray);
        cv.setData(realArray);
        cv.write(outStream);
      }
      outStream.flush();
      outStream.close();
      System.out.println("Flushed / closed: " + outPath.toString());
    } catch (IOException ioe) { 
      System.out.println("caught an ioe:\n" + ioe.toString());
    }

    return _data.getDataAsByteBuffer();
  }

  public static HashMap<String, CoordVariable> loadCoordVarsFromDCache(
                                                                    String fileName, 
                                                                    Configuration conf) { 

    HashMap<String, CoordVariable> retVal = null; 
    System.out.println("In loadCoordVarsFromDCache");
    try { 
      Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
      if (localFiles == null) { 
        System.out.println("There are NO cached files");
        return retVal;
      } else if (localFiles.length == 0) { 
        System.out.println("There are NO cached files");
        return retVal;
      } else { 
        retVal = new HashMap<String, CoordVariable>(localFiles.length);
        for (Path cachedFile : localFiles) { 
          System.out.println("\t Cached file: " + cachedFile.toString());
          if (cachedFile.getName().equals(
              new Path(conf.get(Utils.CACHED_COORD_FILE_NAME)).getName().toString())
          ) { 
            System.out.println("JB, !!! found the cached coord file name");
          } else { 
            System.out.println("JB, no love." + cachedFile.getName().toString() + " *** " + 
                               new Path(conf.get(Utils.CACHED_COORD_FILE_NAME)).getName().toString());
          }
          DataInputStream inStream = new DataInputStream(new FileInputStream(cachedFile.toString()));

          // Pull outthe number of variables in the file
          int varCount = inStream.readInt();

          System.out.println("File: " + cachedFile.toString() + 
                             " contains " + varCount + " variables");

          CoordVariable cv; 
          for (int i=0; i<varCount; i++) { 
            cv = new CoordVariable();
            cv.readFields(inStream);
            retVal.put(cv.getVarName(), cv);
            System.out.println("Read var: " + cv.getVarName());
          }

        }
      } 
    } catch (IOException ioe) { 
      System.out.println("caught an ioe:\n" + ioe.toString());
      ioe.printStackTrace();
    }
    return retVal;
  }
}
