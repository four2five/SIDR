package edu.ucsc.srl.damasc.hadoop.io;

import java.io.File;
import java.net.URI;

import java.util.List;
import java.util.Iterator;

import org.apache.hadoop.fs.ceph.CephFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;

import edu.ucsc.srl.damasc.hadoop.Utils.FSType;
import edu.ucsc.srl.damasc.hadoop.Utils;

import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import ucar.nc2.Dimension;


//import ucar.unidata.io.RandomAccessFile;

import java.io.IOException;

/**
 * Utility methods that are specific to NetCDF files / data
 */
public class NetCDFTools { 

  public int getNDims( String inputFilePath, 
                       String variableName, Configuration conf) { 

	  int nDims = -1;
	  NetcdfFile file = null; 
    //NcCephRaf raf = null;
    //RandomAccessFile raf = null;
    //CephFileSystem cfs = null;

    try { 
	    //CephFileSystem cfs = new CephFileSystem();
	    //cfs.initialize(URI.create("ceph://null"), conf);
	    //int bufferSize = Utils.getBufferSize(conf);

      //String fileToRead = Utils.stripURIInfo(inputFilePath, conf.get("fs.default.name", ""));
      System.out.println(" getNDims, path: " + inputFilePath);
  
      //Path tempPath = new Path(URI.create("ceph://null" + inputFilePath));
      //FileStatus tempFileStatus = cfs.getFileStatus(tempPath);

      //raf = getRAF( conf, inputFilePath);
	    //NcCephRaf raf = new NcCephRaf( 
	    //                      cfs.getFileStatus(
	    //                        new Path( URI.create("ceph://null/" + inputFilePath))), 
	    //                        conf, bufferSize);
      //NcCephRaf raf = new NcCephRaf( tempFileStatus, conf, bufferSize);
      /*
      if( null == raf) { 
        System.out.println("In getNDims. raf is NULL");
        return -1;
      }
      */

      File filePath = new File(Utils.getCephMountPoint(conf), inputFilePath);
	    file = NetcdfFile.open(filePath.toString()); 
	
	    List<Variable> vars = file.getVariables();
	    Iterator<Variable> iter = vars.listIterator();
	
	    Variable ourVar = null;
	
	    while( iter.hasNext() ) { 
	      ourVar = iter.next();
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
	      nDims = ourVar.getDimensions().size();
	    }
    } catch ( IOException ioe ) { 
      ioe.printStackTrace();
    } finally { 
      if( null != file) { 
        try{ 
          file.close();
          //if( null != raf)
           // raf.close();
          //if(null != cfs)
          //  cfs.close();
        } catch( IOException ioe) { 
          System.out.println("Caught while closing file: " + inputFilePath);
          ioe.printStackTrace();
        }
      }
    }

    return nDims;
  }

  public int[] getVariableShape( String inputFilePath, 
                                 String variableName, Configuration conf) { 
   
	  int[] dims = {};
	  NetcdfFile file = null; 
    //RandomAccessFile raf = null;
    //CephFileSystem cfs = null;

    try { 
      System.out.println(" getVariableShape, path: " + inputFilePath);

      File filePath = new File(Utils.getCephMountPoint(conf), inputFilePath);
	    file = NetcdfFile.open(filePath.toString()); 
	
	    List<Variable> vars = file.getVariables();
	    Iterator<Variable> iter = vars.listIterator();
	
	    Variable ourVar = null;
	
	    while( iter.hasNext() ) { 
	      ourVar = iter.next();
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
          //if( null != raf)
          //  raf.close();
          //if(null != cfs)
           // cfs.close();
        } catch( IOException ioe) { 
          System.out.println("Caught while closing file: " + inputFilePath);
          ioe.printStackTrace();
        }
      }
    }

    System.out.println("dims: " + Utils.arrayToString(dims));
    return dims;
  }

  public static int getDataTypeSize(String inputFilePath, String variableName, Configuration conf) {

    return 4;
    /*
    int dataTypeSize = -1;

    return 4;
    /*
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
}
