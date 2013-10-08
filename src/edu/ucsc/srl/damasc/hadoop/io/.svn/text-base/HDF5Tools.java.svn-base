package edu.ucsc.srl.damasc.hadoop.io;

import java.io.File;

import edu.ucsc.srl.damasc.hadoop.Utils;

import ncsa.hdf.hdf5lib.H5;
import ncsa.hdf.hdf5lib.HDF5Constants;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;

import org.apache.hadoop.conf.Configuration;

public class HDF5Tools { 

  public static int[] getVariableShape(String fileName,
                                      String datasetName, Configuration conf) { 
    int fapl = -1;
    int error = -1;
    int fileID = -1;
    int datasetID = -1;
    int dataspaceID = -1;
    int nDims = -1;
    long[] variableLengths = {}; 
    int[] returnVariableLengths = {}; 
    long[] maxLengths = {};
    File filePath = new File(Utils.getCephMountPoint(conf), fileName);

    try { 
      fapl = H5.H5Pcreate(HDF5Constants.H5P_FILE_ACCESS);
      //error = H5.H5Pset_fapl_ceph(fapl, cephConfFile);
      fileID = H5.H5Fopen(filePath.toString(), HDF5Constants.H5F_ACC_RDONLY, fapl);
      datasetID = H5.H5Dopen(fileID, datasetName, HDF5Constants.H5P_DEFAULT);
      dataspaceID = H5.H5Dget_space(datasetID);

      nDims = H5.H5Sget_simple_extent_ndims(dataspaceID);
      // we need long[] for the read, but then we down cast them to int[]
      // because that's our lowest commen denominator across libraries
      variableLengths = new long[nDims];
      maxLengths = new long[nDims];
      returnVariableLengths = new int[nDims];

      H5.H5Sget_simple_extent_dims(dataspaceID, variableLengths, maxLengths);

      for( int i=0; i<returnVariableLengths.length; i++) { 
        returnVariableLengths[i] = (int)variableLengths[i];
      }

      error = H5.H5Sclose(dataspaceID);
      if (error < 0) { 
        System.out.println("H5Sclose failed in getVariableShape()");
      }

      error = H5.H5Dclose(datasetID);
      if (error < 0) { 
        System.out.println("H5Dclose failed in getVariableShape()");
      }

      error = H5.H5Fclose(fileID);
      if (error < 0) { 
        System.out.println("H5Fclose failed in getVariableShape()");
      }

      error = H5.H5Pclose(fapl);
      if (error < 0) { 
        System.out.println("H5Pclose failed in getVariableShape()");
      }


    } catch ( Exception e ) { 
      e.printStackTrace();
      return new int[0];
    }

    return returnVariableLengths;
  }

  public static int getNDims(String cephConfFile, String fileName,
                                String datasetName) { 
    int fapl = -1;
    int error = 0;
    int fileID = -1;
    int datasetID = -1;
    int dataspaceID = -1;
    int nDims = -1;

    try { 
      fapl = H5.H5Pcreate(HDF5Constants.H5P_FILE_ACCESS);
      //error = H5.H5Pset_fapl_ceph(fapl, cephConfFile);
      fileID = H5.H5Fopen(fileName, HDF5Constants.H5F_ACC_RDONLY, fapl);
      datasetID = H5.H5Dopen(fileID, datasetName, HDF5Constants.H5P_DEFAULT);
      dataspaceID = H5.H5Dget_space(datasetID);

      nDims = H5.H5Sget_simple_extent_ndims(dataspaceID);

      error = H5.H5Sclose(dataspaceID);
      if (error < 0) { 
        System.out.println("H5Sclose failed in getNDims()");
      }

      error = H5.H5Dclose(datasetID);
      if (error < 0) { 
        System.out.println("H5Dclose failed in getNDims()");
      }

      error = H5.H5Fclose(fileID);
      if (error < 0) { 
        System.out.println("H5Fclose failed in getNDims()");
      }

      error = H5.H5Pclose(fapl);
      if (error < 0) { 
        System.out.println("H5Pclose failed in getNDims()");
      }
    } catch ( Exception e ) { 
      e.printStackTrace();
      return -1;
    }

    return nDims;
  }

  public static int getDataTypeSize(String cephConfFile, String fileName,
                                    String datasetName) { 
    int fapl = -1;
    int error = 0;
    int fileID = -1;
    int datasetID = -1;
    int dataspaceID = -1;
    int dataTypeID = -1;
    int dataTypeSize = -1;

    try { 
      fapl = H5.H5Pcreate(HDF5Constants.H5P_FILE_ACCESS);
      //error = H5.H5Pset_fapl_ceph(fapl, cephConfFile);
      fileID = H5.H5Fopen(fileName, HDF5Constants.H5F_ACC_RDONLY, fapl);
      datasetID = H5.H5Dopen(fileID, datasetName, HDF5Constants.H5P_DEFAULT);
      dataspaceID = H5.H5Dget_space(datasetID);
      dataTypeID = H5.H5Dget_type(datasetID);
      dataTypeSize = H5.H5Tget_size(dataTypeID);

      error = H5.H5Tclose(dataTypeID);
      if (error < 0) { 
        System.out.println("H5Tclose failed in getDataTypeSize()");
      }

      error = H5.H5Sclose(dataspaceID);
      if (error < 0) { 
        System.out.println("H5Sclose failed in getDataTypeSize()");
      }

      error = H5.H5Dclose(datasetID);
      if (error < 0) { 
        System.out.println("H5Dclose failed in getDataTypeSize()");
      }

      error = H5.H5Fclose(fileID);
      if (error < 0) { 
        System.out.println("H5Fclose failed in getDataTypeSize()");
      }

      error = H5.H5Pclose(fapl);
      if (error < 0) { 
        System.out.println("H5Pclose failed in getDataTypeSize()");
      }

    } catch ( Exception e ) { 
      e.printStackTrace();
      return -1;
    }

    return dataTypeSize;
  }

}
