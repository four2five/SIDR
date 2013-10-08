package edu.ucsc.srl.damasc.hadoop;

import ncsa.hdf.hdf5lib.HDF5Constants;
import ncsa.hdf.hdf5lib.H5;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

public class HDF5Utils
{

  private static final Log LOG = LogFactory.getLog(HDF5Utils.class);


  public static Class convertHDF5DataTypeToJavaClass( int hdf5DataType ) {

    Class retClass = null;
          
    try { 
	    // 1 byte
	    if( H5.H5Tequal(hdf5DataType, HDF5Constants.H5T_NATIVE_CHAR) ||
	        H5.H5Tequal(hdf5DataType, HDF5Constants.H5T_NATIVE_SCHAR) ||
	        H5.H5Tequal(hdf5DataType, HDF5Constants.H5T_NATIVE_UCHAR) ||
	        H5.H5Tequal(hdf5DataType, HDF5Constants.H5T_STD_I8BE) ||
	        H5.H5Tequal(hdf5DataType, HDF5Constants.H5T_STD_I8LE) ||
	        H5.H5Tequal(hdf5DataType, HDF5Constants.H5T_STD_U8BE) ||
	        H5.H5Tequal(hdf5DataType, HDF5Constants.H5T_STD_U8LE) ||
	        H5.H5Tequal(hdf5DataType, HDF5Constants.H5T_STD_B8BE) ||
	        H5.H5Tequal(hdf5DataType, HDF5Constants.H5T_STD_B8LE) 
	      )
	      retClass = byte.class;
	
	    // 2 byte
	    if( H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_NATIVE_SHORT) ||
	        H5.H5Tequal(hdf5DataType, HDF5Constants.H5T_NATIVE_USHORT) ||
	        H5.H5Tequal(hdf5DataType, HDF5Constants.H5T_STD_I16BE) ||
	        H5.H5Tequal(hdf5DataType, HDF5Constants.H5T_STD_I16LE) ||
	        H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_STD_U16BE) ||
	        H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_STD_U16LE) ||
	        H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_STD_B16BE) ||
	        H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_STD_B16LE) 
	      )
	      retClass = short.class;
	
	    // 4 byte float
	    if( H5.H5Tequal(hdf5DataType, HDF5Constants.H5T_NATIVE_FLOAT) ||
          H5.H5Tequal(hdf5DataType, HDF5Constants.H5T_IEEE_F32BE) ||
	        H5.H5Tequal(hdf5DataType, HDF5Constants.H5T_IEEE_F32LE) 
        )
	      retClass = float.class;
	
	    // 4 bytes
	    if( H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_NATIVE_INT) ||
	        H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_STD_I32BE) ||
	        H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_STD_I32LE) ||
	        H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_STD_U32BE) ||
	        H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_STD_U32LE) ||
	        H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_STD_B32BE) ||
	        H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_STD_B32LE) ||
	        H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_UNIX_D32BE) ||
	        H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_UNIX_D32LE) 
	      )
	      retClass = int.class;
	
	    // 8 bytes
	    if( H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_NATIVE_LONG) ||
	        H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_NATIVE_ULONG) ||
	        H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_NATIVE_LLONG) ||
	        H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_NATIVE_ULLONG) ||
	        H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_STD_I64BE) ||
	        H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_STD_I64LE) ||
	        H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_STD_U64BE) ||
	        H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_STD_U64LE) ||
	        H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_STD_B64BE) ||
	        H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_STD_B64LE) ||
	        H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_UNIX_D64BE) ||
	        H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_UNIX_D64LE) 
	      )
	      retClass = long.class;
	
	    // 8 bytes floating point 
	    if(  H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_NATIVE_DOUBLE) ||
	         H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_IEEE_F64BE) || 
	         H5.H5Tequal(hdf5DataType,  HDF5Constants.H5T_IEEE_F64LE)
	
	      ) 
	      retClass = double.class;

      if ( H5.H5Tequal(hdf5DataType, HDF5Constants.H5T_STD_U64LE) )
        retClass = long.class;

    } catch ( HDF5LibraryException le) { 
      le.printStackTrace();
    }

    if( null == retClass ){ 
        LOG.warn("Unknown HDF5 data type " + hdf5DataType);
    }


    return retClass;
  }

  /*
   * do something more intelligent here at a later date
   */
}
