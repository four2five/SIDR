package edu.ucsc.srl.damasc.hadoop;

import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import edu.ucsc.srl.damasc.hadoop.io.NcHdfsRaf;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import ucar.unidata.io.RandomAccessFile;
import edu.ucsc.srl.damasc.hadoop.io.NetCDFHDFSTools;
import ucar.nc2.Dimension;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

public class NetCDFUtils
{

  private static final Log LOG = LogFactory.getLog(NetCDFUtils.class);

  public static ByteBuffer get1DVariable(Configuration conf, String filePath) { 
    RandomAccessFile _raf = null;
    NetcdfFile _ncfile = null;
    String _curFileName;
    String _curVarName; // name of the current variable that is open
    Variable _curVar; // actual Variable object
    ByteBuffer _value = null;
    try { 
      _raf = NetCDFHDFSTools.getRAF(conf, filePath);
      _ncfile = NetcdfFile.open(_raf, filePath);
      List<Variable> vars = _ncfile.getVariables();
      if (vars.size() != 0) { 
        System.out.println("File : " + filePath + " does not contain only a coordinate variable");
        return null;
      }

      System.out.println("Var name is: " + vars.get(0).getShortName());
      _curVar = _ncfile.findVariable(vars.get(0).getShortName());

      // check that there is only one dimension
      ArrayList<Dimension> varDims = new ArrayList<Dimension>(_curVar.getDimensions());
      if (varDims.size() != 1) { 
        System.out.println("Variable: " + _curVar.getShortName() + " is not a coordinate variable, has " + 
          varDims.size() + " dimensions");
        return null;
      }

      _value = (_curVar.read()).getDataAsByteBuffer();
      System.out.println("Read " + _value.capacity() + " bytes from Var: " + _curVar.getShortName() + 
        " that was in file: " + filePath);
    } catch (IOException ioe) { 
      System.out.println("caught an ioe:\n" + ioe.toString());
    }

    return _value;
  }
}
