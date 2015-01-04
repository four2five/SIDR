package edu.ucsc.srl.tools;

import java.io.IOException;

import java.util.logging.Level;
import java.util.logging.Logger;

import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import ucar.ma2.*;

/**
 * This class reads the indicated file and will average the first step on the zero-th dimension
 */
public class ReadFile
{
  public ReadFile() 
  {
  }

  
  private static final String variableName = "windspeed2";

  public ArrayFloat readFile(String filePath)
  {
    NetcdfFile ncfile = null;
    log.log(Level.INFO, "Reading file: " + filePath);
    try {
      ncfile = NetcdfFile.open(filePath);
      ArrayFloat data = loadFirstTimeStep( ncfile, variableName);
      return data;
    } catch (IOException ioe) {
      log.log(Level.SEVERE, "trying to open " + filePath + " caught ioe: " + ioe);
    } finally { 
      if (null != ncfile) try {
        ncfile.close();
      } catch (IOException ioe) {
        log.log(Level.SEVERE, "trying to close " + filePath + " caught ioe: " + ioe);
      }
    }

    return null;
  }

  public ArrayFloat loadFirstTimeStep(NetcdfFile ncfile, String variableName)
  {
    // pull out the variable "windspeed2"
    Variable dataVar = ncfile.findVariable(variableName);

    if (dataVar == null) {
      System.out.println("Cant find Variable " + variableName);
      return null;
    }

    // get the dimensions of the given Variable
    int[] shape = dataVar.getShape();

    log.log(Level.INFO, "Variable: " + variableName + " has " + dataVar.getSize() + " elements");

    // set the first dimension to 1
    shape[0] = 1;
  
    // setup the origin variable
    int numDimensions = shape.length;
    int[] origin = new int[numDimensions];
    for (int i=0; i<origin.length; i++)
    {
      origin[i] = 0;
    } 

    ArrayFloat dataArray = null;
    try
    {
      dataArray = (ArrayFloat) dataVar.read(origin, shape);
    }
    catch (IOException|InvalidRangeException ex)
    {
      log.log(Level.SEVERE, "reading array threw an exception: " + ex.toString());
    }

    log.log(Level.INFO, "Read array has " + dataArray.getSize() + " elements");
    return dataArray;
  }

  private static final Logger log = Logger.getLogger(ReadFile.class.getName());
}

