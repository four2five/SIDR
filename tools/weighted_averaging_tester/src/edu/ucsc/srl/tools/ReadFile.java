package edu.ucsc.srl.tools;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import ucar.nc2.Dimension;
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
    _dataVar = null;
  }

  
  public float[] loadWeightsFile(String weightsFile, int numWeights)
  {
    Scanner scanner = null;
    float[] weights = new float[numWeights];

    try {
      scanner = new Scanner(new File(weightsFile));
      int i=0;

      while(scanner.hasNextFloat())
      {
        weights[i++] = scanner.nextFloat();
      }

    } catch (FileNotFoundException fnfe)
    {
      System.out.println("Caught an fnfe: " + fnfe.toString());
    } finally {
      scanner.close();
    }

    if (scanner != null) {
      scanner.close();
    }

    return weights;
  }

  public ArrayFloat readFile(String filePath, String variableName, int timestep)
  {
    // load the NetCDF data
    NetcdfFile ncfile = null;
    log.log(Level.INFO, "Reading file: " + filePath);
    try {
      ncfile = NetcdfFile.open(filePath);
      ArrayFloat data = loadTimeStep(ncfile, variableName, timestep);

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

  public int[] getSingleStepShape() 
  {
    List<Dimension> dimensionsList = this._dataVar.getDimensions();
    int[] dims = new int[dimensionsList.size()];
    for (int i=0; i<dimensionsList.size(); i++) {
      dims[i] = dimensionsList.get(i).getLength();
    }

    // set dimension zero length to 1
    dims[0] = 1;

    return dims;
  }

  public ArrayFloat loadTimeStep(NetcdfFile ncfile, String variableName, int timestep)
  {
    // pull out the variable "windspeed2"
    this._dataVar = ncfile.findVariable(variableName);

    if (this._dataVar == null) {
      System.out.println("Cant find Variable " + variableName);
      return null;
    }

    // get the dimensions of the given Variable
    int[] shape = this._dataVar.getShape();

    log.log(Level.INFO, "Variable: " + variableName + " has " + this._dataVar.getSize() + " elements");

    // set the first dimension to be one so that one full record is read
    shape[0] = 1;
  
    // setup the origin variable
    int numDimensions = shape.length;
    int[] origin = new int[numDimensions];
    for (int i=0; i<origin.length; i++)
    {
      origin[i] = 0;
    } 

    // set the read origin to the passed in timestep
    origin[0] = timestep;

    log.log(Level.INFO, "Reading " + Arrays.toString(origin) + " : " + Arrays.toString(shape));

    ArrayFloat dataArray = null;
    try
    {
      dataArray = (ArrayFloat) this._dataVar.read(origin, shape);
    }
    catch (IOException|InvalidRangeException ex)
    {
      log.log(Level.SEVERE, "reading array threw an exception: " + ex.toString());
    }

    log.log(Level.INFO, "Read array has " + dataArray.getSize() + " elements");
    return dataArray;
  }

  private static final Logger log = Logger.getLogger(ReadFile.class.getName());
  private Variable _dataVar;
}

