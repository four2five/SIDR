package edu.ucsc.srl.tools;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import ucar.ma2.*;
import ucar.nc2.Dimension;

import java.io.IOException;

import edu.ucsc.srl.damasc.hadoop.io.DataIterator;

public class WeightedSumAllDouble
{

  public WeightedSumAllDouble()
  {
  }

  public float weightedSumFloatValuesBB(ByteBuffer data, int[] corner, int[] shape, 
                                        int[] exShape, int dataTypeSize, float[] weights)
	{
    int numGroups = 0;
    double accumulator = 0.0;
    int count = 0;

    try
    {
      DataIterator dataIter = new DataIterator(data, corner, shape,
                                            exShape, dataTypeSize);

      int[] tempGroup;
      float tempFloat;

      while( dataIter.hasMoreGroups() ) {
        numGroups++;
        tempGroup = dataIter.getNextGroup();
        System.out.println("Processing group: " + Arrays.toString(tempGroup));

        // reset per-group accumulators
        while( dataIter.groupHasMoreValues() ) { 
          //System.out.println("\telement: " + Arrays.toString(dataIter.getCurrentReadCoordinate()) + " val: " + dataIter.getNextValueFloat());
          //System.out.println("\telement: " + Arrays.toString(dataIter.getCurrentReadCoordinate()) +
          //                      " weight " + weights[dataIter.getCurrentReadCoordinate()[1]]);
          count++;
          accumulator += dataIter.getNextValueFloat() * weights[dataIter.getCurrentReadCoordinate()[1]];
        }

      }
    } catch (IOException ioe)
    {
      System.out.println("caught an ioe: " + ioe.toString());
    }


    assert(numGroups == 1);

    //float finalResult = (float) (accumulator / count);
    System.out.println("accum: " + accumulator + "nlon: " + shape[2]);
    float finalResult = (float) (accumulator / (2.0f * shape[2]));
    return finalResult;
  }

  public float weightedSumFloatValues(ArrayFloat data)
  {
    IndexIterator itr = data.getIndexIterator();
    float tempFloat;
    double accumulator = 0.0;
    int count = 0;
    while (itr.hasNext())
    {
      tempFloat = itr.getFloatNext();
      accumulator += tempFloat;
      count++;
    }

    float finalResult = (float) (accumulator / count);
    return finalResult;
  }

  private static final Logger log = Logger.getLogger(WeightedSumAllDouble.class.getName());
}

