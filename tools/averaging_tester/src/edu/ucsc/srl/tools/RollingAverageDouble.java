package edu.ucsc.srl.tools;

import java.util.logging.Level;
import java.util.logging.Logger;

import ucar.ma2.*;

public class RollingAverageDouble
{

  public RollingAverageDouble()
  {
  }

  public float averageFloatValues(ArrayFloat data)
  {
    IndexIterator itr = data.getIndexIterator();
    float tempFloat;
    double delta;
    double rollingAverage = 0.0;
    int count = 0;

    while (itr.hasNext())
    {
      count++;
      tempFloat = itr.getFloatNext();
      // see how far the current value is from the rolling average
      delta = rollingAverage - tempFloat;
      // and then scale that by how many values are incorporated into the average at present
      delta /= count;
      // update the rolling average
      rollingAverage -= delta;
    }

    return (float)rollingAverage;
  }
}
