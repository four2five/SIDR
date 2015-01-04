package edu.ucsc.srl.tools;

import java.util.logging.Level;
import java.util.logging.Logger;

import ucar.ma2.*;

public class RollingAverage
{

  public RollingAverage()
  {
  }

  public float averageFloatValues(ArrayFloat data)
  {
    IndexIterator itr = data.getIndexIterator();
    float tempFloat;
    float delta;
    float rollingAverage = 0.0f;
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

    return rollingAverage;
  }
}
