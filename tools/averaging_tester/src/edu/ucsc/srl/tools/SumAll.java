package edu.ucsc.srl.tools;

import java.util.logging.Level;
import java.util.logging.Logger;

import ucar.ma2.*;

public class SumAll 
{

  public SumAll()
  {
  }

  public float sumFloatValues(ArrayFloat data)
  {
    IndexIterator itr = data.getIndexIterator();
    float tempFloat;
    float accumulator = 0.0f;
    int count = 0;
    while (itr.hasNext())
    {
      tempFloat = itr.getFloatNext();
      accumulator += tempFloat;
      count++;
    }

    float finalResult = accumulator / count;
    return finalResult;
    //log.log(Level.INFO, "average is " + finalResult);
  }

  private static final Logger log = Logger.getLogger(SumAll.class.getName());
}

