package edu.ucsc.srl.damasc.hadoop.io;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import edu.ucsc.srl.damasc.hadoop.HadoopUtils;

import org.apache.hadoop.io.Writable;

/**
 * This class represents the results of an average operation.
 * It tracks the results of average for part of the data and allows
 * for accurrate aggregation of partial results into the final result.
 */
public class AverageResultFloat implements Writable { 
  //private float _bufferedValues; // use this to buffer floats until we need to average them
  private double _bufferedValues; // use this to buffer floats until we need to average them
  private int _bufferedCount; // count of how many values are buffered
  private double _currentValue;
  private int _valuesCombinedCount;

  //private static final Log LOG = LogFactory.getLog(AverageResultFloat.class);

  /**
   * Constructor
   */
  public AverageResultFloat() {
    this.clear();
  }


  /**
   * Create an AverageResultFloat object based on 
   * a single value.  the mapper uses this to do fast creation
   * @param value the single value to use to seed this AverageResultFloat
   * object
   */
  public AverageResultFloat(float value) throws Exception {
    this.clear();
    addValue(value);
  }

  /**
   * Constructor that takes both a current value and a count of the values
   * that were combined to get that value.
   * @param value the current average value of the results aggregated
   * @param valudsCombinedCount the number of values that have been
   * combined so far
   */
  public AverageResultFloat(float value, int valuesCombinedCount) throws Exception {
    this._currentValue = value;
    this._valuesCombinedCount = valuesCombinedCount;
  }

  /**
   * Re-initialize this object for potential reuse
   */
  public void clear() {
    this._currentValue = 0.0;
    this._valuesCombinedCount = 0;
    this._bufferedValues = 0.0;
    this._bufferedCount = 0;
  }

  // This call combines the buffered values into the actual value
  private void foldInBufferedValues() 
  {
    // compute the buffered average, if there are any buffered values
    if (_bufferedCount == 0)
    {
      return;
    }
    else
    {
      float bufferedAverage = (float)(_bufferedValues /  _bufferedCount);
      double ratio = (float)_bufferedCount / (_valuesCombinedCount + _bufferedCount); 

      System.out.println("fold, vcc: " + _valuesCombinedCount + " bc: " + _bufferedCount);
      System.out.println("fold, ba: " + bufferedAverage + " ratio: " + ratio + 
                         " cv: " + _currentValue);
      //  now adjust the current value by the weighted difference
      _currentValue -= (float)((this._currentValue - bufferedAverage) * ratio);
      _valuesCombinedCount += _bufferedCount;

      System.out.println("\tcv: " + _currentValue + " _vcc: " + _valuesCombinedCount);

      // reset the buffered values / count
      _bufferedCount = 0;
      _bufferedValues = 0;
    }

  }

  /**
   * Return the current average based on the values that have been
   * processed so far
   * @return the current average value
   */
  public float getCurrentValue() {
    // remember to fold in the buffered values
    foldInBufferedValues();
    return (float)this._currentValue;
  }

  /**
   * Return the current average value as a float
   * @return the current average value as a float
   */
  public float getCurrentValueFloat() {
    return getCurrentValue();
  }

  /**
   * The number of values that have been added into the running 
   * average so far
   * @return the count of values that have been combined so far
   */
  public int getCurrentCount() {
    return this._valuesCombinedCount;
  }

  /**
   * Adds a value to the running total. This will update both
   * the average value and the count of values that have been
   * processed so far
   * @param value the value to add to the running average
   */
  public void addValue(float value) throws IOException {
    // Need to be careful so that we do not overflow
    double delta = _currentValue - value;
    _valuesCombinedCount++;
    // now weight it by current count + 1
    delta = delta / _valuesCombinedCount;
    _currentValue -= delta;  // subract the weighted delta from the running average

    /*
    // buffer this, if possible
    if (HadoopUtils.safeAdd((float)_bufferedValues, value))
    {
      _bufferedValues += value;
      _bufferedCount++;
    }
    else
    {
      // if the new value won't safely fit, fold in the current buffer 
      // and then buffer it
      foldInBufferedValues();
      _bufferedValues += value;
      _bufferedCount++;
    }
    */

  }

  /**
   * Merge this AverageResultFloat object with another
   * @param result the AverageResultFloat object to merge with this one
   */
  public void addAverageResultFloat( AverageResultFloat result ) { 

    System.out.println("in aresfloat, merging in count: " + result.getCurrentCount() +
                       " to existing count: " + this._valuesCombinedCount);

    // If this object has no values, then use the result object's values
    if (this._valuesCombinedCount == 0)
    {
      this._valuesCombinedCount = result.getCurrentCount();
      this._currentValue = result.getCurrentValueFloat();
    }
    else if (result.getCurrentCount() == 0)
    {
      // no-op since we just use the current values, ignoring the empty result object
    }
    else // actually need to do some math
    {
      // Compute how much the result argument should affect the current value
      float ratio = result.getCurrentCount() / (this._valuesCombinedCount + result.getCurrentCount()); 
      //  now adjust the current value by the weighted difference
      this._currentValue -= (this._currentValue - result.getCurrentValueFloat()) * ratio;	
      this._valuesCombinedCount += result.getCurrentCount();
    }
  }

  /**
   * Set the current value from a float
   * @param value the float value to use to set the current value
   */
  protected void setCurrentValue( float value) {
    this._currentValue = value;
  }

  /**
   * Set the number of values combined to form the current
   * average value  
   * @param newCount the new number of values used to 
   * calculate the current average value
   */
  public void setValuesCombinedCount(int newCount) {
    this._valuesCombinedCount = newCount;
  }

  /**
   * Set both the current average value and the count 
   * of the number of values processed to get to that average.
   * @param value the current average value
   * @param newCount how many values have been processed so far
   */
  public void setValue(float value, int newCount) { 
    this.setCurrentValue(value);
    this.setValuesCombinedCount(newCount);
  }

  /**
   * Write the contents of this AverageResultFloat object out to a String 
   * @return a String representation of this AverageResultFloat object
   */ 
  public String toString() {
    return "value = " + this._currentValue + 
           " count = " + this._valuesCombinedCount;
  }

  /**
   * Serialize this AverageResultFloat object to a DataOutput object
   * @param out the DataOutput object to write the contents of this
   * object to
   */
  @Override
  public void write(DataOutput out) throws IOException {
    // make sure that we fold in the buffered values prior to writing this out
    foldInBufferedValues();
    out.writeDouble(this._currentValue);
    out.writeInt(this._valuesCombinedCount);
  }

  /**
   * Populate this AverageResultFloat object from the data read from 
   * a DataInput object
   * @param in the DataInput object to read data from
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    this.setCurrentValue(in.readDouble());
    this.setValuesCombinedCount(in.readInt());
  }
}
