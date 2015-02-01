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
public class WeightedAverageResultFloat implements Writable { 
  private double _currentValue;
  private int _valuesCombinedCount;
  private boolean _finalValueComputed;

  //private static final Log LOG = LogFactory.getLog(WeightedAverageResultFloat.class);

  /**
   * Constructor
   */
  public WeightedAverageResultFloat() {
    this.clear();
  }


  /**
   * Create an WeightedAverageResultFloat object based on 
   * a single value.  the mapper uses this to do fast creation
   * @param value the single value to use to seed this WeightedAverageResultFloat
   * object
   */
  public WeightedAverageResultFloat(float value) throws Exception {
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
  public WeightedAverageResultFloat(float value, int valuesCombinedCount) throws Exception {
    this._currentValue = value;
    this._valuesCombinedCount = valuesCombinedCount;
    this._finalValueComputed = false;
  }

  /**
   * Re-initialize this object for potential reuse
   */
  public void clear() {
    this._currentValue = 0.0;
    this._valuesCombinedCount = 0;
    this._finalValueComputed = false;
  }

  /**
   * Return the current average based on the values that have been
   * processed so far
   * @return the current average value
   */
  public float getCurrentValue() {
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
    _currentValue += value;  
    _valuesCombinedCount++;
  }

  /**
   * Merge this WeightedAverageResultFloat object with another
   * @param result the WeightedAverageResultFloat object to merge with this one
   */
  public void addWeightedAverageResultFloat( WeightedAverageResultFloat result ) { 

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
      this._currentValue += result.getCurrentValueFloat();	
      this._valuesCombinedCount += result.getCurrentCount();
    }
  }

  /**
   * Set the current value from a float
   * @param value the float value to use to set the current value
   */
  protected void setCurrentValue( float value ) {
    this._currentValue = value;
  }

  protected void setCurrentRawValue( double value ) {
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

  // this call allows the user to calculate the final result be dividing the current 
  // accumlated value
  public void computeFinalResultViaDivisor(float valueToDivideBy)
  {
    this._currentValue = _currentValue / valueToDivideBy;
    this._finalValueComputed = true;
  }

  /**
   * Write the contents of this WeightedAverageResultFloat object out to a String 
   * @return a String representation of this WeightedAverageResultFloat object
   */ 
  public String toString() {
    return "value = " + this._currentValue + 
           " count = " + this._valuesCombinedCount;
  }

  /**
   * Serialize this WeightedAverageResultFloat object to a DataOutput object
   * @param out the DataOutput object to write the contents of this
   * object to
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeDouble(this._currentValue);
    out.writeInt(this._valuesCombinedCount);
  }

  /**
   * Populate this WeightedAverageResultFloat object from the data read from 
   * a DataInput object
   * @param in the DataInput object to read data from
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    this.setCurrentRawValue(in.readDouble());
    this.setValuesCombinedCount(in.readInt());
  }
}
