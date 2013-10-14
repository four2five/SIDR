package edu.ucsc.srl.damasc.hadoop.io;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.Writable;

/**
 * This class represents the results of an average operation.
 * It tracks the results of average for part of the data and allows
 * for accurrate aggregation of partial results into the final result.
 */
public class AverageResultShort implements Writable { 
  private float _currentValue;
  private int _valuesCombinedCount;

  //private static final Log LOG = LogFactory.getLog(AverageResultShort.class);

  /**
   * Constructor
   */
  public AverageResultShort() {
    this.clear();
  }


  /**
   * Create an AverageResultShort object based on 
   * a single value.  the mapper uses this to do fast creation
   * @param value the single value to use to seed this AverageResultShort
   * object
   */
  public AverageResultShort(short value ) throws Exception {
    this._currentValue = (float)value;
    this._valuesCombinedCount = 1;
  }

  /**
   * Constructor that takes both a current value and a count of the values
   * that were combined to get that value.
   * @param value the current average value of the results aggregated
   * @param valudsCombinedCount the number of values that have been
   * combined so far
   */
  public AverageResultShort(short value, int valuesCombinedCount) throws Exception {
    this._currentValue = (float)value;
    this._valuesCombinedCount = valuesCombinedCount;
  }

  /**
   * Re-initialize this object for potential reuse
   */
  public void clear() {
    this._currentValue = (float)0;
    this._valuesCombinedCount = 0;
  }

  /**
   * Return the current average based on the values that have been
   * processed so far
   * @return the current average value
   */
  public short getCurrentValue() {
    return (short)this._currentValue;
  }

  /**
   * Return the current average value as a double
   * @return the current average value as a double
   */
  public float getCurrentValueFloat() {
    return this._currentValue;
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
  public void addValue(short value) throws IOException {
    this._currentValue = ((this._currentValue * this._valuesCombinedCount) + value ) / (this._valuesCombinedCount + 1);
    this._valuesCombinedCount++;
  }

  /**
   * Merge this AverageResultShort object with another
   * @param result the AverageResultShort object to merge with this one
   */
  public void addAverageResultShort( AverageResultShort result ) { 
    this._currentValue =  ((this._currentValue * this._valuesCombinedCount) + 
                          (result.getCurrentValueFloat() * result.getCurrentCount())) 
                         / ((float)this._valuesCombinedCount + result.getCurrentCount());
    this._valuesCombinedCount += result.getCurrentCount();

  }

  /**
   * Set the current average value from an int
   * @param value the value to set as the current average value. 
   */
  protected void setCurrentValue( short value ) {
    this._currentValue = (float)value;
  }

  /**
   * Set the current value from a double
   * @param value the double value to use to set the current value
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
   * Write the contents of this AverageResultShort object out to a String 
   * @return a String representation of this AverageResultShort object
   */ 
  public String toString() {
    return "value = " + this._currentValue + 
           " count = " + this._valuesCombinedCount;
  }

  /**
   * Serialize this AverageResultShort object to a DataOutput object
   * @param out the DataOutput object to write the contents of this
   * object to
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeFloat(this._currentValue);
    out.writeShort(this._valuesCombinedCount);
  }

  /**
   * Populate this AverageResultShort object from the data read from 
   * a DataInput object
   * @param in the DataInput object to read data from
   */
  @Override
  public void readFields(DataInput in) throws IOException {

    this.setCurrentValue(in.readFloat());
    this.setValuesCombinedCount(in.readShort());
  }
}
