package edu.ucsc.srl.damasc.hadoop.io;

import java.util.Arrays;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.lang.UnsupportedOperationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.Writable;

/**
 * This class stores the (potentially partial) result
 * of a holistic computation (like median). 
 */
public class HolisticResultInt extends ScihadoopResult { 
  private static final Log LOG = LogFactory.getLog(HolisticResultInt.class);

  private int[] _values = null; // the values needed to apply the holisitic function
  private int _currentCount = 0; // how many values are currently set

  /**
   * Empty constructor
   */
  public HolisticResultInt() {
    super();
    this._values = null;
    this._currentCount = 0;
  }

  /** 
   * Constructor used to do fast creation
   * @param value a single value to integrate into this partial result
   */
  public HolisticResultInt(int neededCount) throws Exception {
    this._values = new int[neededCount];
    this._currentCount = 0;
  }

  /** 
   * Constructor
   * @param value a single value to integrate into this partial result
   * @param neededCount how many values are needed to generate a 
   * definitive result
   */
  public HolisticResultInt(int neededCount, int value) throws Exception {
    this._values = new int[neededCount];
    this._currentCount = 0;
    this.setValue(value);
  }

  public HolisticResultInt(HolisticResultInt in) throws Exception {
    super();
    setValues(in.getValuesInt());
  }
  /**
   * Returns the array of values seen for this data set so far
   * @return the array of values seen so far (by this object)
   */
  public int[] getValuesInt() throws UnsupportedOperationException {
    return this._values;
  }

  public short[] getValuesShort() throws UnsupportedOperationException { 
    throw new UnsupportedOperationException("HolisticResultInt does not support shorts");
  }

  /**
   * Returns a particular value that has been seen by this result object
   * @param index the array element to return
   * @return the value at the indicated array index location
   */
  public int getValueInt(int index) throws UnsupportedOperationException  {
    return this._values[index];
  }

  public short getValueShort(int index) throws UnsupportedOperationException { 
    throw new UnsupportedOperationException("HolisticResultInt does not support shorts");
  }

  /**
   * Returns the number of values seen so far by this result object
   * @return number of values integrated into this result object so far
   */
  public int getCurrentCount() {
    return this._currentCount;
  }

  /**
   * Sorts the values stored in this object by their value
   */
  public void sort() {
    Arrays.sort(this._values);
  }

  /**
   * Resets this object for possible reuse
   */
  public void clear() {
    Arrays.fill(this._values, 0);
    this._currentCount = 0;
  }

  /**
   * Returns how many values are needed to for this result object to calculate
   * its definitive result
   * @return the number of values needed for this object to generate its 
   * definitive result
   */
  public int getNeededCount() {
    return this._values.length;
  }

  /**
   * Sets how many values are required for this object to generate its result
   * @param neededCount how many values are needed for this object to 
   * have its final result calculated
   */
  public void setNeededCount(int neededCount) {
    int[] newArray = new int[neededCount];
    try{ 
      for ( int i=0; i<this._currentCount; i++) {
        newArray[i] = this.getValueInt(i);
      }
    } catch (UnsupportedOperationException uoe) { 
      // ???? no idea what to do here. Log it I guess?
      LOG.error("Caught an uoe in HolisticResultInt.setNeededCount()");
    }

    this._values = newArray;
  }
  
  /**
   * Adds a value to this result object
   * @param value the value to add to this object
   */
  public void setValue(int value) throws IOException, UnsupportedOperationException {
    if( getNeededCount() == getCurrentCount()) {
      throw new IOException("ERROR: adding an element to an already " + 
                            "full HolisticResultInt object." +
                            "Length: " + this._values.length);
    }

    //this accounts for previous use cases where this class defaulted
    // to having an array of length 1
    if( null == this._values ) { 
      this._values = new int[0];
    }

    this._values[this._currentCount] = value;
    this._currentCount++;
  }

  public void setValue(short value) throws IOException, UnsupportedOperationException { 
    throw new UnsupportedOperationException("HolisticResultInt does not support shorts");
  }

  /**
   * Returns the contents of this result object in String form
   * @return the contents of this object as a String
   */
  public String toString() {
    return "values = " + Arrays.toString(this._values); 
  }

  /**
   * Initializes a result object with an array of results to 
   * add in to this object. 
   * TODO: optimize this by allocating _values here
   * @param values the array of values to add to this object
   */
   // TODO: trigger a compute() if/when the last value is added
  public void setValues(int[] values) throws IOException, UnsupportedOperationException  {
    for ( int i=0; i<values.length; i++) {
      this.setValue(values[i]);
    }
  }

  public void setValues(short[] values) throws IOException, UnsupportedOperationException  { 
    throw new UnsupportedOperationException("HolisticResultInt does not support shorts");
  }

  public void shrinkValuesArray() { 
    // no sense shrinking if the array is full
    if (getNeededCount() == getCurrentCount()) { 
      return;
    } else {
      this._values = Arrays.copyOfRange(this._values, 0, this._currentCount);
    }
  }

  /**
   * Initializes a result object with another result object
   * @param result the HolisticResultInt object to use to initialize this
   * object
   */
  public void merge(HolisticResultInt result) throws IOException {
    this.setValues(result.getValuesInt());
  }

  /**
   * Serialize this object to a DataOutput object
   * @param out the DataOutput object to serialize this object to
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this._values.length);
    out.writeInt(this._currentCount);

    for( int i=0; i<this._currentCount; i++){
      out.writeInt(this._values[i]);
    }
  }

  /**
   * Populate this object with data from a DataInput object
   * @param in the DataInput object to read data from when populating
   * this object
   */
  @Override
  public void readFields(DataInput in) throws IOException {

    int localArraySize = in.readInt();
    this._values = new int[localArraySize];

    this._currentCount = 0;
    int toRead = in.readInt(); // currentCount from sending side

    for( int i=0; i<toRead; i++) {
      this.setValue(in.readInt());
    }
  }
}
