package edu.ucsc.srl.damasc.hadoop.io;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import java.nio.ByteBuffer;
//import java.nio.ByteOrder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.Writable;

/**
 * This class stores the (potentially partial) result
 * of a holistic computation (like median). 
 */
public class HolisticResult2 implements Writable { 
  //private int[] _values = null; // the values needed to apply the holisitic function
  private ByteBuffer _values = null;
  //private int _currentValueCount = 0; // how many values are currently set
  //private boolean _full = false;
  private boolean _final = false;

  /**
   * Empty constructor
   */
  public HolisticResult2() {
    this._values = ByteBuffer.allocate(4);
    //this._values.order(ByteOrder.LITTLE_ENDIAN);
    this._final = false;
  }

  @SuppressWarnings("unused")
private static final Log LOG = LogFactory.getLog(HolisticResult.class);

  /** 
   * Constructor used to do fast creation
   * @param value a single value to integrate into this partial result
   */
  public HolisticResult2(int value ) throws Exception {
    this._values = ByteBuffer.allocate(4);
    //this._values.order(ByteOrder.LITTLE_ENDIAN);
    this._final = false;

    this.setValue(value);
  }

  /** 
   * Constructor
   * @param value a single value to integrate into this partial result
   * @param neededValueCount how many values are needed to generate a 
   * definitive result
   */
  public HolisticResult2(int value, int neededCount) throws Exception {
    this._values = ByteBuffer.allocate(Integer.SIZE * neededCount);
    //this._values.order(ByteOrder.LITTLE_ENDIAN);
    this._final = false;

    this.setValue(value);
  }

  /**
   * Returns the array of values seen for this data set so far
   * @return the array of values seen so far (by this object)
   */
  public int[] getValues() {
    return this._values.asIntBuffer().array();
  }

  public byte[] getBytes() { 
    return this._values.array();
  }

  /**
   * Returns a particular value that has been seen by this result object
   * @param index the array element to return
   * @return the value at the indicated array index location
   */
  public int getValue( int index ) {
    return this._values.getInt(index);
  }

  /**
   * Returns the number of values seen so far by this result object
   * @return number of values integrated into this result object so far
   */
  public int getCurrentCount() {
    return (this._values.capacity() - this._values.remaining()) / 
      Integer.SIZE;
  }

  /**
   * Sorts the values stored in this object by their value
   */
  public void sort() {
    //Arrays.sort(this._values.asIntBuffer());
  }

  /**
   * Resets this object for possible reuse
   */
  public void clear() {
    this._values.clear();
    this._final = false;
  }

  /**
   * Returns how many values are needed to for this result object to calculate
   * its definitive result
   * @return the number of values needed for this object to generate its 
   * definitive result
   */
  public int getNeededCount() {
    return this._values.capacity() / 4;
  }

  /**
   * Final means that this object has received the appropriate number of 
   * results to generate its final answer (and has done so)
   * @return a boolean indicating whether this object has determined its
   * final result. If true, the values array has a single value which is 
   * the result for this data set. 
   */
  public boolean isFinal() {
    return this._final;
  }

  /**
   * Sets the "full" status, which means that the required number of values
   * required to calculate a result have been received by this object
   * @param isFull the value to use for whether this objects value array
   * has a sufficient number of values to calculate a result
   */
   /*
  private void setFull(boolean isFull) {
    this._full = isFull;
  }
  */
  
  /**
   * Sets the "final" boolean, which indicates if this object has calculated
   * its final answer.
   * @param isFinal whether this object has calculated its answer
   */
  private void setFinal( boolean isFinal) {
    this._final = isFinal;
  }

  /**
   * Sets how many values are required for this object to generate its result
   * @param neededValueCount how many values are needed for this object to 
   * have its final result calculated
   */
  public void setNeededCount(int neededCount) {
    ByteBuffer tempBB = 
      ByteBuffer.allocate(4 * neededCount);
    tempBB.put(this._values);

    this._values = tempBB;
  }
  
  /**
   * Returns whether this object has received its required number of 
   * inputs
   * @return whether this object has collected enough values for its result
   * to be calculated
   */
  public boolean isFull() {
    return !(this._values.hasRemaining());
  }

  /**
   * Adds a value to this result object
   * @param value the value to add to this object
   */
  public void setValue(int value) throws IOException {
    if ( this._final == true ) {
      throw new IOException("ERROR: adding a value to a " +
                            "HolisticResult that has been marked final");
    }

    this._values.putInt(value);
  }

  /**
   * This means that the result for this object
   * has been calculated. This generates a new array, holding only the 
   * result, and sets the "final" status to true. 
   * @param value the result for this result object
   */
  public void setFinal( int value ) throws IOException {
    this._values = ByteBuffer.allocate(4);
    this._values.putInt(value);
    this._final = true;
  }
    

  /**
   * Returns the contents of this result object in String form
   * @return the contents of this object as a String
   */
  public String toString() {
    //LOG.info("in toString, this._values count: " + getCurrentCount());
    // this is only pre-allocating 4 chars per int and an apopstraphe in-between. 
    //Is that a sensible default?
    StringBuilder sb = new StringBuilder(getCurrentCount() * 5);

    // do not move the current position
    for( int i=0; i < getCurrentCount(); i++) { 
      if( i > 0){
        sb.append(",");
      }

      sb.append( this._values.getInt(i));
    }
    return sb.toString();
  }

  /**
   * Initializes this object to having a single value in it.
   * This is used to reset a result object
   * @param value the single value to seed this result object with
   */
  public void setHolisticResult( int value ) throws IOException {
    this._values = ByteBuffer.allocate(4);
    this._values.putInt(value);
  }

  /**
   * Initializes a result object with an array of results to 
   * add in to this object. 
   * TODO: optimize this by allocating _values here
   * @param values the array of values to add to this object
   */
  public void setHolisticResult( int[] values ) throws IOException {
        
    for ( int i=0; i<values.length; i++) {
      this._values.putInt(values[i]);
    }
  }

  /**
   * Initializes a result object with a single value and the count
   * of how mamy values are needed to calculate the result
   * @param value the single value to seed this result object with
   * @param neededValueCount how many values are needed to calculate
   * the result
   */
  public void setHolisticResult( int value, int neededCount) 
                                 throws IOException {
    this._values = 
      ByteBuffer.allocate(4* neededCount);
    this._values.putInt(value);

  }

  /**
   * Initializes a result object with another result object
   * @param result the HolisticResult object to use to initialize this
   * object
   */
  public void setHolisticResult( HolisticResult2 result) throws IOException {
    this.setHolisticResult( result.getValues());
  }

  /**
   * Merges a HolisticResult object into this HolisticResult object
   * @param result the HolisiticResult object to merge into this object
   */
  public void merge( HolisticResult2 result ) {
    this._values.put(result.getBytes());
  }

  /**
   * Serialize this object to a DataOutput object
   * @param out the DataOutput object to serialize this object to
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this._values.capacity() / 4);

    out.write(getBytes());
    // these need to be sent last, so that we don't mark
    // something final and then add the one value to it
    out.writeBoolean(this._final);
  }

  /**
   * Populate this object with data from a DataInput object
   * @param in the DataInput object to read data from when populating
   * this object
   */
  @Override
  public void readFields(DataInput in) throws IOException {

    // this may not be necessary
    this.setFinal(false);

    int bbSize = in.readInt();
    this._values = ByteBuffer.allocate(4* bbSize);

    in.readFully(this._values.array());

    this.setFinal(in.readBoolean());
  }

}
