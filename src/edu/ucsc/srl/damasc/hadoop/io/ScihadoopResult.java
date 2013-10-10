package edu.ucsc.srl.damasc.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * This is the interface for functions in SciHadoop. 
 */
public abstract class ScihadoopResult implements Writable { 
  private int _currentCount;
  private boolean _isFull;

  public ScihadoopResult() { 
    this._currentCount = 0;
    //this._neededCount = 0;
    //this._isFull = false;
  }
  
  abstract boolean isHolistic(); 

  abstract void setNeededCount(int neededCount);

  public int getCurrentCount() { 
    return this._currentCount;
  }

  // This lets the operator know that it should reduce 
  // the data, if possible
  abstract void computeIfPossible();

  abstract void sort();

  // Resets the object for possible reuse
  abstract void clear();

  // a ScihadoopResult should only support one data type.
  // the calls for the other data types should raise exceptions
  abstract void setValue(int value) throws IOException, UnsupportedOperationException;
  abstract void setValue(short value) throws IOException, UnsupportedOperationException;
  abstract void setValues(int values[]) throws IOException, UnsupportedOperationException;
  abstract void setValues(short values[]) throws IOException, UnsupportedOperationException;
  abstract int getValueInt(int index) throws UnsupportedOperationException;
  abstract short getValueShort(int index) throws UnsupportedOperationException;
  abstract int[] getValuesInt() throws UnsupportedOperationException;
  abstract short[] getValuesShort() throws UnsupportedOperationException;

  public <T extends ScihadoopResult> void merge(T in);
}
