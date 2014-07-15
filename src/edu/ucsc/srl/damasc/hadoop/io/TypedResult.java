package edu.ucsc.srl.damasc.hadoop.io;

/**
 * An interface that specifies a few calls necessary to expose the wrapped datatypes
 * of aggregation classes. 
 * This is neccesary for doing IO correctly.
 */
public interface TypedResult { 
  //public Class getWrappedValueClass();
  public boolean isFull();
}
