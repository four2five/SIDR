package edu.ucsc.srl.damasc.hadoop.partition;

import java.util.Arrays;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Partitioner;

import edu.ucsc.srl.damasc.hadoop.HadoopUtils;
import edu.ucsc.srl.damasc.hadoop.io.ArraySpec;

//import java.util.Random;


public class ArraySpecPartitioner<K extends WritableComparable<?>,V> extends Partitioner<K,V> 
    implements Configurable {

  private long[] _strides;
  private int[] _outputSpace;
  private int _recordDimension;
  private int _recordDimensionDivisor; 
  private int _retVal;
  Configuration conf;

  private int[] _tempArray;
  private int _tempRecordSize;

  public ArraySpecPartitioner() {}

  /*
   * Setup initial configuration, data, etc. 
  */
  public void setConf(Configuration conf) { 
    this.conf = conf;
    this._outputSpace = HadoopUtils.getTotalOutputSpace(conf);
    int numReducers = -1;
    long maxReducerKeyCount = HadoopUtils.getReducerKeyLimit(conf);
    double configuredWeight = HadoopUtils.getReducerShapeWeight(conf);

    try { 
      this._strides = HadoopUtils.computeStrides(this._outputSpace);
    } catch (Exception e) { 
      // no idea what to use for default strides
      System.out.println("in ArraySpecPartitioner.setConf()");
      e.printStackTrace();
    }

    numReducers = HadoopUtils.getNumberReducers(conf);


    // get the weighted record dimension
    this._recordDimension = HadoopUtils.determineRecordDimensionWeighted(this._outputSpace, 
                                    numReducers, configuredWeight, maxReducerKeyCount);


    // the number of steps on the record dimension that will go into a given reducer
    this._recordDimensionDivisor = HadoopUtils.stepSizeForShape( this._recordDimension, 
        numReducers, this._outputSpace, maxReducerKeyCount);

    // multiple that by the stride for the record dimension, so that our divisor
    // is in terms of total cells
    this._recordDimensionDivisor *= this._strides[this._recordDimension];

    System.out.println("recordDim: " + this._recordDimension + 
                       " recordDimDivisor: " + this._recordDimensionDivisor + 
                       " numReducers: " + numReducers + 
                       " outputSpace: " + Arrays.toString(this._outputSpace) + 
                       " weight: " + configuredWeight);

  }

  public Configuration getConf() { 
    return this.conf;
  }

  public int getRecordDimensionDivisor() { 
    return this._recordDimensionDivisor;
  }
  
  public int getPartition(K key, V value, int numPartitions) {

    this._tempArray = ((ArraySpec)key).getCorner();

    this._tempRecordSize = numRecordSteps(this._tempArray, this._outputSpace, 
                                          this._recordDimension);

    this._retVal = this._tempRecordSize / this._recordDimensionDivisor;

    // the final partition may be larger than the normal partitions. Account for that here
    this._retVal = Math.min(this._retVal, numPartitions - 1);

    return this._retVal;
  }

  private int numRecordSteps( int[] current, int[] total, int recordDimension) { 
    this._tempRecordSize = 0;
    for( int i=0; i<=recordDimension; i++) { 
      this._tempRecordSize += (current[i] * this._strides[i]);  
    }

    return this._tempRecordSize;
  }
}
