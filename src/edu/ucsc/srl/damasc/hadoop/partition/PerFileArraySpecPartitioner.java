package edu.ucsc.srl.damasc.hadoop.partition;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Partitioner;

import edu.ucsc.srl.damasc.hadoop.HadoopUtils;
import edu.ucsc.srl.damasc.hadoop.io.ArraySpec;

public class PerFileArraySpecPartitioner<ArraySpec, V> extends Partitioner<ArraySpec,V> 
implements Configurable { 
  private int _retVal;
  Configuration conf;

  public PerFileArraySpecPartitioner() {}

  /*
   * Setup initial configuration, data, etc. 
  */

  @Override
  public void setConf(Configuration conf) { 
    this.conf = conf;
  }

  @Override
  public Configuration getConf() { 
    return this.conf;
  }

  @Override
  public int getPartition(ArraySpec key, V value, int numPartitions) {
    this._retVal = Math.abs(((edu.ucsc.srl.damasc.hadoop.io.ArraySpec)key).getFileName().hashCode()) % numPartitions;
    System.out.println("fn: " + ((edu.ucsc.srl.damasc.hadoop.io.ArraySpec)key).getFileName() + 
                        " hc: " +  Math.abs(((edu.ucsc.srl.damasc.hadoop.io.ArraySpec)key).getFileName().hashCode()));
    
    return this._retVal;
  }
}
