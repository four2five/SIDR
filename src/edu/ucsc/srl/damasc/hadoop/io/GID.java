package edu.ucsc.srl.damasc.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;

import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public abstract class GID implements WritableComparable<GID> { 

  abstract public void setGroupID( int[] newGroupID );
  abstract public int[] getGroupID();
  abstract public void write(DataOutput out) throws IOException;
  abstract public void readFields(DataInput in) throws IOException;
}
