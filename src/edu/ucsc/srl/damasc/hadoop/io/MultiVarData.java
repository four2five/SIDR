package edu.ucsc.srl.damasc.hadoop.io;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

import edu.ucsc.srl.damasc.hadoop.Utils;

/**
 * This class packages data for a variable along with a coordinate variable
 */
public class MultiVarData implements Writable {
  private HashMap<String, ByteBuffer> _data;

  public MultiVarData () {
    this._data = new HashMap<String, ByteBuffer>();
  }
  
  /**
   * Constructor for one variable
   */  
  public MultiVarData( String varName, ByteBuffer varData)
                   throws Exception {

	  if ( null == varName) { 
		  throw new Exception("A null variable name was passed to MultiVarData constructor");
	  } else if (null == varData) { 
		  throw new IOException("A null byte buffer was passed to MultiVarData constructor");
    }

    this._data = new HashMap<String, ByteBuffer>();
    putVarDataByName(varName, varData);
  }

  /*
   * Retrieve the ByteBuffer for the variable specified.
   */
  public ByteBuffer getVarDataByName( String varName) { 
    if (this._data.containsKey(varName)) { 
      return this._data.get(varName);
    } else { 
      System.out.println("Var: " + varName + " does not exist in this MVD");
      return null;
    }
  }

  /*
   * Add data for a new variable
   */
  public void putVarDataByName( String varName, ByteBuffer inData ) { 
    this._data.put(varName, inData);
  }

  /**
   * Serialize the contents of this ArraySpec to a DataOutput object
   * @param out The DataOutput object to write the contents of this 
   * ArraySpec to
   */
  @Override
  public void write(DataOutput out) throws IOException {

    out.writeInt(this._data.size());
    for (Map.Entry<String, ByteBuffer> entry : this._data.entrySet()) { 
      Text.writeString(out, entry.getKey());
      out.writeInt(entry.getValue().capacity());
      out.write(entry.getValue().array());
    }

  }

  /**
   * Populate an ArraySpec object by reading data from a 
   * DataInput object
   * @param in The DataInput object to read the data from
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    int numVars = in.readInt();
    this._data = new HashMap<String, ByteBuffer>(numVars);
    for (int i=0; i<numVars; i++) { 
      String inVarName = new String(Text.readString(in));
      byte[] inBytes = new byte[in.readInt()];
      in.readFully(inBytes);
      ByteBuffer inBB = ByteBuffer.wrap(inBytes);
      putVarDataByName(inVarName, inBB);
    }
  }
}
