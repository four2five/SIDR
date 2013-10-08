package edu.ucsc.srl.damasc.hadoop.io;

import java.util.Arrays;
import java.util.ArrayList;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

/**
 * This class represents a generic array. It stores 
 * the data required to open a file and read a contigous 
 * array shape from a variable in said file.
 */
public class CoordVariable implements Writable {
  //private int[] _corner = null;  // anchor point
  //private int[] _shape = null;
  private String _fileName = null;
  private String _varName = null;
  //private int[] _varShape = null; // shape of the entire variable
  //private int[] _logicalStartOffset = null; // used to adjust the coordinates
                                    // of the input (in logical space)
  private BytesWritable _data = null;

  public CoordVariable() {
    _data = new BytesWritable();
  }
  
  
  public CoordVariable(CoordVariable dcvToCopy) throws Exception { 
  }
  
  /**
   * Constructor
   * @param fileName Name of the file to open for reading
   * @param varName Name of the variable to read the array from 
   * @param data The variable's data  
   */  
  public CoordVariable(String fileName, String varName,
                   byte[] data) 
                   throws Exception {
    _fileName = new String(fileName);
    _varName = new String(varName);

    if( null == data) { 
    	this._data = null;
    } else { 
    	this._data = new BytesWritable(data);
    }
  }

  /**
   * Constructor where the variable shape is not known
   * @param varName Name of the variable to read the array from java binary tree
   * @param data The variable's data
   */
  public CoordVariable(String varName, byte[] data) throws Exception {
    this( "", varName, data);
  }

  /**
   * Return the name of the file containing the variable which holds
   * the data represented by this ArraySpec.
   * @return the file name that corresponds to this ArraySpec 
   */
  public String getFileName() {
    return _fileName;
  }

  public void setFileName(String fileName) {
    _fileName = fileName;
  }

  /**
   * Return the name of the variable containing the data represented by 
   * this ArraySpec
   * @return name of the Variable containing this ArraySpec
   */
  public String getVarName() {
    return _varName;
  }

	public void setVarName(String varName) {
		_varName = varName ;
	}
	
  public void setData( byte[] newData) { 
    this._data = new BytesWritable(newData);

    // sanity checking
    if(null == _fileName) { 
    	_fileName = "";
    }
    if(null == _varName) {
    	_varName = "";
    }
  }

  /**
   * Sets the logical offset of the this ArraySpec
   * @param newLogicalStartOffset the offset, in the global logical
   * space, where this ArraySpec resides
   */
  public byte[] getData(){
    return this._data.getBytes();
  }

  /**
   * Write the contents of this ArraySpec out to a string
   * @return a String representation of this object
   */
  public String toString() {
    return _fileName + ":" + _varName + ":" + _data.toString();
  }

  /**
   * Serialize the contents of this ArraySpec to a DataOutput object
   * @param out The DataOutput object to write the contents of this 
   * ArraySpec to
   */
  @Override
  public void write(DataOutput out) throws IOException {

    System.out.println("JB in write, fn: " + _fileName + " vn:" + _varName);
    Text.writeString(out, _fileName);
    Text.writeString(out, _varName);
    _data.write(out);

    //out.writeInt(_data.length);
    //for (int i = 0; i < _data.length; i++) { 
      //out.writeDouble(_data[i]);
    //}
  }

  /**
   * Populate an ArraySpec object by reading data from a 
   * DataInput object
   * @param in The DataInput object to read the data from
   */
  @Override
  public void readFields(DataInput in) throws IOException {
       
    _fileName = Text.readString(in);
    _varName = Text.readString(in);
    System.out.println(" loading data, fn: " + _fileName + " vn: " + _varName);
    _data.readFields(in);
    //int len = in.readInt();
    //_data = new double[len];
    //for (int i = 0; i < _data.length; i++) { 
     // _data[i] = in.readDouble();
    //}
  }
}

