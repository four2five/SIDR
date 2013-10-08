package edu.ucsc.srl.damasc.hadoop.io;

import java.util.Arrays;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import edu.ucsc.srl.damasc.hadoop.io.HadoopArraySpec;
import edu.ucsc.srl.damasc.hadoop.HadoopUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

/**
 * Stores the instance of the extraction shape that corresponds to an
 * array. Serves as a key for the shuffle phase of MapReduce jobs. 
 */
public class HadoopGroupID implements WritableComparable<HadoopGroupID> {
  private int[] _groupID;  // the corner of a given extraction shape 
  private String _name;   // the variable name this data came from

  public HadoopGroupID() {}

  private static final Log LOG = LogFactory.getLog(HadoopGroupID.class);

   
  /**
   * Constructor that sets the HadoopGroupID and the name of the variable
   * the ID corresponds to
   * @param groupID HadoopGroupID, as an n-dimensional variable
   * @param name Variable name that this HadoopGroupID belongs to
   */ 
  public HadoopGroupID(int[] groupID, String name) throws Exception {

    this._groupID = new int[groupID.length];
    for (int i=0; i < groupID.length; i++) {
      this._groupID[i] = groupID[i];
    }

    this._name = new String(name);
  }

  /**
   * Return the number of dimensions for this HadoopGroupID
   * @return the number of dimensions for the variable
   * that this HadoopGroupID belongs to
   */
  public int getRank() {
    return this._groupID.length;
  }

  /**
   * Returns the HadoopGroupID 
   * @return The HadoopGroupID as an n-dimensional array
   */
  public int[] getHadoopGroupID() {
    return this._groupID;
  }

  /**
   * Returns the name of the variable that this HadoopGroupID corresponds to
   * @return variable name as a String
   */
  public String getName() {
    return this._name;
  }

  /**
   * Sets the group ID for this HadoopGroupID object
   * @param newHadoopGroupID the ID for this object
   */
  public void setHadoopGroupID( int[] newHadoopGroupID) {
    this._groupID = newHadoopGroupID;
  }

  /**
   * Makes it possible to set the ID for a specific dimension
   * @param dim The dimension to set
   * @param val The value to set indicated dimension to
   */
  public void setDimension( int dim, int val) 
        throws ArrayIndexOutOfBoundsException {
    if ( dim < 0 || dim >= this._groupID.length)
      throw new ArrayIndexOutOfBoundsException("setDimension called with " +
          "dimension " + dim + " on groupID with dimensions " + 
          this._groupID.length);

    this._groupID[dim] = val;
  }

  /**
   * Sets the variable name for this HadoopGroupID object
   * @param newName the name of the Variable for this object
   */
  public void setName( String newName ) {
    this._name = newName;
  }

  /**
   * Returns the contents of this HadoopGroupID object as a String
   * @return a String version of this HadoopGroupID object
   */
  public String toString() {
    return Arrays.toString(this._groupID) + ":" + this._name;
  }

  /**
   * Projects this HadoopGroupID from the local logical space
   * into the global logical space via the extraction shape
   * @param exShape The extraction shape to use to project this
   * HadoopGroupID into the global space
   * @return the group ID for this object, in the global logical space 
   */
  public String toString(int[] exShape) {
    int[] tempArray = new int[this._groupID.length];
    for ( int i=0; i<tempArray.length; i++) {
      tempArray[i] = this._groupID[i] * exShape[i];
    }

    return Arrays.toString(tempArray) + ":" + this._name;
  }

  /**
   * Compares this HadoopGroupID to another HadoopGroupID
   * @param o a HadoopGroupID object to compare to this object
   * @return an int that is less than, equal to, or greater than zero
   * if the object passed in is less than, equal to, or greater than  
   * this HadoopGroupID, respectively
   */
  public int compareTo(HadoopGroupID other) {
    int retVal = 0;

    retVal = this.getRank() - other.getRank();
    if ( 0 != retVal ) 
      return retVal;

    for ( int i = 0; i < this._groupID.length; i++) {
      retVal = this._groupID[i] - other.getHadoopGroupID()[i];

      if (retVal != 0) {
        return retVal;
      }
    }

    return retVal;
  }

  /**
   * Serialize this object to a DataOutput object
   * @param out the DataOutput object to write the contents 
   * of this HadoopGroupID object to
   */
  @Override
  public void write(DataOutput out) throws IOException {

    out.writeInt(this._groupID.length);
    for (int i = 0; i < this._groupID.length; i++)
      out.writeInt(this._groupID[i]);

    Text.writeString(out, this._name);
  }

  /**
   * Populate this HadoopGroupID object from data read from a 
   * DataInput object
   * @param in the DataInput object to use to populate this
   * object from
   */
  @Override
  public void readFields(DataInput in) throws IOException {
       
    int len = in.readInt();
    this._groupID = new int[len];
    for (int i = 0; i < this._groupID.length; i++)
      this._groupID[i] = in.readInt();
    _name = Text.readString(in);
        
  }

  /**
   * Takes a variable shape and uses that to translate the n-dimensional
   * group ID for this object into a single long value. This is used
   * to create the Key for routing data in Hadoop.
   * @param variableShape the shape to use when flattening the group ID
   * for this object. This should be the shape of the variable the HadoopGroupID 
   * is associated with.
   * @return a long value that represents this HadoopGroupID in a flattened 
   * logical space
   */
  public long flatten( int[] variableShape ) throws IOException {
    long[] strides = HadoopUtils.computeStrides( variableShape);
    long flattenedID = 0;

    for( int i = 0; i < this._groupID.length; i++) {
      flattenedID += ( (this._groupID[i]) * (strides[i]) );
    }
        
    //debug only
    if ( flattenedID < 0){
      LOG.info(" fid: " + flattenedID + " vs: " + 
               Arrays.toString(variableShape) + " str: " + 
               Arrays.toString(strides) +
               " gid: " + Arrays.toString(this._groupID) );
    } 

    return flattenedID; 
  }

  /**
   * Translates a flattened HadoopGroupID into an n-dimensional location
   * @param variableShape the shape of the variable that was used
   * to flatten the HadoopGroupID initially
   * @param flattenedValue the flattened value to turn back into an 
   * n-dimensional shape
   * @return an n-dimensional array that is a group ID
   */
  public int[] unflatten( int[] variableShape, long flattenedValue ) 
                          throws IOException {
    long[] strides = HadoopUtils.computeStrides( variableShape);
    int[] results = new int[variableShape.length];

    for( int i = 0; i < variableShape.length; i++) {
      results[i] = (int) (flattenedValue / strides[i]);
      flattenedValue -= ((results[i]) * strides[i]);
    }
        
    return results;
  }
}
