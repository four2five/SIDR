package edu.ucsc.srl.damasc.hadoop.io;

import java.util.Arrays;
import java.util.ArrayList;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

/**
 * This class represents a generic array. It stores 
 * the data required to open a file and read a contigous 
 * array shape from a variable in said file.
 */
public class ArraySpec implements WritableComparable<ArraySpec>, Cloneable {
  private int[] _corner = null;  // anchor point
  private int[] _shape = null;
  private String _varName = null;
  private String _fileName = null;
  private int[] _varShape = null; // shape of the entire variable
  private int[] _logicalStartOffset = null; // used to adjust the coordinates
                                    // of the input (in logical space)

  public ArraySpec() {}
  
  
  public ArraySpec( ArraySpec specToCopy) throws Exception { 
	  this(specToCopy.getCorner(), specToCopy.getShape(),
			  specToCopy.getVarName(), specToCopy.getFileName(),
			  specToCopy.getVariableShape());
  }
  
  /**
   * Constructor
   * @param corner The n-dimensional coordinate for the anchoring
   * corner of the array to be read
   * @param shape The n-dimension shape of the data to be read, 
   * starting at corner
   * @param varName Name of the variable to read the array from 
   * @param fileName Name of the file to open for reading
   * @param variableShape The shape of the variable containing
   * this ArraySpec
   */  
  public ArraySpec(int[] corner, int[] shape, 
                   String varName, String fileName,
                   int[] variableShape) 
                   throws Exception {

	  if ( null == corner) { 
	  	throw new Exception("A null corner[] was passed to ArraySpec constructor");
	  }

	  this._corner = Arrays.copyOf(corner, corner.length);

    if( null == shape) { 
    	this._shape = null;
    } else { 
    	this._shape = Arrays.copyOf(shape, shape.length);
    }

    if( null == variableShape) { 
    	this._varShape = null;
    } else { 
    	this._varShape = Arrays.copyOf(variableShape, variableShape.length);
    }

    _varName = new String(varName);
    _fileName = new String(fileName);
  }

  /**
   * Constructor where the variable shape is not known
   * @param corner The n-dimensional coordinate for the anchoring
   * corner of the array to be read
   * @param shape The n-dimension shape of the data to be read, 
   * starting at corner
   * @param varName Name of the variable to read the array from java binary tree
   * @param fileName Name of the file to open for reading
   */
  public ArraySpec(int[] corner, int[] shape, 
                   String varName, String fileName) throws Exception {
    this( corner, shape, varName, fileName, new int[0]);
  }

  public ArraySpec(int[] corner, String varName) throws Exception {
	this(corner, new int[0], varName, "", new int[0]);
  }

/** 
   * return the number of dimensions for both shape and corner
   * @return the number of dimensions for variable, corner and shape
   * (note: one value is returned, all three must have the same number
   * of dimensions)
   */
  public int getRank() {
    return this._corner.length;
  }

  /**
   * Return the corner that anchors the array represented by this ArraySpec
   * @return an array of integers representing the coordinate of the corner
   * in the respective dimension (array index zero has the coordinate for the 
   * zero-th dimension, etc.)
   */
  public int[] getCorner() {
    return this._corner;
  }

  /**
   * Return the shape to be read from the array represented by this ArraySpec
   * @return an array of integers representing the length of the shape 
   * for the respective dimension (array index zero has the length of the 
   * zero-th dimension, etc.)
   */
  public int[] getShape() {
    return this._shape;
  }

  /**
   * Return the shape of the n-dimensional variable that contains the 
   * array represented by this ArraySpec.
   * @return an n-dimension array of integers storing the length of 
   * the variable in the corresponding array location
   */
  public int[] getVariableShape() {
    return this._varShape;
  }

  /**
   * Get the logical offset for this ArraySpec. This is used to place
   * ArraySpecs in logical spaces spanning multiple files where as 
   * shape and corner and always relative to the specific variable (in the 
   * specific file) being read.
   * @return an n-dimensional array representing the location of this 
   * ArraySpec in the logical space of the currently executing query
   */
  public int[] getLogicalStartOffset() {
    return this._logicalStartOffset;
  }

  /**
   * Return the name of the variable containing the data represented by 
   * this ArraySpec
   * @return name of the Variable containing this ArraySpec
   */
  public String getVarName() {
    return _varName;
  }

  /**
   * Return the name of the file containing the variable which holds
   * the data represented by this ArraySpec.
   * @return the file name that corresponds to this ArraySpec 
   */
  public String getFileName() {
    return _fileName;
  }

  public void setFileName(String newFilename) {
    this._fileName = newFilename;
  }

  /**  
   * Get the number of cells represented by this ArraySpec.
   * @return number of cells represented by this ArraySpec.
   */
  public long getSize() {
    long size = 1;
    for (int i = 0; i < this._shape.length; i++) {
      size *= this._shape[i];
    }

    return size;
  }

  /**
   * Set the shape of the data to be read
   * @param newShape shape of the data to be read
   */
  public void setShape( int[] newShape ) {
    // might want to do some checking of old shape vs new shape later
    this._shape = newShape;
  }

  public void setCorner( int[] newCorner ) { 
    this._corner = Arrays.copyOf(newCorner, newCorner.length);
    if(null == _fileName) { 
    	_fileName = "";
    }
    if(null == _varName) {
    	_varName = "";
    }
  }

  /**
   * Set the shape of the variable that contains the data represented
   * by this ArraySpec.
   * @param newVarShape the Shape of the variable that contains the
   * data for this ArraySpec
   */ 
  public void setVariableShape( int[] newVarShape) {
    this._varShape = Arrays.copyOf(newVarShape, newVarShape.length);
  }

  /**
   * Sets the logical offset of the this ArraySpec
   * @param newLogicalStartOffset the offset, in the global logical
   * space, where this ArraySpec resides
   */
  public void setLogicalStartOffset( int[] newLogicalStartOffset ){
    this._logicalStartOffset = newLogicalStartOffset;
  }

  /**
   * Write the contents of this ArraySpec out to a string
   * @return a String representation of this object
   */
  public String toString() {
    return _fileName + "@" + _varName + "@" + 
            Arrays.toString(_corner) +
           "," + Arrays.toString(_shape); 
  }

  /**
   * Serialize the contents of this ArraySpec to a DataOutput object
   * @param out The DataOutput object to write the contents of this 
   * ArraySpec to
   */
  @Override
  public void write(DataOutput out) throws IOException {

      out.writeInt(_corner.length);
      for (int i = 0; i < _corner.length; i++)
        out.writeInt(_corner[i]);

    out.writeInt(_shape.length);
    for (int i = 0; i < _shape.length; i++)
      out.writeInt(_shape[i]);

      out.writeInt(_varShape.length);
      for (int i = 0; i < _varShape.length; i++)
        out.writeInt(_varShape[i]);

      if ( null == _logicalStartOffset ) {
        out.writeInt(0);
      } else  {
        out.writeInt(_logicalStartOffset.length);
        for (int i= 0; i < _logicalStartOffset.length; i++) {
          out.writeInt(_logicalStartOffset[i]);
        }
      }

    Text.writeString(out, _fileName);
    Text.writeString(out, _varName);
  }

  // this will need a lot of work, just hack it for now
  public boolean abuts(ArraySpec spec) { 
    boolean returnBool = false;
    int i = 0;
    
    int[] specCorner = spec.getCorner();
    int[] thisCorner = getCorner();
    
    int[] thisShape = getShape();
    int[] specShape = spec.getShape();
    
    //sanity check
    if( specCorner.length != thisCorner.length ) { 
      System.out.println("abuts: false. specCorner.length != thisCorner.length");
      return returnBool;
    }

    // Let's roll through the dimensions, highest to lowest, and 
    //see where they stop matching
    for( i=0; i<thisCorner.length; i++) { 
    	if( thisCorner[i] != specCorner[i] ) { 
    		break;
    	}
    }
    
    // setting bool to true for a bit
    returnBool = true;
    
    //now see if the levels below i are all equally full 
    // we can skip the highest dimension, later code will work that out
    for( int j = i + 1; j < thisCorner.length; j++ ) { 
    	if( ((thisShape.length == 0 || thisShape[j] == 1) && (specShape.length == 0 || specShape[j] == 1)) ||
    	     (thisShape.length > 0 && specShape.length > 0 && (thisShape[j] == specShape[j]))
    	  ) { 
    		/* do nothing */
    	}else { 
    		returnBool = false;
    	}
    }
    
    // I think this is safe to do at this point, 
    // as a false should mean that this and spec vary on
    // more than one dimension
    if( false == returnBool ) { 
    	return returnBool;
    }
    
    // now i is the first, non-matching dimension. See if we can fold one
    //into the other
    // NOTE: shape may be a zero-length array, implying all ones
    if( 0 == thisShape.length) { 
    	if( (thisCorner[i] + 1) == specCorner[i]) { 
    		return true;
    	} 
    } else { // non-zero length shape
    	if( (thisCorner[i] + thisShape[i]) == specCorner[i] ) { 
    		return true;
    	} else {
        return false;
      }
    }

    return true;
    
  }

  public void combine( ArraySpec spec ) { 

    int i;
    
    // deal with a non-set size here
    if(0 == this.getShape().length) { 
    	this.setShape(new int[getRank()]);
    	for(i=0; i<this.getRank(); i++) { 
    		this.setShapeDim(i, 1);
    	}
    }
    
    if(0 == spec.getShape().length) { 
    	spec.setShape(new int[getRank()]);
    	for(i=0; i<spec.getRank(); i++) { 
    		spec.setShapeDim(i, 1);
    	}
    }
    
    // first find the initial non-matching dimension
    for( i=0; i<getRank(); i++) { 
    	if( this.getCorner()[i] != spec.getCorner()[i]) { 
    		break;
    	} else { 
    	}
    }
    
    this.setShapeDim(i, this._shape[i] += spec.getShape()[i]);

  }

  /**
   * Populate an ArraySpec object by reading data from a 
   * DataInput object
   * @param in The DataInput object to read the data from
   */
  @Override
  public void readFields(DataInput in) throws IOException {
       
    int len = in.readInt();
    _corner = new int[len];
    for (int i = 0; i < _corner.length; i++)
      _corner[i] = in.readInt();
        
    len = in.readInt();
    _shape = new int[len];
    for (int i = 0; i < _shape.length; i++)
      _shape[i] = in.readInt();

    len = in.readInt();
    _varShape = new int[len];
    for (int i = 0; i < _varShape.length; i++)
      _varShape[i] = in.readInt();

    len = in.readInt();
    if ( 0 == len )  {
      _logicalStartOffset = null;
    } else { 
      _logicalStartOffset = new int[len];
      for (int i = 0; i < _logicalStartOffset.length; i++)
        _logicalStartOffset[i] = in.readInt();
    }

    _fileName = Text.readString(in);
    _varName = Text.readString(in);
  }

  public static void compactList(ArrayList<ArraySpec> arraySpecList) { 
    int lastSize = -1;

    ArraySpec current;
    ArraySpec next;
    while( lastSize != arraySpecList.size() ) { 
      lastSize = arraySpecList.size();
      for( int i=0; i<arraySpecList.size() - 1; i++) { 
        current = arraySpecList.get(i);
        next = arraySpecList.get(i+1); 

        if( current.abuts(next)) { 
          current.combine(next);
          arraySpecList.remove(i+1);
        } else { 
        }
      }
    }
  }

	public void setCornerDim(int curDim, int i) throws ArrayIndexOutOfBoundsException {
		if( curDim >= _corner.length) { 
			throw new ArrayIndexOutOfBoundsException("setDimension called for dimension index " + curDim + 
													" while ArraySpec has length " + _corner.length);
		}
		_corner[curDim] = i;
	}
	
	public void setShapeDim(int curDim, int i) throws ArrayIndexOutOfBoundsException {
		if( curDim >= _shape.length) { 
			throw new ArrayIndexOutOfBoundsException("setDimension called for dimension index " + curDim + 
													" while ArraySpec has shape length " + _corner.length);
		}
		_shape[curDim] = i;
	}

	public void setVariable(String varName) {
		_varName = varName ;
		
	}
	
	public int compareTo(ArraySpec as) { 
		
		if ( 0 != this._fileName.compareTo(as.getFileName())){
			return this._fileName.compareTo(as.getFileName());
		}

		if ( 0 != this._varName.compareTo(as.getVarName())){
			return this._varName.compareTo(as.getVarName());
		}
	      
		// if one is a different length than the other, the shorter one is less
		if( this.getCorner().length < as.getCorner().length) { 
			return -1;
		} else if( this.getCorner().length > as.getCorner().length) { 
			return 1;
		}
		
		// for equal-sized corners, return the first with a lower value
		for( int i=0; i<this.getCorner().length; i++) { 
			if( this.getCorner()[i] < as.getCorner()[i]) { 
				return -1;
			} else if( this.getCorner()[i] > as.getCorner()[i] ){ 
				return 1;
			}
		}
		
		// if all values are equal, this is a match
		return 0;
	}
	
	public boolean equals(Object as) { 
		return (as instanceof ArraySpec && 0 == this.compareTo((ArraySpec)as));
	}
	
	public int hashCode() { 
		int runningHash = 1;
		runningHash *= 31 * Arrays.hashCode(this._corner);
		// since a null shape and all 1's is equal, we need to treat them as such
		if(null == this._shape) { 
			int[] tempArray = new int[this._corner.length];
			Arrays.fill(tempArray, 1);
			runningHash *= 31 * Arrays.hashCode(tempArray);
		} else { 
			runningHash *= 31 * Arrays.hashCode(this._shape);
		}
		
		if( null != this._varName) { 
			//runningHash *= 31 * this._varName.hashCode();
			runningHash *= this._varName.hashCode();
		}
		
		return runningHash;
	}
}



