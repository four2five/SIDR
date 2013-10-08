package edu.ucsc.srl.damasc.hadoop.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import edu.ucsc.srl.damasc.hadoop.Utils;

public class DataIterator{ 

  private int[] _extractionShape; // not oriented to the ByteBuffer
  private int[] _globalCorner;  // in the global coordinate space
  private int[] _bufferLogicalShape;   // relative to ByteBuffer

  private int[] _presentGlobalGroup;

  private int[] _nextGroup;   
  private int[] _nextGroupShape;

  private int[] _presentGroup; // relative to the ByteBuffer
  private long _presentGroupOffset; // same as _present group, but flattened
  private int[] _presentGroupShape;

  private int[] _currentRead; // tracks the current read offset within
                               // the current group
  private int[] _currentReadShape; // tracks the current read offset within

  private long _currentReadOffset; 


  private long[] _strides;  
  private int _dataTypeSize;
  private ByteBuffer _bb;
  private boolean _hasMoreGroups;
  private boolean _groupHasMoreElements;
  private int _nDims;
  private int[] _ones;
  private int[] _rowShape;
  private int[] _zeros;
  private long _rowCounter = 0;
  private int[] _retArray;
  private int _currentValueInt;
  private double _currentValueDouble;

  public DataIterator() { 
  }

  public DataIterator(ByteBuffer bb, int[] logicalCorner, int[] logicalShape, 
                          int[] extractionShape, int dataTypeSize  ) 
                          throws IOException { 
    this._bb = bb;
    this._globalCorner = Arrays.copyOf(logicalCorner, logicalCorner.length);
    this._bufferLogicalShape = Arrays.copyOf(logicalShape, logicalShape.length);
    this._extractionShape = Arrays.copyOf(extractionShape, extractionShape.length);
    this._dataTypeSize = dataTypeSize;
    this._hasMoreGroups = true;
    this._nDims = this._bufferLogicalShape.length;

    this._ones = new int[this._nDims];
    this._zeros = new int[this._nDims];
    this._rowShape = new int[this._nDims];

    for( int i=0; i<this._nDims; i++) { 
      this._ones[i] = 1;
      this._zeros[i] = 0;
      this._rowShape[i] = 1;
    }

    this._rowShape[this._nDims - 1] = this._extractionShape[this._nDims - 1];

    // strides in this case are in terms of elements
    this._strides = Utils.computeStrides( this._bufferLogicalShape );

    // adjust strides to be in terms of bytes
    for( int i=0; i<this._nDims; i++) { 
      this._strides[i] *= this._dataTypeSize;
    }


    this._presentGroup = new int[this._nDims];
    this._presentGroupShape = new int[this._nDims];
    this._presentGlobalGroup = new int[this._nDims];
    for( int i=0; i<this._nDims; i++) {
      this._presentGroup[i] = 0;
      this._presentGlobalGroup[i] = 
        (this._globalCorner[i] + this._presentGroup[i]) / this._extractionShape[i];
      this._presentGroupShape[i] = -1;
    }

    this._nextGroup = this._presentGroup.clone();
    this._nextGroupShape = new int[this._nDims];

    // update _currentOffset
    this._presentGroupOffset = Utils.flatten( this._bufferLogicalShape, this._presentGroup);

    // setup the first read shape
    correctShape( this._nextGroupShape, this._nextGroup, this._extractionShape,
                     this._bufferLogicalShape, this._globalCorner);

    this._retArray = new int[this._nDims];
  }

  private void resetReadCounters() { 
    this._currentRead = this._presentGroup.clone();
    this._currentReadShape = this._rowShape.clone();

    // this is in terms of bytes, so multiply by dataTypeSize
    this._currentReadOffset = this._presentGroupOffset * this._dataTypeSize;

    // reset the group's hasMoreElements flag
    this._groupHasMoreElements = true;

    // setup the first row read
    correctShape(this._currentReadShape, this._nextGroup,
                     this._rowShape, this._nextGroupShape,
                     this._globalCorner );
  }


  public void dumpMetadata() { 
    System.out.println("logical corner: " + Arrays.toString(this._globalCorner));
    System.out.println("logical shape: " + Arrays.toString(this._bufferLogicalShape));
    System.out.println("extraction Shape: " + 
      Arrays.toString(this._extractionShape));
    System.out.println("dataTypeSize: " + this._dataTypeSize);
    System.out.println("strides: " + Arrays.toString(this._strides));
  }

  public boolean hasMoreGroups() { 
    return this._hasMoreGroups;
  }

  // counter is the current position, which is to be updated
  // counterBaseline is the starting point of the current instances
  //  relative to the bytebuffer
  // tileShape is the shape being tiled over the logical space
  // totalSpace is the total logical area of the bytebuffer
  // logical corner is the starting point in global logical space
  // read shape is the shape for the next read
  private boolean tile(int[] counter, int[] counterBaseline, 
                       int[] tileShape, int[] totalSpace,
                       int[] logicalCorner, int[] readShape) 
    throws IOException { 

    int nDims = counter.length; 

    // NetCDF and HDF5 are row major, so we increment the highest dimension (a given row)
    // first, then we see if other dimensions require updating
    counter[nDims - 1] += tileShape[nDims - 1];

    // now check if any dimension has filled up. If so, increment the 
    // level above it and zero-out any lower dimensions. Repeat until no
    // dimensions are full

    int i;
    for( i = nDims - 1; i >=0; i-- ) { 
  
      // if the current level is full but it is not level zero,
      // increment the level below it and zero the current level
      // and everything above it
      if( (counter[i])
                  >= 
          (counterBaseline[i] + totalSpace[i]) 
        ){

        if( i > 0) { 

          counter[i-1] += tileShape[i-1];

          // corrent for local coordinates that are not aligned to 
          // global coordinates
          counter[i-1] -= ((logicalCorner[i-1] + counter[i-1]) % tileShape[i-1]);

          // zero current level and those above it
          for( int j=i; j < nDims; j++) { 
            counter[j] = counterBaseline[j];
          }
        } else  { // i == 0
          return false;
        }
      } else {
        break;
      }
    }

    correctShape( readShape, counter, tileShape, totalSpace, logicalCorner);

    return true;
  }

  public void correctShape( int[] readShape, int[] counter, 
                               int[] tileShape, int[] totalSpace,
                               int[] logicalCorner) {
  
    // set the next read shape. We need two checks to see if the tileShape 
    // over extands or if we're starting part way into the tile shape instance
    for(int i=0; i<this._nDims; i++) { 
      readShape[i] = Math.min( tileShape[i], 
                              (totalSpace[i] - counter[i]) );
    }

    for( int i=0; i<this._nDims; i++) { 
      readShape[i] -= ( (logicalCorner[i] + counter[i]) % tileShape[i]);
    }

  }

  public int[] getCurrentGroup() { 
    return globalize(this._presentGroup);
  }

  public int[] getCurrentShape() {
    return this._presentGroupShape;
  }

  public int[] getCurrentLocalGroup() { 
    return this._presentGroup;
  }
  
  public int[] getCurrentReadGroup() { 
    return this._currentRead;
  }

  public int[] getCurrentReadCoordinate() { 
    this._retArray = this._currentRead.clone();
    this._retArray[this._nDims-1] += this._rowCounter;
    return this._retArray;
  }

  public int[] getCurrentReadShape() { 
    return this._rowShape;
  }

  public long getCurrentReadOffset() { 
    return this._currentReadOffset;
  }

  private void updateGlobalGroup() { 
    for( int i=0; i<this._nDims; i++) {
      this._presentGlobalGroup[i] = 
        (this._globalCorner[i] + this._presentGroup[i]) / this._extractionShape[i];
    }
    
  }

  public int[] getNextGroup() throws IOException { 
    // set the return array aside
    this._presentGroup = this._nextGroup.clone();
    this._presentGroupShape = this._nextGroupShape.clone();
    this._presentGroupOffset = Utils.flatten( this._bufferLogicalShape, this._presentGroup);

    updateGlobalGroup();

    resetReadCounters();

    this._hasMoreGroups = tile(this._nextGroup, this._zeros,
                               this._extractionShape, this._bufferLogicalShape,
                               this._globalCorner, this._nextGroupShape);


    return globalize(this._presentGroup);
  }

  public int[] globalize(int[] groupID) { 

    for( int i=0; i<this._nDims; i++) { 
      this._retArray[i] = groupID[i] + this._globalCorner[i];
    }

    return this._retArray;
  }

  public boolean groupHasMoreValues() { 
    return this._groupHasMoreElements;
  }

  public int getNextValueInt() throws IOException  { 
    this._currentValueInt = this._bb.getInt( (int)this._currentReadOffset );
    incrementPos(this._dataTypeSize);

    return this._currentValueInt;
  }

  public double getNextValueDouble() throws IOException  { 
    this._currentValueDouble = this._bb.getDouble( (int)this._currentReadOffset );
    incrementPos(this._dataTypeSize);

    return this._currentValueDouble;
  }

  private void incrementPos(int dataTypeSize) throws IOException { 
    this._rowCounter++;
    this._currentReadOffset += this._dataTypeSize;

    if( this._rowCounter == 
        (this._presentGroup[this._nDims - 1] + this._currentReadShape[this._nDims - 1])){

      this._groupHasMoreElements = tile(this._currentRead, this._presentGroup,
                                        this._rowShape, this._presentGroupShape,
                                        this._globalCorner, this._currentReadShape);
 
      // reset currentReadOffset and rowCounter
      this._rowCounter = 0;
      this._currentReadOffset = Utils.flatten(this._bufferLogicalShape, this._currentRead) *
                                              dataTypeSize;
    }
  }
}
