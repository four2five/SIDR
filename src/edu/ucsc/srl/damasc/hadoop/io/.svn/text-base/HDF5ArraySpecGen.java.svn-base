package edu.ucsc.srl.damasc.hadoop.io;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.lang.reflect.Array;

/*
import ucar.ma2.Array;
import ucar.ma2.ArrayInt;
import ucar.ma2.Index;
import ucar.ma2.IndexIterator;
import ucar.ma2.InvalidRangeException;
*/

import edu.ucsc.srl.damasc.hadoop.io.ArraySpec;
import edu.ucsc.srl.damasc.hadoop.Utils;

/**
 * This class generates ArraySpecs by iterating the extraction shape
 * over the specified logical space of input
 */
public class HDF5ArraySpecGen { 

    public static boolean debug = false;
    private static final Log LOG = LogFactory.getLog(HDF5ArraySpecGen.class);


    public HDF5ArraySpecGen() { 
    }

    /**
     * Translate an n-dimensional coordinate from its local space
     * to the global space.
     * @param currentCounter the current global coordinate(s)
     * @param corner the current local coordinate
     * @param globalCoordinate object to hold the return value
     * @return the global coordinate for this local coordinate
     */
    /*
    private int[] mapToGlobal( int[] currentCounter, int[] corner, 
                               int[] globalCoordinate) {
        for ( int i=0; i < currentCounter.length; i++) {
            globalCoordinate[i] = currentCounter[i] + corner[i];
        }

        return globalCoordinate;
    }
    */

    /**
     * Map from a global coordinate to a local coordinate. Local means relative to a given instance
     * of the extraction shape
     * @param globalCoord an absolute global coordinate
     * @param arraySpecArray an array representing the local coordinate
     * @param outGroupID the return object, a GroupID object
     * @param extractionShape the extraction shape being used by the currently running MR program
     * @param an ArraySpec object representing the local coordinates resulting from dividing the 
     * global ID by the extraction shape
     */
    private ArraySpec normalizeGroupID( int[] globalCoord, int[] arraySpecArray, 
                                      ArraySpec outputAS, int[] extractionShape ) {

        // short circuit out in case extraction shape is not set
        if ( extractionShape.length == 0 ) {
        	outputAS.setCorner(arraySpecArray);
            return outputAS;
        }

        for ( int i=0; i < arraySpecArray.length; i++ ) {
        	arraySpecArray[i] = globalCoord[i] / extractionShape[i];
        }

        outputAS.setCorner(arraySpecArray);
        return outputAS;
    }

    /**
     * This is called recursively to iterate the extraction shape over the logical space.
     * The result is a set of GroupID objects that cover the logical input space
     * note, in a 4-d array, the fastest changing dimension is the highest (len - 1)
     * NOTE: this will produce GroupIDs that bound the corner & shape (and exceed them)
     * @param groupIDs an ArrayList of GroupID objects, this stores the produced GroupIDs
     * @param corner logical corner for the space that GroupIDs are being generated for
     * @param shape shape of the logical space that GroupIDs are being generated for
     * @param exShape extraction shape used to generate the individual groups (GroupIDs)
     * @param curGroup current GroupID being calculated
     * @param curDim current dimension that is being incremented as GroupIDs are being
     * generated
     * @param levelFull an array that tracks which levels of the logical space have been
     * exhausted in terms of having GroupIDs covering them completely
     */
    public void recursiveGetBounding(ArrayList<ArraySpec> arraySpecs, int[] corner,  int[] shape,
                                     int[] exShape, ArraySpec curAS, 
                                     int curDim, boolean[] levelFull) {
      try {
        LOG.info("recur, dim: " + curDim + " curGroup: " + curAS );

        while ( levelFull[0] == false ) { // while not done
          // have we hit the high dimension yet ?
          if ( curDim < corner.length - 1 ) {
            if (levelFull[curDim + 1] == false ) { 
              // keep going down as far as you can
              recursiveGetBounding(arraySpecs, corner, shape, exShape, curAS,
                                    (curDim + 1), levelFull);
              //return;
            } else { // lower level(s) are full, try incrementing this level
              if ( (curAS.getCorner()[curDim] + 1) * exShape[curDim] <
                    corner[curDim] + shape[curDim] ) { 
                // increment this level and then blank the lower levels (both full and curGroup)
            	  curAS.setCornerDim(curDim, (curAS.getCorner()[curDim]) + 1);

                // blank level full curGroup values for higher dimensions
                for ( int i=curDim + 1; i < corner.length; i++) {
                  levelFull[i] = false;
                  curAS.setCornerDim(i, 0);
                }

                //return; // this will drop down a level next time

              } else { // this means this level is full
                levelFull[curDim] = true;
                return;
              }
            }
          } else { // hit top (fasted varying) (curDim == corner.length)
            while ( curAS.getCorner()[curDim] * exShape[curDim] < 
                    corner[curDim] + shape[curDim] ) {
              // NOTE: we only add groupIDs at the zero level
              /*
              System.out.println("curDim: " + curDim);
              System.out.println( (curGroup.getGroupID()[curDim] * exShape[curDim])  +" < " + 
                                  (corner[curDim] + shape[curDim]) );
              */
              LOG.info("\taddingGroup(): " + curAS);
              arraySpecs.add( new ArraySpec(curAS.getCorner(), ""));
              curAS.setCornerDim(curDim, (curAS.getCorner()[curDim]) + 1);
              /*
              System.out.println( (curGroup.getGroupID()[curDim] * exShape[curDim])  +" < " + 
                                  (corner[curDim] + shape[curDim]) );
              */
            }
            LOG.info("Setting level " + curDim + " to full");
            levelFull[curDim] = true;
            return;
          }

        } // levelFull[levelFull.length - 1] == false

      } catch ( Exception e ) { 
        System.out.println("caught e in IdentityMapper.recursiveGetBounding. \n" + e.toString() );
        System.out.println("curDim: " + curDim + " groupID: " + curAS.toString());
      }
    }

    /**
     * Helper function for splitting out the components of ArraySpec
     * @param key ArraySpec object representing the logical space to generate GroupIDs for
     * @param extractionShape the shape that dictates how the logical space is decomposed
     * @return an array containing the generated GroupIDs
     */
    public ArraySpec[] getBoundingArraySpecs( ArraySpec key, int[] extractionShape) {
        return getBoundingArraySpecs(key.getCorner(), key.getShape(), extractionShape );
    }

    /**
     * Takes a corner, shape and the extraction shape to tile over it and returns an array
     * of GroupID objects that completely covers the logical space defined by 
     * corner and shape
     * @param corner the corner of the logical space to create GroupIDs for
     * @param shape the shape of the logical space to generate GroupIDs for
     * @param extractionShape the shape to be tiled over the logical space when generating 
     * GroupID objects
     * @return an array of GroupID objects the completely covers the logical space defined by
     * the function inputs
     */
    public ArraySpec[] getBoundingArraySpecs( int[] corner, int[]  shape, 
                                          int[] extractionShape) {
        // we'll need to do this recursively as this will need to go down n-dimensions deep

        ArrayList<ArraySpec> groupIDs = new ArrayList<ArraySpec>();
        ArraySpec[] returnArray = new ArraySpec[0];

        // seed the recursive method with the groupID of the first point in the corner
        int[] groupIDArray = new int[extractionShape.length];

        // track which levels are full
        boolean[] levelFull = new boolean[extractionShape.length];

        for( int i=0; i<groupIDArray.length; i++){
            groupIDArray[i] = 0;
            levelFull[i] = false;
        }

        try { 
        	ArraySpec myGroupID = new ArraySpec(groupIDArray, "");
            // groupIDArray is just a place holder variable for use in the function
            myGroupID = normalizeGroupID(corner, groupIDArray, myGroupID, extractionShape); 

            recursiveGetBounding(groupIDs, corner, shape, extractionShape, 
                                 myGroupID, 0, levelFull); 

            // return the array
            returnArray = new ArraySpec[groupIDs.size()];
            returnArray = groupIDs.toArray(returnArray);
        } catch ( Exception e ) {
            System.out.println("Caught exception in IdentityMapper.getBoundingGroupIDs() \n" + 
                               e.toString() );
        }

        return returnArray;
    }

    /**
     * Helper function that populates an array, of the size specified 
     * by variableShape. Typically used for generating data for testing.
     * @param arrayShape shape of the data to be generated
     * @return an int[] object full of data
     */
    @SuppressWarnings("unused")
	private static int[] populateArray( int[] arrayShape ) {
        int[] tempArray = new int[arrayShape.length];
        for(int i=0; i<arrayShape.length; i++){
          tempArray[i] = arrayShape[i];
        }

        Object returnArray = Array.newInstance(int.class, tempArray);

        int totalElementCount = Utils.calcTotalSize(tempArray);

        int counter = 0;

        for( int i=0; i<totalElementCount; i++, counter++) { 
          Array.set(returnArray, i, counter);
        }

        return (int[])returnArray;
    }

    /**
     * This method creates an Array spec that is the intersection of 
     * the current GroupID (normalized into global space) and the actual
     * array being tiled over. The returned value will be smaller than or equal
     * to the extraction shape for any given dimension.
     * @param spec The corner / shape of the data we want
     * @param gid The GroupID for the current tiling of the extraction shape
     * @param extractionShape the extraction shape
     * @param ones an array of all ones (needed by calls in this function)
     * @return returns an ArraySpec object representing the data 
    */
    private static ArraySpec getGIDArraySpec( ArraySpec spec, 
                                              ArraySpec as, int[] extractionShape, 
                                              int[] ones) {

        int[] normalizedGID = new int[extractionShape.length];
        int[] arraySpecCorner = as.getCorner();

        // project the groupID into the global space, via extractionShape, 
        // and then add the startOffset
        for( int i=0; i < normalizedGID.length; i++) {
            normalizedGID[i] = (arraySpecCorner[i] * extractionShape[i]); 
        }
        //System.out.println("normalized: " + Utils.arrayToString(normalizedGID));
        
        // we're going to adjust normaliedGID to be the corner of the subArray
        // need a new int[] for the shape of the subArray
        
        int[] newShape = new int[normalizedGID.length];

        // roll through the various dimensions, creating the correct corner / shape
        // pair for this ArraySpec (as)
        for( int i=0; i < normalizedGID.length; i++) {
            // this as starts prior to the data for this dimension
            newShape[i] = extractionShape[i];
            //newShape[i] =e 1;

            // in this dimension, if spec.getCorner is > normalizedGID,
            // then we need to shrink the shape accordingly.
            // Also, move the normalizedGID to match
            if ( normalizedGID[i] < spec.getCorner()[i]) { 
                newShape[i] = extractionShape[i] - (spec.getCorner()[i] - normalizedGID[i]);
                normalizedGID[i] = spec.getCorner()[i];
            // now, if the shape extends past the spec, shorten it again
            } else if ((normalizedGID[i] + extractionShape[i]) > 
                (spec.getCorner()[i] + spec.getShape()[i]) ){
              newShape[i] = newShape[i] - 
                            ((normalizedGID[i] + extractionShape[i]) - 
                            (spec.getCorner()[i] + spec.getShape()[i]));
            } 
        }

        // now we need to make sure this doesn't exceed the shape of the array
        // we're working off of
        for( int i=0; i < normalizedGID.length; i++) {
          if( newShape[i] > spec.getShape()[i] )  {
            newShape[i] = spec.getShape()[i];
          }
        }


        //System.out.println("\tcorner: " + Utils.arrayToString(normalizedGID) + 
        //                   " shape: " + Utils.arrayToString(newShape) );
    
        ArraySpec returnSpec = null; 
        try {  
            returnSpec = new ArraySpec(normalizedGID, newShape, "_", "_"); 
        } catch ( Exception e ) {
            System.out.println("Caught an exception in HDF5GroupIDGen.getGIDArraySpec()");
        }

        return returnSpec;
    }

    /**
     * This should take an array, get the bounding GroupIDs 
     * @param myGIDG a HDF5GroupIDGen object
     * @param data the data covered by the group of produced IDs
     * @param spec the logical space to get GroupIDs for
     * @param extractionShape the shape to be tiled over the logical data space
     * @param ones helper array that is full of ones
     * @param returnMap a HashMap of GroupID to Array mappings. This carries the 
     * results of this function.
     */
    public static void pullOutSubArrays( HDF5ArraySpecGen myGIDG, int[] data, 
                                          ArraySpec spec, int[] extractionShape, 
                                          int[] ones,
                                          HashMap<ArraySpec, int[]> returnMap) {

        LOG.info("pullOutSubArrays passed ArraySpec: " + spec.toString() + 
                 " ex shape: " + Utils.arrayToString(extractionShape));
        ArraySpec[] arraySpecs = myGIDG.getBoundingArraySpecs( spec, extractionShape);
        LOG.info("getBoundingGroupIDs: getBounding returned " + arraySpecs.length + " gids " + 
                 " for ArraySpec: " + spec.toString() + 
                 " ex shape: " + Utils.arrayToString(extractionShape));

        ArraySpec tempArraySpec;
        int[] tempIntArray;
        int[] readCorner = new int[extractionShape.length];
        int[] tempShapeArray = new int[readCorner.length];

        for( ArraySpec as : arraySpecs ) {
            tempArraySpec = getGIDArraySpec( spec, as, extractionShape, ones);        
            try { 

                // note, the tempArraySpec is in the global space
                // need to translate that into the local space prior to pull out the sub-array
                for( int i=0; i<readCorner.length; i++) {
                    readCorner[i] = tempArraySpec.getCorner()[i] - spec.getCorner()[i];
                }

                //System.out.println("\t\tlocal read corner: " + Utils.arrayToString(readCorner) );
                for( int i=0; i<tempShapeArray.length; i++) {
                  tempShapeArray[i] = tempArraySpec.getShape()[i];
                }
                tempIntArray = 
                 // (int[])data.sectionNoReduce(readCorner, tempArraySpec.getShape(), ones);
                   (int[])Array.newInstance(int.class, tempShapeArray);


               /* 
                System.out.println("\tsubArray ( gid: " + gid.toString(extractionShape) + 
                               " ex shape: " + Utils.arrayToString(extractionShape) + ")" + 
                               " read corner: " + Utils.arrayToString(readCorner) + 
                               " read shape: " + Utils.arrayToString(tempArraySpec.getShape()) + 
                               "\n"); 
               */
                
                returnMap.put(as, tempIntArray);

            } catch (Exception e) {
                System.out.println("Caught an e in HDF5GroupIDGen.pullOutSubArrays()");
            }
        }

        return;
    }  
   
    /**
     * This is used for testing this class
     */ 
    public static void main(String[] args) {

        // let's add some testing harnesses here
      //System.out.println("Arg[0]: " + args[0]);         

      /*
      String inputFile = "";
      String extractionShape = "";

      if ( args[0].compareTo("-h") == 0 ) {
        System.out.println("groupIDGen.jar --inputFile <intputFile> --extractionShape=\"<extractionShape>\"");
        return;
      } else {
        for ( int i=0; i<args.length; i++) {
          if ( args[i].compareTo("--inputFile") == 0 ) {

          }
        }
      }
      */


        try { 
        	@SuppressWarnings("unused")
			HDF5ArraySpecGen myGIDG = new HDF5ArraySpecGen();

           /*  
            int[] extractionShape = { 2, 4, 2};

            int[] corner = {3,0,0};
            int[] shape = {8,4,4};
            int[] variableShape = {16,4,4};

            int[] ones = new int[corner.length];

            for( int i=0; i<ones.length; i++) {
                ones[i] = 1;
            }
            */
            
            
           /* 
            int[] extractionShape = { 1,360,360,50 };

            int[] corner = {2,0,0,0};
            int[] shape = {6,360,360,50};
            int[] variableShape = {5475,360,360,50};
            */
            
            /*
            int[] extractionShape = { 1,36,36,50 };

            int[] corner = {0,0,0,0};
            int[] shape = {5475,360,360,50};
            int[] variableShape = {5475,360,360,50};
            */

            // wrong order

            /*
            long[] variableShape = {10, 360, 360, 50};
            long[] extractionShape = {1, 360, 360, 50};
            long[] allZeroes = {0,0,0,0};

            System.out.println("exShape: " + Arrays.toString(extractionShape));
            System.out.println("exShape2: " + Utils.arrayToString(extractionShape));

            long[] ones = new long[variableShape.length];
            for( int i=0; i<ones.length; i++) {
                ones[i] = 1;
            }

            ArrayList<long[]> cornerList  = new ArrayList<long[]>();
            ArrayList<long[]> shapeList  = new ArrayList<long[]>();

            long[] tempCorner = new long[variableShape.length];
            long[] tempShape = new long[variableShape.length];

            tempCorner[0] = 0;
            tempCorner[1] = 0;
            tempCorner[2] = 0;
            tempCorner[3] = 0;
            tempShape[0] = 2;
            tempShape[1] = 360;
            tempShape[2] = 360;
            tempShape[3] = 50;
            cornerList.add(tempCorner.clone());
            shapeList.add(tempShape.clone());

            tempCorner[0] = 2;
            tempCorner[1] = 0;
            tempCorner[2] = 0;
            tempCorner[3] = 0;
            tempShape[0] = 1;
            tempShape[1] = 212;
            tempShape[2] = 360;
            tempShape[3] = 50;
            cornerList.add(tempCorner.clone());
            shapeList.add(tempShape.clone());

            tempCorner[0] = 2;
            tempCorner[1] = 212;
            tempCorner[2] = 0;
            tempCorner[3] = 0;
            tempShape[0] = 1;
            tempShape[1] = 1;
            tempShape[2] = 5;
            tempShape[3] = 50;
            cornerList.add(tempCorner.clone());
            shapeList.add(tempShape.clone());

            tempCorner[0] = 2;
            tempCorner[1] = 212;
            tempCorner[2] = 5;
            tempCorner[3] = 0;
            tempShape[0] = 1;
            tempShape[1] = 1;
            tempShape[2] = 1;
            tempShape[3] = 45;
            cornerList.add(tempCorner.clone());
            shapeList.add(tempShape.clone());

            tempCorner[0] = 2;
            tempCorner[1] = 212;
            tempCorner[2] = 5;
            tempCorner[3] = 45;
            tempShape[0] = 1;
            tempShape[1] = 1;
            tempShape[2] = 1;
            tempShape[3] = 5;
            cornerList.add(tempCorner.clone());
            shapeList.add(tempShape.clone());

            tempCorner[0] = 2;
            tempCorner[1] = 212;
            tempCorner[2] = 6;
            tempCorner[3] = 0;
            tempShape[0] = 1;
            tempShape[1] = 1;
            tempShape[2] = 354;
            tempShape[3] = 50;
            cornerList.add(tempCorner.clone());
            shapeList.add(tempShape.clone());

            tempCorner[0] = 2;
            tempCorner[1] = 213;
            tempCorner[2] = 0;
            tempCorner[3] = 0;
            tempShape[0] = 1;
            tempShape[1] = 147;
            tempShape[2] = 360;
            tempShape[3] = 50;
            cornerList.add(tempCorner.clone());
            shapeList.add(tempShape.clone());

            tempCorner[0] = 3;
            tempCorner[1] = 0;
            tempCorner[2] = 0;
            tempCorner[3] = 0;
            tempShape[0] = 2;
            tempShape[1] = 360;
            tempShape[2] = 360;
            tempShape[3] = 50;
            cornerList.add(tempCorner.clone());
            shapeList.add(tempShape.clone());

            //cornerList.add( {0,0,0,0} );
            //shapeList.add( {2, 360, 360, 50} );


           
            long[][] allCorners = new long[cornerList.size()][];
            allCorners = cornerList.toArray(allCorners);
            long[][] allShapes = new long[shapeList.size()][];
            allShapes = shapeList.toArray(allShapes);

            ArraySpec tempSpec = new ArraySpec();

            HashMap<GroupID, Array> returnMap = new HashMap<GroupID, Array>(); 
            for ( int i = 0; i < allCorners.length; i++ ) { 
              ArraySpec key = new ArraySpec(allCorners[i], allShapes[i], "var1","_", variableShape);

              System.out.println("inputSplit corner: " + Utils.arrayToString( allCorners[i] )  +
                                 " shape: " + Utils.arrayToString( allShapes[i] ) );

                int[] values = populateArray(variableShape);

                int[] subArray = 
                  (ArrayInt)values.sectionNoReduce(allCorners[i], allShapes[i], ones);
                pullOutSubArrays( myGIDG, subArray, key, extractionShape, ones, returnMap);
              }


        */
        } catch ( Exception e ) {
            System.out.println("caught an exception in main() \n" + e.toString() );
        }

    }
}
