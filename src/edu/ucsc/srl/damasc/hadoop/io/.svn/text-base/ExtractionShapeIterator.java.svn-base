package edu.ucsc.srl.damasc.hadoop.io;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.ArrayList;


/*
import ucar.ma2.Array;
import ucar.ma2.ArrayInt;
import ucar.ma2.Index;
import ucar.ma2.IndexIterator;
import ucar.ma2.InvalidRangeException;
*/

import edu.ucsc.srl.damasc.hadoop.io.ArraySpec;

/**
 */
public class ExtractionShapeIterator { 

    public static boolean debug = false;
    private static final Log LOG = LogFactory.getLog(ExtractionShapeIterator.class);


    public ExtractionShapeIterator() { 
    }

    /*
    private int[] mapToGlobal( int[] currentCounter, int[] corner, 
                               int[] globalCoordinate) {
        for ( int i=0; i < currentCounter.length; i++) {
            globalCoordinate[i] = currentCounter[i] + corner[i];
        }

        return globalCoordinate;
    }
    */

    private ArraySpec normalizeGroupID( int[] globalCoord, int[] groupIDArray, 
                                      ArraySpec outGroupID, int[] extractionShape ) {

        // short circuit out in case extraction shape is not set
        if ( extractionShape.length == 0 ) {
            outGroupID.setCorner(groupIDArray);
            return outGroupID;
        }

        for ( int i=0; i < groupIDArray.length; i++ ) {
            groupIDArray[i] = globalCoord[i] / extractionShape[i];
        }

        outGroupID.setCorner(groupIDArray);
        return outGroupID;
    }

    public void recursiveGetBounding(ArrayList<ArraySpec> arraySpecs, int[] corner,  
                                     int[] shape,
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

    public ArraySpec[] getBoundingArraySpecs( ArraySpec key, int[] extractionShape) {
        return getBoundingArraySpecs(key.getCorner(), key.getShape(), extractionShape );
    }

    public ArraySpec[] getBoundingArraySpecs( int[] corner, int[]  shape, 
                                          int[] extractionShape) {
        // we'll need to do this recursively as this will need to go down n-dimensions deep

        ArrayList<ArraySpec> groupIDs = new ArrayList<ArraySpec>();
        ArraySpec[] returnArray = new ArraySpec[0];

        // seed the recursive method with the groupID of the first point in the corner
        int[] arraySpecArray = new int[extractionShape.length];

        // track which levels are full
        boolean[] levelFull = new boolean[extractionShape.length];

        for( int i=0; i<arraySpecArray.length; i++){
        	arraySpecArray[i] = 0;
            levelFull[i] = false;
        }

        try { 
            ArraySpec myArraySpec = new ArraySpec(arraySpecArray, "");
            // groupIDArray is just a place holder variable for use in the function
            myArraySpec = normalizeGroupID(corner, arraySpecArray, myArraySpec, extractionShape); 

            recursiveGetBounding(groupIDs, corner, shape, extractionShape, 
                                 myArraySpec, 0, levelFull); 

            // return the array
            returnArray = new ArraySpec[groupIDs.size()];
            returnArray = groupIDs.toArray(returnArray);
        } catch ( Exception e ) {
            System.out.println("Caught exception in IdentityMapper.getBoundingGroupIDs() \n" + 
                               e.toString() );
        }

        return returnArray;
    }

    @SuppressWarnings("unused")
	private static ArraySpec getGIDArraySpec( ArraySpec spec, 
                                              ArraySpec as, int[] extractionShape, 
                                              int[] ones) {

        int[] normalizedAS = new int[extractionShape.length];
        int[] inAS = as.getCorner();

        // project the groupID into the global space, via extractionShape, 
        // and then add the startOffset
        for( int i=0; i < normalizedAS.length; i++) {
        	normalizedAS[i] = (inAS[i] * extractionShape[i]); 
        }
        //System.out.println("normalized: " + Utils.arrayToString(normalizedGID));
        
        // we're going to adjust normaliedAS to be the corner of the subArray
        // need a new int[] for the shape of the subArray
        
        int[] newShape = new int[normalizedAS.length];

        // roll through the various dimensions, creating the correct corner / shape
        // pair for this ArraySpec (as)
        for( int i=0; i < normalizedAS.length; i++) {
            // this as starts prior to the data for this dimension
            newShape[i] = extractionShape[i];

            // in this dimension, if spec.getCorner is > normalizedGID,
            // then we need to shrink the shape accordingly.
            // Also, move the normalizedAS to match
            if ( normalizedAS[i] < spec.getCorner()[i]) { 
                newShape[i] = extractionShape[i] - (spec.getCorner()[i] - normalizedAS[i]);
                normalizedAS[i] = spec.getCorner()[i];
            // now, if the shape extends past the spec, shorten it again
            } else if ((normalizedAS[i] + extractionShape[i]) > 
                (spec.getCorner()[i] + spec.getShape()[i]) ){
              newShape[i] = newShape[i] - 
                            ((normalizedAS[i] + extractionShape[i]) - 
                            (spec.getCorner()[i] + spec.getShape()[i]));
            } 
        }

        // now we need to make sure this doesn't exceed the shape of the array
        // we're working off of
        for( int i=0; i < normalizedAS.length; i++) {
          if( newShape[i] > spec.getShape()[i] )  {
            newShape[i] = spec.getShape()[i];
          }
        }


        //System.out.println("\tcorner: " + Utils.arrayToString(normalizedGID) + 
        //                   " shape: " + Utils.arrayToString(newShape) );
    
        ArraySpec returnSpec = null; 
        try {  
            returnSpec = new ArraySpec(normalizedAS, newShape, "_", "_"); 
        } catch ( Exception e ) {
            System.out.println("Caught an exception in ExtractionShapeIterator.getGIDArraySpec()");
        }

        return returnSpec;
    }

    /*
    public static void pullOutSubArrays( ExtractionShapeIterator myGIDG, ArrayInt data, 
                                          ArraySpec spec, int[] extractionShape, 
                                          int[] ones,
                                          HashMap<GroupID, Array> returnMap) {

        LOG.info("pullOutSubArrays passed ArraySpec: " + spec.toString() + 
                 " ex shape: " + Utils.arrayToString(extractionShape));
        GroupID[] gids = myGIDG.getBoundingGroupIDs( spec, extractionShape);
        LOG.info("getBoundingGroupIDs: getBounding returned " + gids.length + " gids " + 
                 " for ArraySpec: " + spec.toString() + 
                 " ex shape: " + Utils.arrayToString(extractionShape));

        ArraySpec tempArraySpec;
        ArrayInt tempIntArray;
        int[] readCorner = new int[extractionShape.length];

        for( GroupID gid : gids ) {
            //System.out.println("gid: " + gid);

            tempArraySpec = getGIDArraySpec( spec, gid, extractionShape, ones);        
            try { 

                // note, the tempArraySpec is in the global space
                // need to translate that into the local space prior to pull out the subarray
                for( int i=0; i<readCorner.length; i++) {
                    readCorner[i] = tempArraySpec.getCorner()[i] - spec.getCorner()[i];
                }

                //System.out.println("\t\tlocal read corner: " + Utils.arrayToString(readCorner) );
                tempIntArray = (ArrayInt)data.sectionNoReduce(readCorner, tempArraySpec.getShape(), ones);

                System.out.println("\tsubArray ( gid: " + gid.toString(extractionShape) + 
                               " ex shape: " + Utils.arrayToString(extractionShape) + ")" + 
                               " read corner: " + Utils.arrayToString(readCorner) + 
                               " read shape: " + Utils.arrayToString(tempArraySpec.getShape()) + 
                               "\n"); 
                
                returnMap.put(gid, tempIntArray);

            } catch (Exception e) {
                System.out.println("Caught an e in ExtractionShapeIterator.pullOutSubArrays()");
            }
        }

        return;
    }  
    */
   
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


        try { 
            NetCDFGroupIDGen myGIDG = new NetCDFGroupIDGen();

            int[] extractionShape = { 2, 4, 2};

            int[] corner = {3,0,0};
            int[] shape = {8,4,4};
            int[] variableShape = {16,4,4};

            int[] ones = new int[corner.length];

            for( int i=0; i<ones.length; i++) {
                ones[i] = 1;
            }
            
            
            int[] extractionShape = { 1,360,360,50 };

            int[] corner = {2,0,0,0};
            int[] shape = {6,360,360,50};
            int[] variableShape = {5475,360,360,50};
            
            int[] extractionShape = { 1,36,36,50 };

            int[] corner = {0,0,0,0};
            int[] shape = {5475,360,360,50};
            int[] variableShape = {5475,360,360,50};

            // wrong order
            int[] variableShape = {10, 360, 360, 50};
            int[] extractionShape = {1, 360, 360, 50};
            int[] allZeroes = {0,0,0,0};

            System.out.println("exShape: " + Arrays.toString(extractionShape));
            System.out.println("exShape2: " + Utils.arrayToString(extractionShape));

            int[] ones = new int[variableShape.length];
            for( int i=0; i<ones.length; i++) {
                ones[i] = 1;
            }

            ArrayList<int[]> cornerList  = new ArrayList<int[]>();
            ArrayList<int[]> shapeList  = new ArrayList<int[]>();

            int[] tempCorner = new int[variableShape.length];
            int[] tempShape = new int[variableShape.length];

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


           
            int[][] allCorners = new int[cornerList.size()][];
            allCorners = cornerList.toArray(allCorners);
            int[][] allShapes = new int[shapeList.size()][];
            allShapes = shapeList.toArray(allShapes);

            ArraySpec tempSpec = new ArraySpec();

            HashMap<GroupID, Array> returnMap = new HashMap<GroupID, Array>(); 
            for ( int i = 0; i < allCorners.length; i++ ) { 
              ArraySpec key = new ArraySpec(allCorners[i], allShapes[i], "var1","_", variableShape);

              System.out.println("inputSplit corner: " + Utils.arrayToString( allCorners[i] )  +
                                 " shape: " + Utils.arrayToString( allShapes[i] ) );

                ArrayInt values = populateArray(variableShape);

                ArrayInt subArray = 
                  (ArrayInt)values.sectionNoReduce(allCorners[i], allShapes[i], ones);
                pullOutSubArrays( myGIDG, subArray, key, extractionShape, ones, returnMap);
              }


        } catch ( Exception e ) {
            System.out.println("caught an exception in main() \n" + e.toString() );
        }

    */
    }
}
