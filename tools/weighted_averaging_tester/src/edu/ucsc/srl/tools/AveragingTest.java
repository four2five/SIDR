package edu.ucsc.srl.tools;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
 
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import ucar.ma2.*;
import ucar.nc2.Dimension;

public class AveragingTest 
{
 
  private static Options buildOptions(Options options)
  {
    options.addOption("f", true, "File to read data from");
    options.addOption("v", true, "Varible read");
    options.addOption("t", true, "Timestep to read (zero-base index relative to the start of variable, not absolute value)");
    options.addOption("w", true, "Weights File");
    options.addOption("h", false, "Print the help output");
    return options;
  }

  public static void main(String args[])
  {
    Options options = new Options();
    String variableName = null;
    String weightsFilename = null;
    int timeStep = 0;

    // build up the Options object
    options  = buildOptions(options);

    // instantiate a parser
    CommandLineParser parser = new BasicParser();

    try 
    {
      CommandLine line = parser.parse(options, args);

      if (line.hasOption("h"))
      {
        help(options);
      }

      if (line.hasOption("v"))
      {
        variableName = line.getOptionValue("v");
      }
      else
      {
        log.log(Level.SEVERE, "Missing -v option");
        help(options);
      }

      if (line.hasOption("t"))
      {
        log.log(Level.INFO, "-t: " + line.getOptionValue("t"));
        timeStep = Integer.parseInt(line.getOptionValue("t"));
      }
      else
      {
        log.log(Level.INFO, "Missing -t option");
      }

      if (line.hasOption("w"))
      {
        weightsFilename = line.getOptionValue("w");
      }
      else
      {
        log.log(Level.SEVERE, "Missing -w option");
        help(options);
      }

      if (line.hasOption("f"))
      {
        log.log(Level.INFO, "I should parse file: " + line.getOptionValue("f"));
        ReadFile rf = new ReadFile();
        ArrayFloat data = rf.readFile(line.getOptionValue("f"), variableName, timeStep);
        ByteBuffer bb = data.getDataAsByteBuffer();

        int[] singleStepSize = rf.getSingleStepShape();

        // number of weights entries is the length of dim 1 (latitude)
        int numWeights = singleStepSize[1];
        float[] weights = rf.loadWeightsFile(weightsFilename, numWeights);

        int[] corner = new int[singleStepSize.length];
        for (int i=0; i<corner.length; i++){
          corner[i] = 0;
        }
        corner[0] = timeStep;

        System.out.println("var: " + variableName +
                           " corner: " + Arrays.toString(corner) +
                           " shape: " + Arrays.toString(singleStepSize)
                           );

	      float result = 0.0f;

        // apply the SumAllDouble approach
        WeightedSumAllDouble wsad = new WeightedSumAllDouble();
        //result = wsad.weightedSumFloatValues(data); 

        // Using the singleStepSize as the extraction shape                              
        // Hard coding 4 as the date type size since there are 4 bytes in a float
        result = wsad.weightedSumFloatValuesBB(bb, corner, singleStepSize, singleStepSize, 4, weights); 
        log.log(Level.INFO, "weightedSumallDouble result: " + result);
      }
      else
      {
        log.log(Level.SEVERE, "Missing f option");
        help(options);
      }
    }
    catch( ParseException exp )
    {
      log.log(Level.SEVERE, "Parsing failed. Reason: " + exp.getMessage());
    }

  }

  private static void help(Options options) {
    // This prints out some help
    HelpFormatter formater = new HelpFormatter();
 
    formater.printHelp("Main", options);
    System.exit(0);
  }


  private static final Logger log = Logger.getLogger(AveragingTest.class.getName());
}

