package edu.ucsc.srl.tools;

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

public class AveragingTest 
{
 
  private static Options buildOptions(Options options)
  {
    options.addOption("f", true, "File to read data from");
    options.addOption("v", true, "Varible read");
    options.addOption("t", true, "Timestep to read (zero-base index relative to the start of variable, not absolute value)");
    options.addOption("h", false, "Print the help output");
    return options;
  }

  public static void main(String args[])
  {
    Options options = new Options();
    String variableName = null;
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

      if (line.hasOption("f"))
      {
        log.log(Level.INFO, "I should parse file: " + line.getOptionValue("f"));
        ReadFile rf = new ReadFile();
        ArrayFloat data = rf.readFile(line.getOptionValue("f"), variableName, timeStep);

        // apply the SumAll approach
        SumAll sa = new SumAll();
        float result = sa.sumFloatValues(data); 
        log.log(Level.INFO, "sumall result: " + result);

        // apply the SumAllDouble approach
        SumAllDouble sad = new SumAllDouble();
        result = sad.sumFloatValues(data); 
        log.log(Level.INFO, "sumallDouble result: " + result);

        // apply the Rolling average approach
        RollingAverage ra = new RollingAverage();
        result = ra.averageFloatValues(data);
        log.log(Level.INFO, "rollingaverage result: " + result);

        // apply the Rolling average double approach
        RollingAverageDouble rad = new RollingAverageDouble();
        result = rad.averageFloatValues(data);
        log.log(Level.INFO, "rollingaverageDouble result: " + result);
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

