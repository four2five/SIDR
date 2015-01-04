package edu.ucsc.srl.tools;

import java.util.logging.Level;
import java.util.logging.Logger;
 
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import ucar.ma2.*;

public class AveragingTest 
{
 
  private static Options buildOptions(Options options)
  {
    options.addOption("f", true, "File to read data from");
    options.addOption("h", false, "Print the help output");
    return options;
  }

  public static void main(String args[])
  {
    Options options = new Options();
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

      if (line.hasOption("f"))
      {
        log.log(Level.INFO, "I should parse file: " + line.getOptionValue("f"));
        ReadFile rf = new ReadFile();
        ArrayFloat data = rf.readFile(line.getOptionValue("f"));

        // apply the SumAll approach
        SumAll sa = new SumAll();
        float result = sa.sumFloatValues(data); 
        log.log(Level.INFO, "sumall result: " + result);

        // apply the Rolling average approach
        RollingAverage ra = new RollingAverage();
        result = ra.averageFloatValues(data);
        log.log(Level.INFO, "rollingaverage result: " + result);

      }
      else
      {
        log.log(Level.SEVERE, "Missing f option");
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

