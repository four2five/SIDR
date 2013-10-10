package edu.ucsc.srl.damasc.hadoop;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

public class GenerateMainTool { 

	private static void write_starting_code(BufferedWriter writer, Boolean netcdf, 
																					Boolean netcdf_hdfs, Boolean hdf5) 
                                                            throws IOException { 
	  writer.write(
	    "package edu.ucsc.srl.damasc.hadoop; \n" + 
	    "\n" + 
	    "import java.io.FileInputStream; \n" + 
	    "import java.io.IOException; \n" + 
	    "import java.util.Properties; \n" + 
	    "\n" + 
	    "import org.apache.hadoop.util.ProgramDriver; \n" + 
	    "\n" + 
	    "public class CoreTool { \n" + 
	    " public static void main(String[] args) {\n" + 
	    "\n" + 
      "   int exitCode = -1;\n" + 
      "   ProgramDriver pgd = new ProgramDriver();\n" + 
      "\n"  
	  );

    if (netcdf) { 
      writer.write(
        "   try {\n" + 
        "     pgd.addClass(\"netcdf_identity\", \n" + 
        "       edu.ucsc.srl.damasc.hadoop.tools.netcdf.Identity.class, \"NetCDF identity job\"); \n" + 
        "     pgd.addClass(\"netcdf_average\", \n" + 
        "       edu.ucsc.srl.damasc.hadoop.tools.netcdf.Average.class, \"NetCDF average job\"); \n" + 
        "     pgd.addClass(\"netcdf_median\", \n" + 
        "       edu.ucsc.srl.damasc.hadoop.tools.netcdf.Median.class, \"NetCDF median job\"); \n" + 
        "     pgd.addClass(\"netcdf_simplemedian\", \n" + 
        "       edu.ucsc.srl.damasc.hadoop.tools.netcdf.SimpleMedian.class, \"NetCDF simple median job\"); \n" + 
        "     pgd.addClass(\"netcdf_supersimplemedian\", \n" + 
        "       edu.ucsc.srl.damasc.hadoop.tools.netcdf.SuperSimpleMedian.class, \"NetCDF super simple median job\"); \n" + 
        "     pgd.addClass(\"netcdf_filter\", \n" + 
        "       edu.ucsc.srl.damasc.hadoop.tools.netcdf.Filter.class, \"NetCDF filter job\"); \n" + 
        "   } catch (Throwable e) { \n" + 
        "     e.printStackTrace();   \n" + 
        "   } \n" + 
        "\n"
      );
    }

    if (netcdf_hdfs) { 
      writer.write(
        "   try {\n" + 
        //"     pgd.addClass(\"hdf5_identity\", \n" + 
        //"       edu.ucsc.srl.damasc.hadoop.tools.hdf5.Identity.class, \"HDF5 identity job\"); \n" + 
        //"     pgd.addClass(\"hdf5_simplemedian\", \n" + 
        //"       edu.ucsc.srl.damasc.hadoop.tools.hdf5.SimpleMedian.class, \"HDF5 simple median job\"); \n" + 
        //"     pgd.addClass(\"hdf5_supersimplemedian\", \n" + 
        //"       edu.ucsc.srl.damasc.hadoop.tools.hdf5.SuperSimpleMedian.class, \"HDF5 super simple median job\"); \n" + 
        "     pgd.addClass(\"netcdf_hdfs_median\", \n" + 
        "       edu.ucsc.srl.damasc.hadoop.tools.netcdf_hdfs.Median.class, \"NetCDF HDFS median job\"); \n" + 
        "     pgd.addClass(\"netcdf_hdfs_median2\", \n" + 
        "       edu.ucsc.srl.damasc.hadoop.tools.netcdf_hdfs.Median2.class, \"NetCDF HDFS median2 job\"); \n" + 
        //"     pgd.addClass(\"netcdf_hdfs_average\", \n" + 
        //"       edu.ucsc.srl.damasc.hadoop.tools.netcdf_hdfs.Average.class, \"NetCDF HDFS average job\"); \n" + 
        //"     pgd.addClass(\"netcdf_hdfs_filter\", \n" + 
        //"       edu.ucsc.srl.damasc.hadoop.tools.netcdf_hdfs.Filter.class, \"NetCDF HDFS filter job\"); \n" + 
        //"     pgd.addClass(\"netcdf_hdfs_coordvar_filter\", \n" + 
        //"       edu.ucsc.srl.damasc.hadoop.tools.netcdf_hdfs.CoordVarFilter.class, \"NetCDF HDFS filter job\"); \n" + 
        "   } catch (Throwable e) { \n" + 
        "     e.printStackTrace();   \n" + 
        "   } \n" + 
        "\n"
      );
    }

    if (hdf5) { 
      writer.write(
        "   try {\n" + 
        "     pgd.addClass(\"hdf5_average\", \n" + 
        "       edu.ucsc.srl.damasc.hadoop.tools.hdf5.Average.class, \"HDF5 average job\"); \n" + 
        //"     pgd.addClass(\"hdf5_identity\", \n" + 
        //"       edu.ucsc.srl.damasc.hadoop.tools.hdf5.Identity.class, \"HDF5 identity job\"); \n" + 
        //"     pgd.addClass(\"hdf5_median\", \n" + 
        //"       edu.ucsc.srl.damasc.hadoop.tools.hdf5.Median.class, \"HDF5 median job\"); \n" + 
        //"     pgd.addClass(\"hdf5_simplemedian\", \n" + 
        //"       edu.ucsc.srl.damasc.hadoop.tools.hdf5.SimpleMedian.class, \"HDF5 simple median job\"); \n" + 
        //"     pgd.addClass(\"hdf5_supersimplemedian\", \n" + 
        //"       edu.ucsc.srl.damasc.hadoop.tools.hdf5.SuperSimpleMedian.class, \"HDF5 super simple median job\"); \n" + 
        //"     pgd.addClass(\"hdf5_filter\", \n" + 
        //"       edu.ucsc.srl.damasc.hadoop.tools.hdf5.Filter.class, \"HDF5 filter job\"); \n" + 
        "   } catch (Throwable e) { \n" + 
        "     e.printStackTrace();   \n" + 
        "   } \n" + 
        "\n"
      );
    }

    writer.write(
      "   try { \n" + 
      "     pgd.driver(args); \n" + 
      "   } catch (Throwable e) { \n" + 
      "     e.printStackTrace(); \n" + 
      "   }\n" + 
      "\n" + 
      "   System.exit(exitCode); \n" + 
      " } \n" + 
      "} \n" + 
      "\n"
    );
	
	}
	
	public static void main(String[] args) { 

    Properties prop = new Properties();
    try {
      //load a properties file generated by Ant
      prop.load(new FileInputStream("file_formats.properties"));
    } catch (IOException ex) {
      ex.printStackTrace();
    }

    Boolean build_netcdf = new Boolean(prop.getProperty("netcdf"));
    Boolean build_netcdf_hdfs = new Boolean(prop.getProperty("netcdf_hdfs"));
    Boolean build_hdf5 = new Boolean(prop.getProperty("hdf5"));

    // open the filepath to the generated data
    String outputFile = args[0];
    System.out.println("Writing generated file as " + outputFile);
    BufferedWriter writer = null;
    FileWriter output = null;
    try {
      output = new FileWriter(outputFile);
      writer = new BufferedWriter(output);

      //generate starting broilerplate
      write_starting_code(writer,build_netcdf, build_netcdf_hdfs, build_hdf5);

      // writer.write(s);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException e) {
        // Ignore issues during closing
        }
      }
    }



    if (build_netcdf) { 
      System.out.println("JB, you should generate NetCDF classes");
    } else { 
      System.out.println("JB, screw NetCDF classes");
    }

    if (build_netcdf_hdfs) { 
      System.out.println("JB, you should generate NetCDF HDFS classes");
    } else { 
      System.out.println("JB, screw NetCDF HDFS classes");
    }

    if (build_hdf5) { 
      System.out.println("JB, you should generate HDF5 classes");
    } else { 
      System.out.println("JB, screw HDF5 classes");
    }

  }
}
