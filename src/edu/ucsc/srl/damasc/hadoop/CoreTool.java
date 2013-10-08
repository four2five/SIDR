package edu.ucsc.srl.damasc.hadoop; 

import java.io.FileInputStream; 
import java.io.IOException; 
import java.util.Properties; 

import org.apache.hadoop.util.ProgramDriver; 

public class CoreTool { 
 public static void main(String[] args) {

   int exitCode = -1;
   ProgramDriver pgd = new ProgramDriver();

   try {
     pgd.addClass("netcdf_hdfs_median", 
       edu.ucsc.srl.damasc.hadoop.tools.netcdf_hdfs.Median.class, "NetCDF HDFS median job"); 
   } catch (Throwable e) { 
     e.printStackTrace();   
   } 

   try { 
     pgd.driver(args); 
   } catch (Throwable e) { 
     e.printStackTrace(); 
   }

   System.exit(exitCode); 
 } 
} 

