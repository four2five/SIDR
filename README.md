SIDR 0.2.0
====

This code is based on that used for the experimental portion of the SC '13 paper: http://sc13.supercomputing.org/schedule/event_detail.php?evid=pap313
Since then we have added some functionality and bug fixes.

Installation 
====
These instructions worked on Ubuntu 14.04. YMMV.

I tend to put the export commands into my .bashrc file so that they're persistent.

The hadoop_conf_files directory contains the configuration files I use to run SIDR on a single-node setup. 

1. `sudo apt-get install ant-contrib build-essential git maven python-software-properties autoconf libtool`
    * export CLASSPATH=$CLASSPATH:/usr/share/java/ant-contrib.jar
2. install java 8 (we use oracle’s, you can try openjdk if you want)    
    `sudo add-apt-repository ppa:webupd8team/java;`  
    `sudo apt-get update;`   
    `sudo apt-get install oracle-java8-installer;`
    * export JAVA_HOME=/usr/lib/jvm/java-8-oracle
3. Create a directory to build the code in:   
    `mkdir git`                                  
    `cd git`   
    `git init`   
4. Get our patched version of NetCDF:   
    `git clone https://github.com/four2five/thredds.git`   
    `cd thredds`   
    `git checkout buck_sc13_4.3.18`   
    * set Maven’s memory limit higher than the default ( the next step fails on my host if I don’t do this):     
    * export MAVEN_OPTS="-Xmx512m -XX:MaxPermSize=128m"   
    `mvn install`    
    The jar you need is in thredds/cdm/target and should be named netcdf-4.3.18.jar   
    * export NETCDF_HOME=/home/buck/git/thredds    
    * export NETCDF_JAR=netcdf-4.3.18.jar    
    * export CLASSPATH=$CLASSPATH:$NETCDF_HOME/cdm/target/$NETCDF_JAR    
5. get our patched version of Hadoop (only necessary for the early results work)    
   `cd ../`   
   `git clone https://github.com/four2five/hadoop-common.git`   
   `cd hadoop-common`    
   `git checkout buck_early_results_sc13`    
   `ant mvn-install examples`    
   * Configure hadoop however you want. Make sure that it works with wordcount (or some other comparable test ). I like this tutorial: http://www.michael-noll.com/tutorials/running-hadoop-on-ubuntu-linux-multi-node-cluster/
    * export HADOOP_HOME=/home/buck/git/hadoop-common    
    * export HADOOP_JAR=hadoop-core-1.0.4-early-results.jar    
    * export PATH=$PATH:$HADOOP_HOME/bin    
    * NOTE: There are some example hadoop configuration files in the SIDR repo that you will check out in the next step. Take a look at those for examples of how we configure our nodes.
6. get the SIDR code (updated version of our SciHadoop code)     
   `cd ../`    
   `git clone https://github.com/four2five/SIDR.git`    
   `cd SIDR`     
   `git checkout sc13_experiments_improved`    
   `ant netcdf_hdfs jar`    
    * export SCIHADOOP_HOME=/home/buck/git/SIDR    
    * export SCIHADOOP_JAR=SIDR.jar    
  * Check out the files in the hadoop_conf_files directory. You will want to use these as a template for your hadoop configuration files (especially the HADOOP_CLASSPATH lines)
7. generate test data    
  * NOTE: this will generate several files, one per data type supported. Only test against one of these files at a time    
   `cd tools/file_generator/`    
   `ant jar`    
   `ant run`    
   `hadoop dfs -mkdir netcdf_input`    
   `hadoop dfs -put ./<generated filename> netcdf_input/file1.nc`    
8. run a jobList    
   `cd ..`    
   `./scTestList.script`    

