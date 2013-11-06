SIDR
====

This code was used for the experimental portion of the SC '13 paper: http://sc13.supercomputing.org/schedule/event_detail.php?evid=pap313

We're working on a more feature complete version of this code that should be posted shortly (Oct 2013).

Installation 
====
These instructions worked on Ubuntu 12.04. YMMV.

I tend to put the export commands into my .bashrc file so that they're persistent.

The hadoop_conf_files directory contains the configuration files I use to run SIDR on a single-node setup. 

1. `sudo apt-get install ant-contrib build-essential git maven python-software-properties autoconf libtool`
    * export CLASSPATH=$CLASSPATH:/usr/share/java/ant-contrib.jar
2. install java 7 (we use oracle’s, you can try openjdk if you want)
    `sudo add-apt-repository ppa:webupd8team/java;`  
    `sudo apt-get update;`   
    `sudo apt-get install oracle-java7-installer;`   
    * export JAVA_HOME=/usr/lib/jvm/java-7-oracle     
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
   `git checkout buck-2.2.0r-v1`    
   ` mvn clean package -Pdist -DskipTests -Pnative`    
   * Configure hadoop however you want. Make sure that it works with wordcount (or some other comparable test ). I like this tutorial: http://www.michael-noll.com/tutorials/running-hadoop-on-ubuntu-linux-single-node-cluster/    
    * export HADOOP_HOME=/home/buck/git/hadoop-common    
    * export HADOOP_JAR=hadoop-core-1.0.4-early-results.jar    
    * export PATH=$PATH:$HADOOP_HOME/bin    
6. get the SIDR code (updated version of our SciHadoop code)
   `cd ../`    
   `git clone https://github.com/four2five/SIDR.git`    
   `cd SIDR`     
   `git checkout origin/sc13_experiments_improved_hadoopv2`    
   `ant netcdf_hdfs jar`    
    * export SCIHADOOP_HOME=/home/buck/git/SIDR    
    * export SCIHADOOP_JAR=SIDR.jar    
7. generate test data    
   `cd tools`    
   `ant jar`    
   `ant run`    
   `hadoop dfs -mkdir netcdf_input`    
   `hadoop dfs -put ./<generated filename> netcdf_input/file1.nc`    
8. run a jobList    
   `cd ..`    
   `./scTestList.script`    

