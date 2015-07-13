/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.NameNodeProxies.ProxyAndInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark.CreateFileStats;
import org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark.OpenFileStats;
import org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark.OperationStatsBase;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

/**
 * Main class for a series of name-node benchmarks.
 * 
 * Each benchmark measures throughput and average execution time 
 * of a specific name-node operation, e.g. file creation or block reports.
 * 
 * The benchmark does not involve any other hadoop components
 * except for the name-node. Each operation is executed
 * by calling directly the respective name-node method.
 * The name-node here is real all other components are simulated.
 * 
 * Command line arguments for the benchmark include:
 * <ol>
 * <li>total number of operations to be performed,</li>
 * <li>number of threads to run these operations,</li>
 * <li>followed by operation specific input parameters.</li>
 * <li>-logLevel L specifies the logging level when the benchmark runs.
 * The default logging level is {@link Level#ERROR}.</li>
 * <li>-UGCacheRefreshCount G will cause the benchmark to call
 * {@link NameNodeRpcServer#refreshUserToGroupsMappings} after
 * every G operations, which purges the name-node's user group cache.
 * By default the refresh is never called.</li>
 * <li>-keepResults do not clean up the name-space after execution.</li>
 * <li>-useExisting do not recreate the name-space, use existing data.</li>
 * </ol>
 * 
 * The benchmark first generates inputs for each thread so that the
 * input generation overhead does not effect the resulting statistics.
 * The number of operations performed by threads is practically the same. 
 * Precisely, the difference between the number of operations 
 * performed by any two threads does not exceed 1.
 * 
 * Then the benchmark executes the specified number of operations using 
 * the specified number of threads and outputs the resulting stats.
 */
public class NNThroughputBenchmarkWenting implements Tool {
  private static final Log LOG = LogFactory.getLog(NNThroughputBenchmarkWenting.class);
  private static final int BLOCK_SIZE = 16;
  private static final String GENERAL_OPTIONS_USAGE = 
    "     [-keepResults] | [-logLevel L] | [-UGCacheRefreshCount G]";

  static Configuration config;
  static DistributedFileSystem dfs;
  static FileSystem fs;
  static String src;
  static String prefix = "";
  //static NamenodeProtocols nameNodeProto;
  //static FileSystem fileSystem;

  NNThroughputBenchmarkWenting(Configuration conf) throws IOException {
    config = conf;
    // We do not need many handlers, since each thread simulates a handler
    // by calling name-node methods directly
    config.setInt(DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_KEY, 1);
    // Turn off minimum block size verification
    config.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);
    // set exclude file
    config.set(DFSConfigKeys.DFS_HOSTS_EXCLUDE,
      "${hadoop.tmp.dir}/dfs/hosts/exclude");
    File excludeFile = new File(config.get(DFSConfigKeys.DFS_HOSTS_EXCLUDE,
      "exclude"));
    if(!excludeFile.exists()) {
      if(!excludeFile.getParentFile().exists() && !excludeFile.getParentFile().mkdirs())
        throw new IOException("NNThroughputBenchmark: cannot mkdir " + excludeFile);
    }
    new FileOutputStream(excludeFile).close();
    // set include file
    config.set(DFSConfigKeys.DFS_HOSTS, "${hadoop.tmp.dir}/dfs/hosts/include");
    File includeFile = new File(config.get(DFSConfigKeys.DFS_HOSTS, "include"));
    new FileOutputStream(includeFile).close();
  }

  void close() throws IOException {
    if(dfs != null)
      dfs.close();
  }

  static void setNameNodeLoggingLevel(Level logLevel) {
    LOG.fatal("Log level = " + logLevel.toString());
    // change log level to NameNode logs
    LogManager.getLogger(NameNode.class.getName()).setLevel(logLevel);
    ((Log4JLogger)NameNode.stateChangeLog).getLogger().setLevel(logLevel);
    LogManager.getLogger(NetworkTopology.class.getName()).setLevel(logLevel);
    LogManager.getLogger(FSNamesystem.class.getName()).setLevel(logLevel);
    LogManager.getLogger(LeaseManager.class.getName()).setLevel(logLevel);
    LogManager.getLogger(Groups.class.getName()).setLevel(logLevel);
  }

  /**
   * Base class for collecting operation statistics.
   * 
   * Overload this class in order to run statistics for a 
   * specific name-node operation.
   */
  abstract class OperationStatsBase {
    protected static final String BASE_DIR_NAME = "/nnThroughputBenchmark";
    protected static final String OP_ALL_NAME = "all";
    protected static final String OP_ALL_USAGE = "-op all <other ops options>";

    protected final String baseDir;
    protected short replication;
    protected int  numThreads = 0;        // number of threads
    protected int  numOpsRequired = 0;    // number of operations requested
    protected int  numOpsExecuted = 0;    // number of operations executed
    protected long cumulativeTime = 0;    // sum of times for each op
    protected long elapsedTime = 0;       // time from start to finish
    protected boolean keepResults = false;// don't clean base directory on exit
    protected Level logLevel;             // logging level, ERROR by default
    protected int ugcRefreshCount = 0;    // user group cache refresh count
    protected boolean printLatency = false;
    protected boolean useFederation = false;
    protected List<StatsDaemon> daemons;

    /**
     * Operation name.
     */
    abstract String getOpName();

    /**
     * Parse command line arguments.
     * 
     * @param args arguments
     * @throws IOException
     */
    abstract void parseArguments(List<String> args) throws IOException;

    /**
     * Generate inputs for each daemon thread.
     * 
     * @param opsPerThread number of inputs for each thread.
     * @throws IOException
     */
    abstract void generateInputs(int[] opsPerThread) throws IOException;

    /**
     * This corresponds to the arg1 argument of 
     * {@link #executeOp(int, int, String)}, which can have different meanings
     * depending on the operation performed.
     * 
     * @param daemonId id of the daemon calling this method
     * @return the argument
     */
    abstract String getExecutionArgument(int daemonId);

    /**
     * Execute name-node operation.
     * 
     * @param daemonId id of the daemon calling this method.
     * @param inputIdx serial index of the operation called by the deamon.
     * @param arg1 operation specific argument.
     * @return time of the individual name-node call.
     * @throws IOException
     */
    abstract long executeOp(int daemonId, int inputIdx, String arg1) throws IOException;

    /**
     * Print the results of the benchmarking.
     */
    abstract void printResults();

    OperationStatsBase() {
      baseDir = BASE_DIR_NAME + "/" + getOpName();
      replication = (short) config.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
      numOpsRequired = 10;
      numThreads = 3;
      logLevel = Level.ERROR;
      ugcRefreshCount = Integer.MAX_VALUE;
    }

    void benchmark() throws IOException {
      daemons = new ArrayList<StatsDaemon>();
      long start = 0;
      try {
        numOpsExecuted = 0;
        cumulativeTime = 0;
        if(numThreads < 1)
          return;
        int tIdx = 0; // thread index < nrThreads
        int opsPerThread[] = new int[numThreads];
        for(int opsScheduled = 0; opsScheduled < numOpsRequired; 
                                  opsScheduled += opsPerThread[tIdx++]) {
          // execute  in a separate thread
          opsPerThread[tIdx] = (numOpsRequired-opsScheduled)/(numThreads-tIdx);
          if(opsPerThread[tIdx] == 0)
            opsPerThread[tIdx] = 1;
        }
        // if numThreads > numOpsRequired then the remaining threads will do nothing
        for(; tIdx < numThreads; tIdx++)
          opsPerThread[tIdx] = 0;
        generateInputs(opsPerThread);
        setNameNodeLoggingLevel(logLevel);
        for(tIdx=0; tIdx < numThreads; tIdx++)
          daemons.add(new StatsDaemon(tIdx, opsPerThread[tIdx], this));
        start = Time.now();
        LOG.info("Starting " + numOpsRequired + " " + getOpName() + "(s). with recording latency");

        for(StatsDaemon d : daemons)
          d.start();
      } finally {
        while(isInPorgress()) {
          // try {Thread.sleep(500);} catch (InterruptedException e) {}
        }
        elapsedTime = Time.now() - start;
        for(StatsDaemon d : daemons) {
        	incrementStats(d.localNumOpsExecuted, d.localCumulativeTime);
          // System.out.println(d.toString() + ": ops Exec = " + d.localNumOpsExecuted);
        	if(printLatency)
        	    d.printLatency();
        }
      }
    }

    private boolean isInPorgress() {
      for(StatsDaemon d : daemons)
        if(d.isInProgress())
          return true;
      return false;
    }

    void cleanUp() throws IOException {
      dfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE,
          false);
      if(!keepResults)
        dfs.delete(getBaseDir(), true);
    }

    int getNumOpsExecuted() {
      return numOpsExecuted;
    }

    long getCumulativeTime() {
      return cumulativeTime;
    }

    long getElapsedTime() {
      return elapsedTime;
    }

    long getAverageTime() {
      return numOpsExecuted == 0 ? 0 : cumulativeTime / numOpsExecuted;
    }

    double getOpsPerSecond() {
      return elapsedTime == 0 ? 0 : 1000*(double)numOpsExecuted / elapsedTime;
    }

    Path getBaseDir() {
      return new Path(baseDir);
    }
    
    String getBaseDirString() {
        return baseDir;
    }

    String getClientName(int idx) {
      return getOpName() + "-client-" + idx;
    }

    void incrementStats(int ops, long time) {
      numOpsExecuted += ops;
      cumulativeTime += time;
    }

    /**
     * Parse first 2 arguments, corresponding to the "-op" option.
     * 
     * @param args argument list
     * @return true if operation is all, which means that options not related
     * to this operation should be ignored, or false otherwise, meaning
     * that usage should be printed when an unrelated option is encountered.
     */
    protected boolean verifyOpArgument(List<String> args) {
      if(args.size() < 2 || ! args.get(1).startsWith("-op"))
        printUsage();

      // process common options
      int krIndex = args.indexOf("-keepResults");
      keepResults = (krIndex >= 0);
      if(keepResults) {
        args.remove(krIndex);
      }
      
      int plIndex = args.indexOf("-printLatency");
      printLatency = (plIndex >= 0);
      if(printLatency) {
        args.remove(plIndex);
      }
      
      int ufIndex = args.indexOf("-useFederation");
      useFederation = (ufIndex >= 0);
      if(useFederation) {
        args.remove(ufIndex);
      }

      int llIndex = args.indexOf("-logLevel");
      if(llIndex >= 0) {
        if(args.size() <= llIndex + 1)
          printUsage();
        logLevel = Level.toLevel(args.get(llIndex+1), Level.ERROR);
        args.remove(llIndex+1);
        args.remove(llIndex);
      }

      int ugrcIndex = args.indexOf("-UGCacheRefreshCount");
      if(ugrcIndex >= 0) {
        if(args.size() <= ugrcIndex + 1)
          printUsage();
        int g = Integer.parseInt(args.get(ugrcIndex+1));
        if(g > 0) ugcRefreshCount = g;
        args.remove(ugrcIndex+1);
        args.remove(ugrcIndex);
      }

      String type = args.get(2);
      if(OP_ALL_NAME.equals(type)) {
        type = getOpName();
        return true;
      }
      if(!getOpName().equals(type))
        printUsage();
      return false;
    }

    void printStats() {
      LOG.info("--- " + getOpName() + " stats  ---");
      LOG.info("# operations: " + getNumOpsExecuted());
      LOG.info("Elapsed Time: " + getElapsedTime());
      LOG.info(" Ops per sec: " + getOpsPerSecond());
      LOG.info("Average Time: " + getAverageTime());
    }
  }

  /**
   * One of the threads that perform stats operations.
   */
  private class StatsDaemon extends Thread {
    private final int daemonId;
    private int opsPerThread;
    private String arg1;      // argument passed to executeOp()
    private volatile int  localNumOpsExecuted = 0;
    private volatile long localCumulativeTime = 0;
    private final OperationStatsBase statsOp;
    private Map<Long,Long> latencySet = new HashMap<Long,Long>();

    StatsDaemon(int daemonId, int nrOps, OperationStatsBase op) {
      this.daemonId = daemonId;
      this.opsPerThread = nrOps;
      this.statsOp = op;
      setName(toString());
    }

    @Override
    public void run() {
      localNumOpsExecuted = 0;
      localCumulativeTime = 0;
      arg1 = statsOp.getExecutionArgument(daemonId);
      try {
        benchmarkOne();
      } catch(IOException ex) {
        LOG.error("StatsDaemon " + daemonId + " failed: \n" 
            + StringUtils.stringifyException(ex));
      }
    }

    @Override
    public String toString() {
      return "StatsDaemon-" + daemonId;
    }
    
    private int refreshUserToGroupsMappings() throws IOException {
        // Get the current configuration
        Configuration conf = getConf();
        
        // for security authorization
        // server principal for this call   
        // should be NN's one.
        conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY, 
            conf.get(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, ""));

        //DistributedFileSystem dfs = getDFS();
        URI dfsUri = dfs.getUri();
        boolean isHaEnabled = HAUtil.isLogicalUri(conf, dfsUri);

        if (isHaEnabled) {
          // Run refreshUserToGroupsMapings for all NNs if HA is enabled
          String nsId = dfsUri.getHost();
          List<ProxyAndInfo<RefreshUserMappingsProtocol>> proxies =
              HAUtil.getProxiesForAllNameNodesInNameservice(conf, nsId,
                  RefreshUserMappingsProtocol.class);
          for (ProxyAndInfo<RefreshUserMappingsProtocol> proxy : proxies) {
            proxy.getProxy().refreshUserToGroupsMappings();
            System.out.println("Refresh user to groups mapping successful for "
                + proxy.getAddress());
          }
        } else {
          // Create the client
          RefreshUserMappingsProtocol refreshProtocol =
              NameNodeProxies.createProxy(conf, FileSystem.getDefaultUri(conf),
                  RefreshUserMappingsProtocol.class).getProxy();

          // Refresh the user-to-groups mappings
          refreshProtocol.refreshUserToGroupsMappings();
          System.out.println("Refresh user to groups mapping successful");
        }
        
        return 0;
    }

    void benchmarkOne() throws IOException {
      for(int idx = 0; idx < opsPerThread; idx++) {
        if((localNumOpsExecuted+1) % statsOp.ugcRefreshCount == 0)
          refreshUserToGroupsMappings();
        long stat = statsOp.executeOp(daemonId, idx, arg1);
        latencySet.put(Time.now(), stat);
        localNumOpsExecuted++;
        localCumulativeTime += stat;
      }
    }

    boolean isInProgress() {
      return localNumOpsExecuted < opsPerThread;
    }

    /**
     * Schedule to stop this daemon.
     */
    void terminate() {
      opsPerThread = localNumOpsExecuted;
    }
    
    void printLatency(){
    	for (Map.Entry<Long, Long> entry : latencySet.entrySet()) {
    		Long time = entry.getKey();
    		Long latency = entry.getValue();
    		LOG.info("latency,"+this.statsOp.getOpName()+","+time+","+latency);
    	}
    }
  }

  /**
   * Clean all benchmark result directories.
   */
  class CleanAllStats extends OperationStatsBase {
    // Operation types
    static final String OP_CLEAN_NAME = "clean";
    static final String OP_CLEAN_USAGE = "-op clean";

    CleanAllStats(List<String> args) {
      super();
      parseArguments(args);
      numOpsRequired = 1;
      numThreads = 1;
      keepResults = true;
    }

    @Override
    String getOpName() {
      return OP_CLEAN_NAME;
    }

    @Override
    void parseArguments(List<String> args) {
      boolean ignoreUnrelatedOptions = verifyOpArgument(args);
      if(args.size() > 3 && !ignoreUnrelatedOptions)
        printUsage();
    }

    @Override
    void generateInputs(int[] opsPerThread) throws IOException {
      // do nothing
    }

    /**
     * Does not require the argument
     */
    @Override
    String getExecutionArgument(int daemonId) {
      return null;
    }

    /**
     * Remove entire benchmark directory.
     */
    @Override
    long executeOp(int daemonId, int inputIdx, String ignore) 
    throws IOException {
      dfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE,
    	          false);
      long start = Time.now();
      dfs.delete(new Path(BASE_DIR_NAME), true);
      long end = Time.now();
      return end-start;
    }

    @Override
    void printResults() {
      LOG.info("--- " + getOpName() + " inputs ---");
      LOG.info("Remove directory " + BASE_DIR_NAME);
      printStats();
    }
  } 

  /**
   * File creation statistics.
   * 
   * Each thread creates the same (+ or -1) number of files.
   * File names are pre-generated during initialization.
   * The created files do not have blocks.
   */
  class CreateFileStats extends OperationStatsBase
  {
      static final String OP_CREATE_NAME = "create";
      static final String OP_CREATE_USAGE = "-op create [-threads T] [-files N] [-filesPerDir P] [-close]";
      protected FileNameGenerator nameGenerator;
      protected String[][] fileNames;
      private boolean closeUponCreate;
      
      CreateFileStats(final List<String> args) { 
    	  super();
    	  this.parseArguments(args);
          if (fs == null) {
              if (this.useFederation) {
                  try {
                      fs = FileSystem.get(getConf());
                      prefix = "/data1/";
                      LOG.info("Using FileSystem fs = FileSystem.get(getConf()); here");
                  }
                  catch (IOException e) {
                      e.printStackTrace();
                  }
              }
              else {
                  fs = (FileSystem)dfs;
                  LOG.info("Using DistributedFileSystem here");
              }
          }
          
      }
      
      String getOpName() {
          return "create";
      }
      
      void parseArguments(final List<String> args) {
          final boolean ignoreUnrelatedOptions = this.verifyOpArgument((List)args);
          int nrFilesPerDir = 4;
          this.closeUponCreate = false;
          for (int i = 3; i < args.size(); ++i) {
              if (args.get(i).equals("-files")) {
                  if (i + 1 == args.size())  printUsage();
                  this.numOpsRequired = Integer.parseInt(args.get(++i));
              }
              else if (args.get(i).equals("-threads")) {
                  if (i + 1 == args.size())   printUsage();
                  this.numThreads = Integer.parseInt(args.get(++i));
              }
              else if (args.get(i).equals("-filesPerDir")) {
                  if (i + 1 == args.size())  printUsage();
                  nrFilesPerDir = Integer.parseInt(args.get(++i));
              }
              else if (args.get(i).equals("-close")) {
                  this.closeUponCreate = true;
              }
              else if (!ignoreUnrelatedOptions) {
                  printUsage();
              }
          }
          this.nameGenerator = new FileNameGenerator(this.getBaseDirString(), nrFilesPerDir);
      }
      
      void generateInputs(final int[] opsPerThread) throws IOException {
          assert opsPerThread.length == this.numThreads : "Error opsPerThread.length";
          dfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE, false);
          LOG.info("Generate " + this.numOpsRequired + " intputs for " + this.getOpName());
          this.fileNames = new String[this.numThreads][];
          for (int idx = 0; idx < this.numThreads; ++idx) {
              final int threadOps = opsPerThread[idx];
              this.fileNames[idx] = new String[threadOps];
              for (int jdx = 0; jdx < threadOps; ++jdx) {
                  this.fileNames[idx][jdx] = this.nameGenerator.getNextFileName("ThroughputBench");
              }
          }
      }
      
      void dummyActionNoSynch(final int daemonId, final int fileIdx) {
          for (int i = 0; i < 2000; ++i) {
              this.fileNames[daemonId][fileIdx].contains("" + i);
          }
      }
      
      String getExecutionArgument(final int daemonId) {
          return this.getClientName(daemonId);
      }
      
      long executeOp(final int daemonId, final int inputIdx, final String clientName) throws IOException {
          final long start = Time.now();
          final int bufferSize = 100;
          final PathData item = PathData.expandAsGlob("/data1/" + this.fileNames[daemonId][inputIdx], fs.getConf())[0];
          final FSDataOutputStream out = item.fs.create(item.path, true, bufferSize, this.replication, dfs.getDefaultBlockSize());
          final byte[] b = new byte[100];
          out.write(b, 0, 100);
          out.close();
          final long end = Time.now();
          return end - start;
      }
      
      void printResults() {
          LOG.info((Object)("--- " + this.getOpName() + " inputs ---"));
          LOG.info((Object)("nrFiles = " + this.numOpsRequired));
          LOG.info((Object)("nrThreads = " + this.numThreads));
          LOG.info((Object)("nrFilesPerDir = " + this.nameGenerator.getFilesPerDirectory()));
          this.printStats();
      }
  }

  
  class OpenFileStats extends CreateFileStats
  {
      static final String OP_OPEN_NAME = "open";
      static final String OP_USAGE_ARGS = " [-threads T] [-files N] [-filesPerDir P] [-useExisting]";
      static final String OP_OPEN_USAGE = "-op open [-threads T] [-files N] [-filesPerDir P] [-useExisting]";
      private boolean useExisting;
      
      OpenFileStats( List<String> args) {
          super(args);
      }
      
      String getOpName() {
          return "open";
      }
      
      void parseArguments(List<String> args) {
          final int ueIndex = args.indexOf("-useExisting");
          this.useExisting = (ueIndex >= 0);
          if (this.useExisting) {
              args.remove(ueIndex);
          }
          super.parseArguments(args);
      }
      
      void generateInputs(final int[] opsPerThread) throws IOException {
          final String[] createArgs = { src, "-op", "create", "-threads", String.valueOf(this.numThreads), "-files", String.valueOf(this.numOpsRequired), "-filesPerDir", String.valueOf(this.nameGenerator.getFilesPerDirectory()), "-close" };
          final NNThroughputBenchmarkWenting.CreateFileStats opCreate = new NNThroughputBenchmarkWenting.CreateFileStats((List)Arrays.asList(createArgs));
          if (!this.useExisting) {
              opCreate.benchmark();
              LOG.info((Object)("Created " + this.numOpsRequired + " files."));
          }
          else {
              LOG.info((Object)("useExisting = true. Assuming " + this.numOpsRequired + " files have been created before."));
          }
          super.generateInputs(opsPerThread);
          if (!useExisting && fs.exists(opCreate.getBaseDir()) && !fs.exists(this.getBaseDir())) {
              LOG.info("rename path from " + opCreate.getBaseDir() + "to" + this.getBaseDir());
              dfs.rename(opCreate.getBaseDir(), this.getBaseDir());
          }
          if (!useExisting && !fs.exists(this.getBaseDir())) {
              throw new IOException(this.getBaseDir() + " does not exist.");
          }
      }
      
      long executeOp(final int daemonId, final int inputIdx, final String ignore) throws IOException {
          final long start = Time.now();
          fs.getFileBlockLocations(fs.getFileStatus(new Path(this.fileNames[daemonId][inputIdx])), 0L, 16L);
          final long end = Time.now();
          return end - start;
      }
  }

  class FileStatusStats extends OpenFileStats {

	   static final String OP_FILE_STATUS_NAME = "fileStatus";
	   static final String OP_FILE_STATUS_USAGE = "-op fileStatus [-threads T] [-files N] [-filesPerDir P] [-useExisting]";


	   FileStatusStats(List<String> args) {
	      super(args);
	   }

	   String getOpName() {
	      return "fileStatus";
	   }

	   long executeOp(int daemonId, int inputIdx, String ignore) throws IOException {
	      long start = Time.now();
	      fs.getFileStatus(new Path(prefix + this.fileNames[daemonId][inputIdx]));
	      long end = Time.now();
	      return end - start;
	   }
	}
 

  /**
   * Directory creation statistics.
   *
   * Each thread creates the same (+ or -1) number of directories.
   * Directory names are pre-generated during initialization.
   */
  class MkdirsStats extends OperationStatsBase {
    // Operation types
    static final String OP_MKDIRS_NAME = "mkdirs";
    static final String OP_MKDIRS_USAGE = "-op mkdirs [-threads T] [-dirs N] " +
        "[-dirsPerDir P]";

    protected FileNameGenerator nameGenerator;
    protected String[][] dirPaths;

    MkdirsStats(List<String> args) {
      super();
      parseArguments(args);
    }

    @Override
    String getOpName() {
      return OP_MKDIRS_NAME;
    }

    @Override
    void parseArguments(List<String> args) {
      boolean ignoreUnrelatedOptions = verifyOpArgument(args);
      int nrDirsPerDir = 2;
      for (int i = 3; i < args.size(); i++) {       // parse command line
        if(args.get(i).equals("-dirs")) {
          if(i+1 == args.size())  printUsage();
          numOpsRequired = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-threads")) {
          if(i+1 == args.size())  printUsage();
          numThreads = Integer.parseInt(args.get(++i));
        } else if(args.get(i).equals("-dirsPerDir")) {
          if(i+1 == args.size())  printUsage();
          nrDirsPerDir = Integer.parseInt(args.get(++i));
        } else if(!ignoreUnrelatedOptions)
          printUsage();
      }
      nameGenerator = new FileNameGenerator(getBaseDirString(), nrDirsPerDir);
    }

    @Override
    void generateInputs(int[] opsPerThread) throws IOException {
      assert opsPerThread.length == numThreads : "Error opsPerThread.length";
      dfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE,
              false);
      LOG.info("Generate " + numOpsRequired + " inputs for " + getOpName());
      dirPaths = new String[numThreads][];
      for(int idx=0; idx < numThreads; idx++) {
        int threadOps = opsPerThread[idx];
        dirPaths[idx] = new String[threadOps];
        for(int jdx=0; jdx < threadOps; jdx++)
          dirPaths[idx][jdx] = nameGenerator.
              getNextFileName("ThroughputBench");
      }
    }

    /**
     * returns client name
     */
    @Override
    String getExecutionArgument(int daemonId) {
      return getClientName(daemonId);
    }

    /**
     * Do mkdirs operation.
     */
    @Override
    long executeOp(int daemonId, int inputIdx, String clientName)
        throws IOException {
      long start = Time.now();
      // not a glob & file not found, so add the path with a null stat
      PathData item = PathData.expandAsGlob("/data1/"+dirPaths[daemonId][inputIdx],dfs.getConf())[0];
      item.fs.mkdirs(item.path, FsPermission.getDefault());
      long end = Time.now();
      return end-start;
    }

    @Override
    void printResults() {
      LOG.info("--- " + getOpName() + " inputs ---");
      LOG.info("nrDirs = " + numOpsRequired);
      LOG.info("nrThreads = " + numThreads);
      LOG.info("nrDirsPerDir = " + nameGenerator.getFilesPerDirectory());
      printStats();
    }
  }

  

  static void printUsage() {
    System.err.println("Usage: NNThroughputBenchmark"
        + "\n\t"    + OperationStatsBase.OP_ALL_USAGE
        + " | \n\t" + CreateFileStats.OP_CREATE_USAGE
        + " | \n\t" + MkdirsStats.OP_MKDIRS_USAGE
        + " | \n\t" + OpenFileStats.OP_OPEN_USAGE
        //+ " | \n\t" + DeleteFileStats.OP_DELETE_USAGE
        + " | \n\t" + FileStatusStats.OP_FILE_STATUS_USAGE
        //+ " | \n\t" + RenameFileStats.OP_RENAME_USAGE
        // + " | \n\t" + BlockReportStats.OP_BLOCK_REPORT_USAGE
        // + " | \n\t" + ReplicationStats.OP_REPLICATION_USAGE
        + " | \n\t" + CleanAllStats.OP_CLEAN_USAGE
        + " | \n\t" + GENERAL_OPTIONS_USAGE
    );
    System.exit(-1);
  }

  public static void runBenchmark(Configuration conf, List<String> args)
      throws Exception {
    NNThroughputBenchmarkWenting bench = null;
    try {
      bench = new NNThroughputBenchmarkWenting(conf);
      bench.run(args.toArray(new String[]{}));
    } finally {
      if(bench != null)
        bench.close();
    }
  }

  private void initFileSystem(List<String> args) throws Exception {
	    if (dfs != null) return;
	    src = args.get(0);
	    Configuration conf = getConf();
	    
	    Path globPath = new Path(src);
	    FileSystem fs = globPath.getFileSystem(conf);
	    
	    FileSystem.printStatistics();
	    if (!(fs instanceof DistributedFileSystem)) {
	      String error = "FileSystem is " + fs.getUri();
	      System.err.println(error);
	      throw new Exception(error);
	    }
	    
	    dfs = (DistributedFileSystem) fs;
  }
  /**
   * Main method of the benchmark.
   * @param aArgs command line parameters
   */
  @Override // Tool
  public int run(String[] aArgs) throws Exception {
    List<String> args = new ArrayList<String>(Arrays.asList(aArgs));
    if(args.size() < 3 || ! args.get(1).startsWith("-op"))
      printUsage();

    String type = args.get(2);
    boolean runAll = OperationStatsBase.OP_ALL_NAME.equals(type);

    // Start the NameNode
    String[] argv = new String[] {};
    LOG.info("Starting Name Node");
    LOG.info(args.toString());
    
    
    this.initFileSystem(args);

    List<OperationStatsBase> ops = new ArrayList<OperationStatsBase>();
    OperationStatsBase opStat = null;
    try {
     
      if(runAll || MkdirsStats.OP_MKDIRS_NAME.equals(type)) {
        opStat = new MkdirsStats(args);
        ops.add(opStat);
      }
      if(runAll || CreateFileStats.OP_CREATE_NAME.equals(type)) {
          opStat = new CreateFileStats(args);
          ops.add(opStat);
        }
      if(runAll || OpenFileStats.OP_OPEN_NAME.equals(type)) {
          opStat = new OpenFileStats(args);
          ops.add(opStat);
        }
      
      if(runAll || FileStatusStats.OP_FILE_STATUS_NAME.equals(type)) {
          opStat = new FileStatusStats(args);
          ops.add(opStat);
        }
      
      if(runAll || CleanAllStats.OP_CLEAN_NAME.equals(type)) {
        opStat = new CleanAllStats(args);
        ops.add(opStat);
      }
      if(ops.size() == 0)
        printUsage();
      // run each benchmark
      for(OperationStatsBase op : ops) {
        LOG.info("Starting benchmark: " + op.getOpName());
        op.benchmark();
        op.cleanUp();
      }
      // print statistics
      for(OperationStatsBase op : ops) {
        LOG.info("");
        op.printResults();
      }
    } catch(Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw e;
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    NNThroughputBenchmarkWenting bench = null;
    try {
      bench = new NNThroughputBenchmarkWenting(new HdfsConfiguration());
      ToolRunner.run(bench, args);
    } finally {
      if(bench != null)
        bench.close();
    }
  }

  @Override // Configurable
  public void setConf(Configuration conf) {
    config = conf;
  }

  @Override // Configurable
  public Configuration getConf() {
    return config;
  }
}
