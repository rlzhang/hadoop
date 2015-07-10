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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
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
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

public class RealWorkloadGenerator implements Tool {
  private static final Log LOG = LogFactory.getLog(RealWorkloadGenerator.class);
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

  RealWorkloadGenerator(Configuration conf) throws IOException {
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
  
  static void printUsage() {
    System.err.println("Usage: NNThroughputBenchmark"
    );
    System.exit(-1);
  }

  public static void runBenchmark(Configuration conf, List<String> args)
      throws Exception {
    RealWorkloadGenerator bench = null;
    try {
      bench = new RealWorkloadGenerator(conf);
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

    // Start the NameNode
    String[] argv = new String[] {};
    LOG.info("Starting Name Node");
    LOG.info(args.toString());
    
    
   this.initFileSystem(args);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    RealWorkloadGenerator bench = null;
    try {
      bench = new RealWorkloadGenerator(new HdfsConfiguration());
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
