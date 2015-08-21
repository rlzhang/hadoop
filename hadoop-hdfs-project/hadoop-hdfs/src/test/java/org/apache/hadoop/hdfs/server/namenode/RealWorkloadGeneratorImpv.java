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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

public class RealWorkloadGeneratorImpv implements Tool {
	private static final Log LOG = LogFactory.getLog(RealWorkloadGeneratorImpv.class);

	static Configuration config;
	static DistributedFileSystem dfs; // used for write namenode like mkdir or create
	static FileSystem fs; //used for read namenode like fileStatus
	static String host; 
	static String prefix = "";
	private int numThreads = 10;
	private String path = "";
	private String file = "";
	int readLineCount = 0;
	final static String FINISH_READ_MESSAGE = "WWT_READING_DONE";
	private long globalCount = 0;
	static int random = 1;
	private boolean printLatency=false;

	static BlockingQueue<String> queue = new LinkedBlockingQueue<String>();

	RealWorkloadGeneratorImpv(Configuration conf) throws IOException {
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
		if (!excludeFile.exists()) {
			if (!excludeFile.getParentFile().exists()
					&& !excludeFile.getParentFile().mkdirs())
				throw new IOException("RealWorkloadGenerator: cannot mkdir "
						+ excludeFile);
		}
		new FileOutputStream(excludeFile).close();
		// set include file
		config.set(DFSConfigKeys.DFS_HOSTS,
				"${hadoop.tmp.dir}/dfs/hosts/include");
		File includeFile = new File(config.get(DFSConfigKeys.DFS_HOSTS,
				"include"));
		new FileOutputStream(includeFile).close();
	}

	void close() throws IOException {

	}

	static void setNameNodeLoggingLevel(Level logLevel) {
		LOG.fatal("Log level = " + logLevel.toString());
		// change log level to NameNode logs
		LogManager.getLogger(NameNode.class.getName()).setLevel(logLevel);
		((Log4JLogger) NameNode.stateChangeLog).getLogger().setLevel(logLevel);
		LogManager.getLogger(NetworkTopology.class.getName())
				.setLevel(logLevel);
		LogManager.getLogger(FSNamesystem.class.getName()).setLevel(logLevel);
		LogManager.getLogger(LeaseManager.class.getName()).setLevel(logLevel);
		LogManager.getLogger(Groups.class.getName()).setLevel(logLevel);
	}

	static void printUsage() {
		System.err.println("Usage: RealWorkloadGenerator" + "\n\t" 
	            + "host " + Mkdir.OP_MKDIR_USAGE + "\n\t" 
				+ "host " + Touch.OP_TOUCH_USAGE + "\n\t" 
				+ "host " + Access.OP_ACCESS_USAGE + "\n\t" 
				+ "host " + Create.OP_CREATE_USAGE);
		System.exit(-1);
	}

	public static void runBenchmark(Configuration conf, List<String> args)
			throws Exception {
		RealWorkloadGeneratorImpv bench = null;
		try {
			bench = new RealWorkloadGeneratorImpv(conf);
			bench.run(args.toArray(new String[] {}));
		} finally {
			if (bench != null)
				bench.close();
		}
	}

	private void initFileSystem(List<String> args) throws Exception {
		if (dfs != null)
			return;
		host = args.get(0);
		Configuration conf = getConf();

		Path globPath = new Path(host);
		fs = globPath.getFileSystem(conf);

		if (!(fs instanceof DistributedFileSystem)) {
			String error = "FileSystem is " + fs.getUri();
			System.err.println(error);
			throw new Exception(error);
		}

		dfs = (DistributedFileSystem) fs;
	}

	/**
	 * Main method of the benchmark.
	 * 
	 * @param aArgs
	 *            command line parameters
	 */
	@Override
	// Tool
	public int run(String[] aArgs) throws Exception {
		List<String> args = new ArrayList<String>(Arrays.asList(aArgs));

		// check arguments
		if (args.size() < 2 || !args.get(1).startsWith("-op"))
			printUsage();

		if (!parseArguments(args)) {
			printUsage();
		}

		String type = args.get(2);

		List<Operation> daemons = new ArrayList<Operation>();
		if (type.equals(Mkdir.Mkdir_OpName)) {
			for (int i = 0; i < this.numThreads; i++) {
				daemons.add(new Mkdir(queue, this.path));
			}
		} else if (type.equals(Touch.Touch_OpName)) {
			for (int i = 0; i < this.numThreads; i++) {
				daemons.add(new Touch(queue, this.path));
			}
		} else if (type.equals(Access.Access_OpName)) {
			for (int i = 0; i < this.numThreads; i++) {
				daemons.add(new Access(queue, this.path,args));
			}
		} else if (type.equals(Create.Create_OpName)) {
			for (int i = 0; i < this.numThreads; i++) {
				daemons.add(new Create(queue, this.path, args));
			}
		} else {
			printUsage();
		}
		
		int plIndex = args.indexOf("-printLatency");
	      printLatency = (plIndex >= 0);
	      if(printLatency) {
	        args.remove(plIndex);
	      }

		// Start the NameNode
		LOG.info("Starting Name Node");
		LOG.info(args.toString());

		this.initFileSystem(args);

		FileReader reader = new FileReader(queue, this.file, this.random);
		new Thread(reader).start();
		long start = Time.now();
		for (Operation op : daemons) {
			op.start();
		}
		for (Operation op : daemons) {
			op.join();
		}
		for (Operation op : daemons) {
			increaseCount(op.localCount);
		}
		
		long elapsedTime = Time.now() - start;
		LOG.info("Spending " + elapsedTime + " miliseconds");
		LOG.info("Ops per sec: " + (globalCount * 1000/ elapsedTime ));
		return 0;
	}

	private void increaseCount(long localCount) {
		globalCount +=localCount;	
	}

	public static void main(String[] args) throws Exception {
		RealWorkloadGeneratorImpv bench = null;
		try {
			bench = new RealWorkloadGeneratorImpv(new HdfsConfiguration());
			ToolRunner.run(bench, args);
		} finally {
			if (bench != null)
				bench.close();
		}
	}

	@Override
	// Configurable
	public void setConf(Configuration conf) {
		config = conf;
	}

	@Override
	// Configurable
	public Configuration getConf() {
		return config;
	}

	private boolean parseArguments(List<String> args) {
		for (int i = 3; i < args.size(); i++) { // parse command line
			if (args.get(i).equals("-threads")) {
				if (i + 1 == args.size())
					return false;
				numThreads = Integer.parseInt(args.get(++i));
			} else if (args.get(i).equals("-path")) {
				if (i + 1 == args.size())
					return false;
				path = args.get(++i);
			} else if (args.get(i).equals("-file")) {
				if (i + 1 == args.size())
					return false;
				file = args.get(++i);
			} else if(args.get(i).equals("-random")){
				if (i + 1 == args.size())
					return false;
				random = Integer.parseInt(args.get(++i));
				LOG.info("random = " + random);
			}
		}
		return true;
	}

	class FileReader implements Runnable {

		private final BlockingQueue<String> queue;
		private String fileName;
		private int random = 1;
		private Random randomGenerator;

		FileReader(BlockingQueue<String> q, String fileName, int random) {
			this.queue = q;
			this.fileName = fileName;
			this.random = random;
			this.randomGenerator = new Random(Time.now());
		}

		@Override
		public void run() {
			int milestoneCount = 100000;
			FileInputStream fstream;
			try {
				fstream = new FileInputStream(this.fileName);
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fstream));

				String strLine;
				while ((strLine = br.readLine()) != null) {
					if(randomGenerator.nextInt(this.random)== this.random-1){
					    this.queue.put(strLine);
					    readLineCount++;
					}
					// print progress
					if (readLineCount % milestoneCount == (milestoneCount-1)) {
						LOG.info("Processed " + readLineCount + " lines");
					}
				}
				this.queue.put(FINISH_READ_MESSAGE);

				// Close the input stream
				br.close();
				LOG.info("Read lines: " + readLineCount);

			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	abstract class Operation extends Thread {
		static final String OpName = "";
		protected final BlockingQueue<String> queue;
		protected String path;
		protected Map<Long, Long> latencySet = new HashMap<Long, Long>();
		protected long localCount = 0;

		public Operation(BlockingQueue<String> q, String path) {
			this.queue = q;
			this.path = path;
		}

		abstract public void executeOne(String line) throws IOException;

		@Override
		public void run() {
			long threadId = Thread.currentThread().getId();
			LOG.info("Thread # " + threadId + " is doing this task");
			String line = null;
			while (true) {
				try {

					line = queue.take();
					if (line.equals(FINISH_READ_MESSAGE)) {
						queue.put(line);
						break;
					}
					 executeOne(line);
					//retryExecuteOne(line, 0, maxTries);

				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(printLatency)
			    this.printLatency();

		}

		
		
		private void printLatency() {
			for (Map.Entry<Long, Long> entry : latencySet.entrySet()) {
				Long time = entry.getKey();
				Long latency = entry.getValue();
				LOG.info("latency," + time + "," + latency);
			}
		}
	}

	class Mkdir extends Operation {
		public Mkdir(BlockingQueue<String> q, String path) {
			super(q, path);
		}

		static final String Mkdir_OpName = "mkdir";
		static final String OP_MKDIR_USAGE = "-op mkdir [-threads T] [-path P] "
				+ "[-file F]";

		@Override
		public void executeOne(String line) throws IOException {
			 retryExecuteOne(line, 0, 3);	
		}
		
		private void retryExecuteOne(String line, int i, int limit) {
			try {
				PathData item = PathData.expandAsGlob(path + line, dfs.getConf())[0];
				item.fs.mkdirs(item.path, FsPermission.getDefault());
				this.localCount++;
			} catch (Exception e) {
				// handle exception
				if (i >= limit) {
					LOG.info("Op failed" + line);
					e.printStackTrace();
					return;
			    }
				retryExecuteOne(line, ++i, limit);
			}
		}

	}

	class Touch extends Operation {
		public Touch(BlockingQueue<String> q, String path) {
			super(q, path);
		}

		static final String OP_TOUCH_USAGE = "-op touch [-threads T] [-path P] "
				+ "[-file F]";
		static final String Touch_OpName = "touch";

		@Override
		public void executeOne(String line) throws IOException {
			short replication = (short) config.getInt(
					DFSConfigKeys.DFS_REPLICATION_KEY, 3);
			final int bufferSize = 100;
			final PathData item = PathData.expandAsGlob(path + line,
					dfs.getConf())[0];
			final FSDataOutputStream out = item.fs.create(item.path, true,
					bufferSize, replication, dfs.getDefaultBlockSize());
			final byte[] b = new byte[10];
			out.write(b, 0, 10);
			out.close();
			this.localCount++;
		}
	}

	class Access extends Operation {

		private static final String OP_ACCESS_USAGE = "-op access [-threads T] [-path P] "
				+ "[-file F] [-random R] [-prefixNumber N]";
		static final String Access_OpName = "access";
		protected int randomNumber=1;
		
		public Access(BlockingQueue<String> q, String path, List<String> args) {
			super(q, path);
			if (fs == null) {
				try {
					fs = FileSystem.get(getConf());
				} catch (IOException e) {
					System.out.println("Cannot create file system");
					e.printStackTrace();
				}
			}
			LOG.info("Using FileSystem fs = FileSystem.get(getConf()); here");
			parseLocalArgument(args);
			
		}
		private boolean parseLocalArgument(List<String> args) {
			for (int i = 3; i < args.size(); i++) { // parse command line
				if (args.get(i).equals("-prefixNumber")) {
					if (i + 1 == args.size())
						return false;
					randomNumber = Integer.parseInt(args.get(++i));
				}
			}
			return true;

		}

		@Override
		public void executeOne(String line) throws IOException {
			long start = Time.now();
			fs.getFileStatus(new Path(path+(new Random().nextInt(this.randomNumber)+1) + line)).getOwner();
			long elapsedTime = Time.now() - start;
			latencySet.put(Time.now(), elapsedTime);
			this.localCount++;
		}

	}

	class Create extends Access {

		private String subDirectoryName = "/test";

		static final String OP_CREATE_USAGE = "-op create [-threads T] [-path P] "
				+ "[-file F] [-random R] [-subDir D]";
		static final String Create_OpName = "create";
		
		public Create(BlockingQueue<String> q, String path, List<String> args) {
			super(q, path,args);
			if (!this.parseLocalArgument(args)) {
				printUsage();
			}
			LOG.info("subDir "+subDirectoryName);
			LOG.info("randomNumber "+this.randomNumber);
		}

		private boolean parseLocalArgument(List<String> args) {
			for (int i = 3; i < args.size(); i++) { // parse command line
				if (args.get(i).equals("-subDir")) {
					if (i + 1 == args.size())
						return false;
					subDirectoryName = args.get(++i);
				}
			}
			return true;

		}

		@Override
		public void executeOne(String line) throws IOException {
			retryExecuteOne(line, 0, 3);

		}

		private void retryExecuteOne(String line, int i, int limit) {
			try {
				long start = Time.now();
				PathData item = PathData.expandAsGlob(path+(new Random().nextInt(this.randomNumber)+1) + line
						+ subDirectoryName, dfs.getConf())[0];
				
				item.fs.mkdirs(item.path, FsPermission.getDefault());
//				System.out.println(path+(new Random().nextInt(this.randomNumber)+1) + line
//						+ subDirectoryName);
				long elapsedTime = Time.now() - start;
				latencySet.put(Time.now(), elapsedTime);
				this.localCount++;
			} catch (Exception e) {
				// handle exception
				if (i >= limit) {
					LOG.info("Op failed" + line);
					e.printStackTrace();
					return;
			    }
				try {
					Thread.sleep(100);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				retryExecuteOne(line, ++i, limit);
			}
		}
		
	}
}
