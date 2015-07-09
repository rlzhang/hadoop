package org.apache.hadoop.hdfs.server.namenode.dummy;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.NameNodeDummy;

public class ClientMerge extends Thread{

  private final static String SLASH = "/";
  private final static String BASEURL = SLASH + INodeServer.PREFIX;
	private DFSClient client;
	private String path;
	private static DirectoryListing curListing;
	private static CountDownLatch latch;
	private static Object obj = new Object();

	public ClientMerge(DFSClient client,String path){
		this.client = client;
		this.path = path;
	}
	
	public void run(){
		try {
			if (NameNodeDummy.DEBUG)
			   NameNodeDummy.debug("[ClientMerge] run : Get directory listing from "+ path);
			if (client == null) {
			  System.err.println("DFSClient is null!");
			  return;
			}
			DirectoryListing thisListing2 = client.listPaths(
					path, HdfsFileStatus.EMPTY_NAME);
			synchronized(obj){
			  ClientMerge.setCurListing(DirectoryListing.merge(getCurListing(), thisListing2));
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
			latch.countDown();
		}
		
	}

	private static Set<Integer> set = new HashSet<Integer>();
	
	/**
	 * server and path must be unique.
	 * @param server
	 * @param path
	 */
	private static void addToSet(String server, String path){
		int hash = (server+path).hashCode();
		set.add(Integer.valueOf(hash));
	}
	
	private static boolean isContain(String server, String path){
		int hash = (server+path).hashCode();
		return set.contains(hash);
	}

  public static DirectoryListing mergeWithThreadPool(ExternalStorage[] es,String src,DirectoryListing curListing){
		if(NameNodeDummy.isNullOrBlank(es)) return curListing;
		ClientMerge.setCurListing(curListing);
		latch = new CountDownLatch(es.length);
		ExecutorService excutor = Executors.newFixedThreadPool(es.length);
		if (NameNodeDummy.DEBUG)
			NameNodeDummy.debug("[ClientMerge] Start threads number is "+es.length);
		for(int i=0;i<es.length;i++){
			String path = BASEURL + es[i].getSourceNNServer() + src;
			
			if(!isContain(es[i].getTargetNNServer(),path)) {
			  NameNodeDummy.info("[ClientMerge] Connect to new name node server "+ es[i].getTargetNNServer() + ";path is "+path);
			  //Add to lru cache.
			  NameNodeDummy.addToLRUMap(src, es[i]);
			  excutor.execute(new ClientMerge(DFSClient.getDfsclient(es[i].getTargetNNServer()),path));
			  addToSet(es[i].getTargetNNServer(),path);
			} else {
			  latch.countDown();
			}
		}
		//Not safe clear! Have to fix!
		set.clear();
		excutor.shutdown();
		try {
			latch.await();
		} catch (InterruptedException e) {
		  e.printStackTrace();
		}
		return ClientMerge.getCurListing();
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	public static DirectoryListing getCurListing() {
		return curListing;
	}

	public static void setCurListing(DirectoryListing curListing) {
		ClientMerge.curListing = curListing;
	}

}
