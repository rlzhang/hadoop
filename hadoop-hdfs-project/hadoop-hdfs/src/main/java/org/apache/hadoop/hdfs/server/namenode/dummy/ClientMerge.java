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

public class ClientMerge {

  private final static String SLASH = "/";
  private final static String BASEURL = SLASH + INodeServer.PREFIX;
  private DirectoryListing curListing;
  private CountDownLatch latch;
  private ExternalStorage[] es;
  private Set<Integer> set = new HashSet<Integer>();
  private String src;

  Object obj = new Object();

  public CountDownLatch getCountDownLatch() {
    return latch;
  }

  /**
   * server and path must be unique.
   * @param server
   * @param path
   */
  public void addToSet(String server, String path) {
    int hash = (server + path).hashCode();
    set.add(Integer.valueOf(hash));
  }

  private boolean isContain(String server, String path) {
    int hash = (server + path).hashCode();
    return set.contains(hash);
  }

  public ClientMerge(ExternalStorage[] es, String src,
      DirectoryListing curListing) {
    this.es = es;
    this.src = src;
    this.curListing = curListing;
  }

  public DirectoryListing start() {
    if (NameNodeDummy.isNullOrBlank(es))
      return curListing;
    latch = new CountDownLatch(es.length);
    ExecutorService excutor = Executors.newFixedThreadPool(es.length);
    if (NameNodeDummy.DEBUG)
      NameNodeDummy.debug("[ClientMerge] Start threads number is " + es.length);
    for (int i = 0; i < es.length; i++) {
      String path = BASEURL + es[i].getSourceNNServer() + src;

      if (!isContain(es[i].getTargetNNServer(), path)) {
        NameNodeDummy.info("[ClientMerge] Connect to new name node server "
            + es[i].getTargetNNServer() + ";path is " + path);
        //Add to lru cache.
        NameNodeDummy.addToLRUMap(src, es[i]);
        excutor.execute(new MergeThread(DFSClient.getDfsclient(es[i]
            .getTargetNNServer()), path, this));
        addToSet(es[i].getTargetNNServer(), path);
      } else {
        latch.countDown();
      }
    }
    excutor.shutdown();
    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      set.clear();
    }
    return this.getCurListing();
  }

  public DirectoryListing getCurListing() {
    return curListing;
  }

  public void setCurListing(DirectoryListing curListing) {
    this.curListing = curListing;
  }

}

class MergeThread extends Thread {
  private DFSClient client;
  private String path;
  ClientMerge cm;

  public MergeThread(DFSClient client, String path, ClientMerge cm) {
    this.client = client;
    this.path = path;
    this.cm = cm;
  }

  public void run() {

    //System.out.println(Thread.currentThread().getId() + ", Done!");
    try {
      if (NameNodeDummy.DEBUG)
        NameNodeDummy.debug("[ClientMerge] run : Get directory listing from "
            + path);
      if (client == null) {
        System.err.println("DFSClient is null!");
        return;
      }
      DirectoryListing thisListing2 =
          client.listPaths(path, HdfsFileStatus.EMPTY_NAME);
      synchronized (cm.obj) {
        cm.setCurListing(cm.getCurListing().merge(thisListing2));
        //cm.setCurListing(DirectoryListing.merge(cm.getCurListing(),
          //  thisListing2));
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      cm.getCountDownLatch().countDown();
    }

  }
}
