package org.apache.hadoop.hdfs.server.namenode.dummy;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeDummy;
import org.apache.hadoop.hdfs.server.namenode.Quota;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.security.AccessControlException;

import com.esotericsoftware.kryonet.Connection;

/**
 * Start a new thread to handle data come from client.
 * @author rzhan33
 *
 */
public class UpdateNIOData extends Thread {

  private static Collection<String> host = Collections.synchronizedCollection(new HashSet<String>());
   static Map<String, ConcurrentHashMap<Integer, MapRequest>> serversMap =
      new java.util.concurrent.ConcurrentHashMap<String, ConcurrentHashMap<Integer, MapRequest>>();
  private ConcurrentHashMap<Integer, MapRequest> map;

  private static Object obj = new Object();
  Object object;
  INodeDirectory parent;
  long listSize;
  //INodeServer server;
  Connection connection;
  public UpdateNIOData(Object object,
      INodeDirectory parent, long listSize, Connection connection) {
    
    this.object = object;
    this.parent = parent;
    this.listSize = listSize;
    this.connection = connection;
    //waitClientData(hostName);
  }

  public static Map<String, ConcurrentHashMap<Integer, MapRequest>> getServersMap() {
    return serversMap;
  }

  private void init(String hostName) {
    if (serversMap.get(hostName) == null)
      synchronized (obj) {
        if (serversMap.get(hostName) == null) {
          map = new ConcurrentHashMap<Integer, MapRequest>();
          serversMap.put(hostName, map);
        }
      }
    this.map = serversMap.get(hostName);

  }

  public void run() {

    String hostName = connection.getRemoteAddressTCP().getAddress().getHostAddress();
    this.init(hostName);
    MapRequest[] request = (MapRequest[]) object;
    if (request.length < 1) {
      System.err.println("Wrong len from NIO channel: " + request.length);
      map.clear();
      return;
    }
    if (parent == null) {
      NameNodeDummy.LOG.error("Namenode server not ready yet, do nothing!");
      map.clear();
      return;
    }
    for (int i = 0; i < request.length; i++) {
      //if (!NameNodeDummy.TEST)
      map.put(request[i].getKey(), request[i]);
    }

    System.out.println("Server received handleMapRequestArray size is "
        + map.size());
    this.check();
  }

  public void updateBlocksMap(INodeFile file) {
    // Add file->block mapping
    final BlockInfo[] blocks = file.getBlocks();
    if (blocks != null) {
      final BlockManager bm =
          NameNodeDummy.getNameNodeDummyInstance().getBlockManager();
      for (int i = 0; i < blocks.length; i++) {
        if (NameNodeDummy.DEBUG)
          System.out
              .println("[INodeServer:updateBlocksMap]--------Adding to blockmap: blockid = "
                  + blocks[i].getBlockId()
                  + "; Collection = "
                  + file.getFullPathName());
        file.setBlock(i, bm.addBlockCollection(blocks[i], file));
      }
    }
  }

  /**
   * If not finish in 5 minutes, clear cache.
   * @param hostName
   */
  private void waitClientData(String hostName) {
    if (!host.contains(hostName))
    synchronized(host) {
      if (!host.contains(hostName)) {
      Timer timer = new Timer();
      host.add(hostName);
      timer.schedule(new ClearMapByHost(hostName),600 * 1000);
      }
    }
    
  }

  /**
   * 
   * @param inode
   */
  private void addChildren(INode inode) {

    if (inode.isFile()) {
      addINodeToMap(inode);
      this.updateBlocksMap(inode.asFile());
      return;
    }
    if (!inode.isDirectory()) {
      System.err.println("Not a directory and file! " + inode.getFullPathName()
          + inode.isSymlink());
      return;
    }
    ReadOnlyList<INode> roList =
        inode.asDirectory().getChildrenList(Snapshot.CURRENT_STATE_ID);
    Iterator<INode> ite = roList.iterator();
    //for (int i = 0; i < roList.size(); i++) {
    while (ite.hasNext()) {
      addChildren(ite.next());
    }
    if (!INodeServer.isTest)
      addINodeToMap(inode);
  }

  private void receivedAllData(Connection connection, long size) {
    ClientCommends cc = new ClientCommends();
    cc.setCommand(1);
    cc.setListSize(size);
    connection.sendTCP(cc);
  }

  private void check() {
    if (listSize == map.size())
      synchronized (obj) {
        if (listSize == map.size()) {
          System.out.println("Server received all the data!" + map.size());
          //INodeServer.getThreadPool().shutdown();
          this.receivedAllData(connection, listSize);
          SplitTree splitTree = new SplitTree();
          INode inode = splitTree.mergeListToINode(map);
          if (NameNodeDummy.DEBUG)
            System.out.println("After merged: inode = "
                + inode.getFullPathName());
          // this.addBlockMap(inode);
          if (parent != null) {
            // parent.addChild(inode);
            this.recursiveAddNode(inode, parent);
          }
          //Display tree.
          //System.out.println(Tools.display(inode, 10, true));
          map.clear();
          try {
            if (!INodeServer.isTest)
              NameNodeDummy.getNameNodeDummyInstance().saveNamespace();
            System.out.println("Force saved the namespace!!");
          } catch (AccessControlException e) {
            e.printStackTrace();
          } catch (IOException e) {
            e.printStackTrace();
          }
          // Should update here.
          this.updateQuota();
        }
      }

  }

  private void updateQuota() {
    INodeTools.updateCountForQuotaRecursively(INodeServer.root,
        Quota.Counts.newInstance());
    if (NameNodeDummy.DEBUG)
      System.out.println(NameNodeDummy.getNameNodeDummyInstance()
          .printNSInfo(INodeServer.root, 0, 10).replaceAll("&nbsp;", " ")
          .replace("<br/>", "\n"));

  }

  /**
   * If the sub-tree existing on the target NN, will recursively add
   * diff one
   * 
   * @param child
   */
  private synchronized void recursiveAddNode(INode child, INodeDirectory parent) {

    INode temp =
        parent.getChild(DFSUtil.string2Bytes(child.getLocalName()),
            Snapshot.CURRENT_STATE_ID);
    //Reset parent
    child.setParent(parent);
    if (child.isFile()) {
      if (temp == null) {
        parent.addChild(child);

        addINodeToMap(child);
        NameNodeDummy.getNameNodeDummyInstance().addINode(child.asFile());
        NameNode.getNameNodeMetrics().incrFilesCreated();
        NameNode.getNameNodeMetrics().incrCreateFileOps();
        //This might a bug cause NSQuotaExceededException
        //long increaseNS = child.computeQuotaUsage().get(Quota.NAMESPACE);
        //NameNodeDummy.getNameNodeDummyInstance().setQuota(parent.getFullPathName(), (parent.computeQuotaUsage().get(Quota.NAMESPACE) + increaseNS), parent.computeQuotaUsage().get(Quota.DISKSPACE));

      }
      return;
    }
    boolean isLoop = false;
    if (temp != null) {
      // If child exist, compare if they have difference, if not , directly return;

      //LOG.info(temp.computeQuotaUsage().get(Quota.NAMESPACE) +" vs " + child.computeQuotaUsage().get(Quota.NAMESPACE) + "Found path existing , ignore "
      //  + child.getFullPathName());
      parent = (temp.isDirectory() ? temp.asDirectory() : parent);
      isLoop = true;
    } else {

      //System.out.println("====" + child.getId()
      //+ ";child.getParent.getFullPathName()=" + child.getParent().getFullPathName()
      //  + ";child.getGroupName()=" + child.getFullPathName());
      /** Ignore Namespace (servername) **/
      //if (child.getId() == 1&&child.getLocalName().startsWith(PREFIX)) {
      //isLoop = true;
      //} else {
      if (child.getLocalName().endsWith(
          NameNodeDummy.getNameNodeDummyInstance().getNamenodeAddress()
              .getHostName())) {
        System.out.println("[INodeServer] Found transfer metadata back again:"
            + child.getFullPathName());
        isLoop = true;
      } else {
        parent.addChild(child);
        //Reset parent
        //child.setParent(parent);
        this.addChildren(child);
        if (!INodeServer.isTest) {
          //NameNode.getNameNodeMetrics().incrFilesCreated();
          //NameNode.getNameNodeMetrics().incrCreateFileOps();
          // This might caused a bug of NSQuotaExceededException

          //long increaseNS = child.computeQuotaUsage().get(Quota.NAMESPACE);
          //NameNodeDummy.getNameNodeDummyInstance().setQuota(parent.getFullPathName(), (parent.computeQuotaUsage().get(Quota.NAMESPACE) + increaseNS), parent.computeQuotaUsage().get(Quota.DISKSPACE));         

        }
        return;
      }

    }
    if (isLoop) {
      ReadOnlyList<INode> roList =
          child.asDirectory().getChildrenList(Snapshot.CURRENT_STATE_ID);
      Iterator<INode> ite = roList.iterator();
      //for (int i = 0; i < roList.size(); i++) {
      while (ite.hasNext()) {
        recursiveAddNode(ite.next(), parent);
      }
    }
  }

  public static void addINodeToMap(INode inode) {
    NameNodeDummy.getNameNodeDummyInstance().getFSNamesystem().getFSDirectory()
        .addToInodeMap(inode);
  }

}


class ClearMapByHost extends TimerTask {
  
  private String hostname;
  public ClearMapByHost(String host) {
    this.hostname = host;
  }

  private void clear() {
    Map map = UpdateNIOData.getServersMap().get(hostname);
    if (map != null) {
      System.out.println("Clear server map memory!");
      map.clear();
    }
    map = null;
  }

  public void run() {
    this.clear();
  }
}