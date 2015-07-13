package org.apache.hadoop.hdfs.server.namenode.dummy;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.jsp.JspWriter;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeDummy;
import org.apache.hadoop.hdfs.server.namenode.Quota;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

/**
 * 
 * @author Ray Zhang
 *
 */
public class SplitTree {
  private AtomicInteger index = new AtomicInteger(0);
  //private List<MapRequest> list = new ArrayList<MapRequest>();
  private CallBack callback;
  private JspWriter out;
  private long quota;
  private MapRequest[] list;
  private int i = 0;
  
//  public SplitTree(CallBack callback, JspWriter out) {
//    this.register(callback, out);
//  }
  public void register(CallBack callback, JspWriter out) {
    this.callback = callback;
    this.out = out;
  }
  /**
   * Split big tree structure to nodes or small sub-tree
   * @param inode
   * @param size Decide how and when to split a tree
   * @param parent
   */
//  public void splitToSmallTree(INode inode, int parent) {
//    int myID = this.increaseIndex();
//    list.add(myID, new MapRequest(myID, parent, inode));
//    //inode.setParent(null);
//    if (!inode.isDirectory())
//      return;
//
//    ReadOnlyList<INode> roList =
//        inode.asDirectory().getChildrenList(Snapshot.CURRENT_STATE_ID);
//
//    Iterator<INode> ite = roList.iterator();
//    //for (int i = 0; i < roList.size(); i++) {
//    while (ite.hasNext()) {
//      splitToSmallTree(ite.next(), myID);
//    }
//
//    //inode.asDirectory().clearChildren();
//  }

  public long getQuota(INode inode) {
    ContentSummary cs = inode.computeContentSummary();
    quota = cs.getDirectoryCount() + cs.getFileCount();
    //quota = inode.computeQuotaUsage().get(Quota.NAMESPACE);
    System.out.println("Quota is " + quota);
    remain = quota;
    return quota;
  }
  
  private void getSendArray(long size) {
    //MapRequest[] sendArray = null;
    list = null;
    if (size > INodeServer.MAX_GROUP){
      list = new MapRequest[INodeServer.MAX_GROUP];
    } else {
      list = new MapRequest[(int) size];
    }

    //if (NameNodeDummy.TEST)
    //System.out.println("Group size is " + sendArray.length);
    //return sendArray;
  }
  /**
   * Split big tree structure to each nodes or small sub-tree (On building...)
   * @param inode
   * @param size Decide how and when to split a tree
   * @param parent
   */
  public synchronized boolean intelligentSplitToSmallTree(INode inode, long size, int parent) {

    this.getSendArray(quota);
    boolean ifSuccess =
     this.intelligentSplitToSmallTreeBase(inode, size, parent, false);
    list = null;
    return ifSuccess;
    //if(ifSuccess)
    //map.get(new Integer(0)).getInode().asDirectory().clearChildren();
  }

  
  long remain;
  private boolean sendData(int size){
    boolean suc = true;
    if (size == list.length) {
      try {
        //System.out.println("Send data size is " + size);
        this.callback.sendTCP(list, out);
        remain = remain - list.length;
        i = 0;
        this.getSendArray(remain);
        
     } catch (Exception e) {
       //e.printStackTrace();
       suc = false;
       System.err.println(e.getMessage());
       
     }
    }
    return suc;
  }
  /**
   * Basic function.
   * @param inode
   * @param size
   * @param parent
   * @param ifBreak
   * @return
   */
  private synchronized boolean intelligentSplitToSmallTreeBase(INode inode, long size,
      int parent, boolean ifBreak) {
    //boolean ifSuccess = false;
    if (remain == 0) return true;
    int myID = this.increaseIndex();
    if (NameNodeDummy.DEBUG)
      System.out.println("localname:" + inode.getLocalName());
    //list.add(new MapRequest(myID, parent, inode));
    list[i++] = new MapRequest(myID, parent, inode);
    boolean isSuc = this.sendData(i);
    if (!isSuc) {
      System.err.println("Failed to send " + inode.getFullPathName());
      return false;
    }
    /**
    int splitSize = (int) ((quota / size) + 1);
    if (NameNodeDummy.DEBUG)
      System.out.println("Split to " + splitSize + "; metadata size is "
          + quota);
    if (splitSize > 1) {
      // Have handled in Kryo
      //inode.setParent(null);
    } else
      return ifSuccess;
   **/
    if (ifBreak || !inode.isDirectory())
      return true;

    ReadOnlyList<INode> roList =
        inode.asDirectory().getChildrenList(Snapshot.CURRENT_STATE_ID);
    //if (checkSize(splitSize, roList.size())) {
    //Iterator<INode> ite = roList.iterator();
    for (int i = 0; i < roList.size(); i++) {
   // while (ite.hasNext()) {
      intelligentSplitToSmallTreeBase(roList.get(i), size, myID, false);
    }
    //}

    return true;
  }

  /**
   * Decide how to split the tree.
   * @param expectSize
   * @param currentSize
   * @return
   */
  private boolean checkSize(int expectSize, int currentSize) {
    boolean pass = false;
    int minimum = (int) (expectSize * 0.8);
    System.out.println(currentSize + "=currentSize;Minimum match " + minimum);
    if (minimum <= currentSize)
      pass = true;
    System.out.println(minimum + "checkSize:" + pass);
    return pass;
  }

  /**
  public void printMap(Map<Integer, SubTree> map) {
    Iterator<Entry<Integer, SubTree>> iter = map.entrySet().iterator();
    while (iter.hasNext()) {
      Entry<Integer, SubTree> entry = iter.next();
      Integer id = entry.getKey();
      SubTree sub = entry.getValue();
      System.out.println(id + ":" + sub.getParentID() + ":"
          + sub.getInode().getLocalName());
    }
  }
  **/
  /**
   * Merge back the divided sub-tree on the target namenode.
   * @param map
   * @return
   */
  public INode mergeListToINode(Map<Integer, MapRequest> map) {
    if (map.isEmpty())
      return null;
    INode root = map.get(0).getInode();
    //System.out.println("[mergeListToINode]Root dir:" + root.getFullPathName());
    Iterator<Entry<Integer, MapRequest>> iter = map.entrySet().iterator();
    while (iter.hasNext()) {
      Entry<Integer, MapRequest> entry = iter.next();
      Integer id = entry.getKey();
      MapRequest sub = entry.getValue();
      if (id.intValue() != 0)
        map.get(sub.getParentID()).getInode().asDirectory()
            .addChild(sub.getInode());
      if (NameNodeDummy.DEBUG)
        System.out.println("[mergeListToINode]Merge file "
            + sub.getInode().getLocalName());
      if (NameNodeDummy.DEBUG)
        if (sub.getInode().isFile()) {
          System.out.println("File name is "
              + sub.getInode().asFile().getFullPathName());
        }
    }
    return root;
  }

//  public List<MapRequest> getSplittedNodes() {
//    return list;
//  }

  public int getIndex() {
    return index.get();
  }

  public int increaseIndex() {
    return this.index.getAndIncrement();
  }

}
