package org.apache.hadoop.hdfs.server.namenode.dummy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

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
  private List<MapRequest> list = new ArrayList<MapRequest>();

  /**
   * Split big tree structure to nodes or small sub-tree
   * @param inode
   * @param size Decide how and when to split a tree
   * @param parent
   */
  public void splitToSmallTree(INode inode, int parent) {
    int myID = this.increaseIndex();
    list.add(myID, new MapRequest(myID, parent, inode));
    //inode.setParent(null);
    if (!inode.isDirectory())
      return;

    ReadOnlyList<INode> roList =
        inode.asDirectory().getChildrenList(Snapshot.CURRENT_STATE_ID);

    Iterator<INode> ite = roList.iterator();
    //for (int i = 0; i < roList.size(); i++) {
    while (ite.hasNext()) {
      splitToSmallTree(ite.next(), myID);
    }

    //inode.asDirectory().clearChildren();
  }

  /**
   * Split big tree structure to each nodes or small sub-tree (On building...)
   * @param inode
   * @param size Decide how and when to split a tree
   * @param parent
   */
  public void intelligentSplitToSmallTree(INode inode, long size, int parent) {

    boolean ifSuccess =
        this.intelligentSplitToSmallTreeBase(inode, size, parent, false);
    //if(ifSuccess)
    //map.get(new Integer(0)).getInode().asDirectory().clearChildren();
  }

  /**
   * Basic function.
   * @param inode
   * @param size
   * @param parent
   * @param ifBreak
   * @return
   */
  private boolean intelligentSplitToSmallTreeBase(INode inode, long size,
      int parent, boolean ifBreak) {
    boolean ifSuccess = false;
    int myID = this.increaseIndex();
    if (NameNodeDummy.DEBUG)
      System.out.println("localname:" + inode.getLocalName());
    list.add(new MapRequest(myID, parent, inode));
    long quota = inode.computeQuotaUsage().get(Quota.NAMESPACE);
    int splitSize = (int) ((quota / size) + 1);
    if (NameNodeDummy.DEBUG)
      System.out.println("Split to " + splitSize + "; metadata size is "
          + quota);
    if (splitSize > 1) {
      // Have handled in Kryo
      //inode.setParent(null);
    } else
      return ifSuccess;

    if (ifBreak || !inode.isDirectory())
      return true;

    ReadOnlyList<INode> roList =
        inode.asDirectory().getChildrenList(Snapshot.CURRENT_STATE_ID);
    //if (checkSize(splitSize, roList.size())) {
    Iterator<INode> ite = roList.iterator();
    //for (int i = 0; i < roList.size(); i++) {
    while (ite.hasNext()) {
      intelligentSplitToSmallTreeBase(ite.next(), size, myID, false);
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

  public List<MapRequest> getSplittedNodes() {
    return list;
  }

  public int getIndex() {
    return index.get();
  }

  public int increaseIndex() {
    return this.index.getAndIncrement();
  }

}
