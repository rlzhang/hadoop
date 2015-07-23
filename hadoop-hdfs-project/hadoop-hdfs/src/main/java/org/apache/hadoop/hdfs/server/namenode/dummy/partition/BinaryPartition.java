package org.apache.hadoop.hdfs.server.namenode.dummy.partition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.TreeMap;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.NameNodeDummy;
import org.apache.hadoop.hdfs.server.namenode.Quota;
import org.apache.hadoop.hdfs.server.namenode.dummy.INodeServer;
import org.apache.hadoop.hdfs.server.namenode.dummy.ToMove;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

/**
 * Automatically move metadata.
 * @author rzhan33
 *
 */
public class BinaryPartition {

  private final static int MB = 1024 * 1024;
  //150 bytes
  private final static int EACH_NODE_SIZE = 150;
  /**
   * If name node capacity less than threshhold, will trigger automatically
   * partition.
   */
  private final static double THRESHOLD = 0.30;

  /**
   * How much percentage at least expect to move out, for knapsack problem.
   */
  //private final static double TARGETMOVE = 0.10;

  //Default half of heap memory size
  private final static double MAX_MOVE_SPACE = 0.5;
  /**
   * Give each target name node space to grow after moving namespace
   */
  private final static double GROWSPACE = 0.10;

  /**
   * Prepare at least has this number of free space, after source name node moved namespace out.
   */
  private final static double FREESPACE = THRESHOLD + GROWSPACE;

  private final static int MAX_LEVEL = 20;

  NamenodeTable nt;

  /**
   * If have to kick off moving process.
   * @param nt
   * @return
   */
  public boolean ifStartOld(NamenodeTable nt) {
    return (nt.getFreeCapacity() < nt.getTotalCapacity() * THRESHOLD) ? true
        : false;
  }
  
  
  public boolean ifStart(NamenodeTable nt) {
    long totalSize = getTotalNamespaceSize(); 
    return (totalSize >= nt.getTotalCapacity() * THRESHOLD) ? true
        : false;
  }

  public BinaryPartition(NamenodeTable nt) {
    this.nt = nt;
  }

  public BinaryPartition() {

  }

  /**
   * Get a set of external namespace subtree to move out
   * @param external
   * @param totalWeight
   */
  private List<INodeDirectory> knapsackCluster(List<INodeDirectory> external,
      int totalWeight) {

    Knapsack[] bags = new Knapsack[external.size()];
    int defaultValue = 1;
    for (int i = 0; i < bags.length; i++) {
      /**
       * @ToDos has to find a way convert long safely to int
       */

      bags[i] =
          new Knapsack(defaultValue, longToInt(this.getINodeSize(external
              .get(i))), external.get(i));
    }
    KnapsackProblem kp = new KnapsackProblem(bags, totalWeight);
    kp.solve();
    List<Knapsack> kps = kp.getBestSolution();
    int sizeToMove = kp.getBestValue();
    boolean check = this.ifGoodOnSourceNN(nt, sizeToMove, FREESPACE);
    if (check) {
      return this.knapsackToINodeDirectory(kps);
    }
    return null;
  }

  private List<INodeDirectory> knapsackToINodeDirectory(List<Knapsack> kps) {
    List<INodeDirectory> returnValue = new ArrayList<INodeDirectory>();
    for (int i = 0; i < kps.size(); i++) {
      returnValue.add(kps.get(i).getDir());
    }
    return returnValue;
  }

  /**
   * Change bytes to MB.
   * We assume metadata size in any Namenode never exceed Integer.MAX_VALUE Megabytes.
   * @param l
   * @return
   */
  private int longToInt(long l) {
    int t = 0;
    t = (int) (l / (1024 * 1024));
    return t;
  }

  /**
   * Check if has external name node namespace can be divided out, in order to
   * better balance and reduce overflow table.
   */
  public List<ToMove> preDecision(Map<String, NamenodeTable> map,
      INodeDirectory root) {
    List<ToMove> returnValue = new ArrayList<ToMove>();
    List<INodeDirectory> external = new ArrayList<INodeDirectory>();
    assert (root != null);
    ReadOnlyList<INode> roList =
        root.getChildrenList(Snapshot.CURRENT_STATE_ID);
    for (int i = 0; i < roList.size(); i++) {
      INode unknow = roList.get(i);
      if (unknow.isDirectory()) {
        if (unknow.getLocalName().startsWith(INodeServer.PREFIX)
            && unknow.getUserName().equals(INodeServer.DUMMY)) {
          external.add(unknow.asDirectory());
        }
      }
    }
    nt = map.get(NameNodeDummy
        .getNameNodeDummyInstance().getNamenodeAddress().getHostName());
    if (nt == null) {
      System.err.println("Cannot run pre decision, NamenodeTable is null!");
      return null;
    }
    external =
        this.knapsackCluster(external,
            (int) (nt.getTotalCapacity() * GROWSPACE));
    if (external == null)
      return null;
    // map = this.sortMap(map);

    // add ToMove wrapper here.
    /**
     * Check if can move back to original one
     */
    for (int i = 0; i < external.size(); i++) {
      INodeDirectory inode = external.get(i);
      String src = inode.getLocalName();
      src = src.substring(INodeServer.PREFIX.length(), src.length());
      NamenodeTable nt = map.get(src);
      if (nt == null) {
        System.err.println("Unknow error! Cannot find namenode address " + src);
        continue;
      }
      // Check if good to move out
      boolean couldMove = this.preDecisionInt(nt, this.getINodeSize(inode));
      System.out.println((couldMove == true ? "You can " : "You cannot ")
          + "move out " + inode.getFullPathName());
      if (!couldMove) {
        // Cannot move back, find another namenode.
        nt = this.getMaxCapacityNamenode(map);
        couldMove = this.preDecisionInt(nt, this.getINodeSize(inode));
      }
      if (couldMove) {
        ToMove to = new ToMove();
        to.setDir(inode);
        to.setTargetNN(nt);
        returnValue.add(to);
      } else {
        System.err.println("Unknow error! Cannot find a namenode for " + src);
      }
    }
    return returnValue;
  }

  /**
   * Check if good to move the external namespace to its original name node.
   * 
   * @param map
   *            In memory name node capacity table
   * @param nt
   *            Which name node is the target
   * @param sizeToMove
   *            What's the size want to move out
   */
  private boolean preDecisionInt(NamenodeTable nt, long sizeToMove) {

    return this.ifHasEnoughCapacityOnTargetNamenode(nt, sizeToMove, GROWSPACE);

  }

  /**
   * For example: If threshold set up is 30% free space to trigger movement,
   * then we allow 10% grow space after moved metadata, so the free space on
   * target namenode should more than 40% after moved metadata over.
   * 
   * @param nt The target name node.
   * @param sizeToMove
   * @param growSpace
   * @return
   */
  private boolean ifHasEnoughCapacityOnTargetNamenode(NamenodeTable nt,
      long sizeToMove, double growSpace) {
    System.out
        .println("[ifHasEnoughCapacityOnTargetNamenode] The target name node server: expect free space "
            + (THRESHOLD + growSpace)
            * nt.getTotalCapacity()
            + "; after move in "
            + (nt.getFreeCapacity() - sizeToMove)
            + "; "
            + nt.getNamenodeServer());
    return ((THRESHOLD + growSpace) * nt.getTotalCapacity() <= (nt
        .getFreeCapacity() - sizeToMove)) ? true : false;
  }
  
  private long getTotalNamespaceSize() {
    long totalSize = (long) ( NameNodeDummy.getNameNodeDummyInstance().getCountOfFilesDirectoriesAndBlocks() * EACH_NODE_SIZE / MB);
    return totalSize;
  }

  /**
   * Check if match minimum requirement to move
   * After move data, we should keep the source name node memory under some percentage, as like 40% free space.
   * But you don't want to move to much out, as like more than half of namespace?
   * @param nt The source name node.
   * @param sizeToMove
   * @param growSpace
   * @return
   */
  private boolean ifGoodOnSourceNN(NamenodeTable nt, long sizeToMove,
      double freeSpace) {
   // long after = (nt.getFreeCapacity() + sizeToMove);
    long totalSize = getTotalNamespaceSize();
    System.out
        .println("[ifGoodOnSourceNN] The source name node server: Plan to move "
            + sizeToMove
            + "; expect free space "
            + nt.getTotalCapacity() * freeSpace + ";" + "Cannot large than " + (totalSize * MAX_MOVE_SPACE));
    //System.out.println("[ifGoodOnSourceNN]  Cannot large than " + nt.getTotalCapacity()/2 + ", size to move out " + sizeToMove);
   //Old one calculate heap memory
    //return (after > nt.getTotalCapacity() * freeSpace && sizeToMove < (nt
      //  .getTotalCapacity() * MAX_MOVE_SPACE)) ? true : false;
    //Calculate namespace size
    return (sizeToMove > nt.getTotalCapacity() * GROWSPACE && sizeToMove < (totalSize * MAX_MOVE_SPACE)) ? true : false;
  }

  /**
   * If pre-decision failed, will go to here This method will try to divide a
   * tree by three type of ways
   */
  public ToMove divideOriginalTree(Map<String, NamenodeTable> map,
      INodeDirectory root, String thisServer) {
    // Check if type one matches
    ToMove type1 =
        this.divideOriginalTreeInt1(map, root, thisServer, GROWSPACE, FREESPACE);
    System.out.println(" --- Type one partitioning find "
        + (type1 == null ? "" : type1.getDir().getFullPathName()));
    System.out.println();
    if (type1 != null && type1.getDir() != null)
      return type1;
    
    // Test only!
    if (true){
      System.out.println("Nothing to move!");
      return null;
    }
      
    // Check if type two matches
    Pairs p = this.divideOriginalTreeInt2(map, root, thisServer);
    if (p == null)
      return null;
    System.out.println(" --- Type two partitioning find "
        + ((p == null || p.inode == null) ? "" : p.inode.getFullPathName())
        + "; split from "
        + (p != null ? (p.isStartFromLeft == true ? "left" : "right") : ""));
    System.out.println();
    ToMove tm = new ToMove();
    tm.setDir(p.inode);
    tm.setType(2);
    tm.setStartFromLeft(p.isStartFromLeft);
    tm.setTargetNN(this.getMaxCapacityNamenode(map));
    //tm.setQueue(p.queue);
    tm.setQueue(p.allQueue);
    if (p.inode != null)
      return tm;
    // Check if type three matches
    ToMove type3 =
        this.divideOriginalTreeInt1(map, root, thisServer, 0.05, 0.32);
    System.out.println(" --- Type three partitioning find "
        + (type3 == null ? "" : type3.getDir().getFullPathName()));
    System.out.println();
    return tm;
  }

  /**
   * Type one, check if can divide a sub-tree.
   * 
   * @param map
   * @param root
   * @return
   */
  @Deprecated
  private ToMove divideOriginalTreeWayOne(Map<String, NamenodeTable> map,
      INodeDirectory root, String thisServer, double targetNNGrow,
      double sourceNNFree) {
    ToMove tm = new ToMove();
    ReadOnlyList<INode> roList =
        root.getChildrenList(Snapshot.CURRENT_STATE_ID);
    Queue<INodeDirectory> queue = new LinkedList<INodeDirectory>();
    for (int i = 0; i < roList.size(); i++) {
      INode inode = roList.get(i);
      if (inode.isDirectory()) {

        NamenodeTable max = this.getMaxCapacityNamenode(map);
        if (this.ifHasEnoughCapacityOnTargetNamenode(max,
            this.getINodeSize(inode), targetNNGrow)
            && this.ifGoodOnSourceNN(map.get(thisServer),
                this.getINodeSize(inode), sourceNNFree)) {
          tm.setDir(inode.asDirectory());
          tm.setTargetNN(max);
          break;
        } else {
          this.addChildrenAsDir(inode.asDirectory(), queue);
        }
      }
    }
    
    
    return tm;
  }

  /**
   * Add sub-directories.
   * @param dir
   * @param queue
   */
  private void addChildrenAsDir(INodeDirectory dir, Queue<INodeDirectory> queue) {
    ReadOnlyList<INode> roList = dir.getChildrenList(Snapshot.CURRENT_STATE_ID);
    for (int i = 0; i < roList.size(); i++) {
      INode inode = roList.get(i);
      if (inode.isDirectory()) {
        queue.add(inode.asDirectory());
      }
    }
  }

 
  private ToMove divideOriginalTreeInt1(Map<String, NamenodeTable> map,
      INodeDirectory root, String thisServer, double targetNNGrow,
      double sourceNNFree) {
    ToMove tm = new ToMove();
    ReadOnlyList<INode> roList =
        root.getChildrenList(Snapshot.CURRENT_STATE_ID);
    Queue<INodeDirectory> queue = new LinkedList<INodeDirectory>();
    for (int i = 0; i < roList.size(); i++) {
      INode inode = roList.get(i);
      if (inode.isDirectory()) {
        NamenodeTable max = this.getMaxCapacityNamenode(map);
        if (this.ifHasEnoughCapacityOnTargetNamenode(max,
            this.getINodeSize(inode), targetNNGrow)
            && this.ifGoodOnSourceNN(map.get(thisServer),
                this.getINodeSize(inode), sourceNNFree)) {
          queue.clear();
          queue = null;
          tm.setDir(inode.asDirectory());
          tm.setTargetNN(max);
          return tm;
        } else {
          //queue.add(inode.asDirectory());
          this.addChildrenAsDir(inode.asDirectory(), queue);
        }
      }
    }
    return this.divideOriginalTreeInt1Int(map, queue, 1, thisServer,
        targetNNGrow, sourceNNFree);
  }

  private ToMove divideOriginalTreeInt1Int(Map<String, NamenodeTable> map,
      Queue<INodeDirectory> queue, int level, String thisServer,
      double targetNNGrow, double sourceNNFree) {
    ToMove tm = new ToMove();
    if (level > MAX_LEVEL) {
      queue.clear();
      queue = null;
      return null;
    }

    Queue<INodeDirectory> queue2 = new LinkedList<INodeDirectory>();
    while (queue.peek() != null) {
      INodeDirectory inode = queue.poll();
      if (inode.isDirectory()) {
        NamenodeTable max = this.getMaxCapacityNamenode(map);
        if (this.ifHasEnoughCapacityOnTargetNamenode(max,
            this.getINodeSize(inode), targetNNGrow)
            && this.ifGoodOnSourceNN(map.get(thisServer),
                this.getINodeSize(inode), sourceNNFree)) {
          queue2.clear();
          queue.clear();
          queue2 = null;
          queue = null;
          tm.setDir(inode);
          tm.setTargetNN(max);
          return tm;
        } else {
          //queue2.add(inode);
          this.addChildrenAsDir(inode.asDirectory(), queue2);
        }
      }
    }
    queue.clear();
    queue = null;
    return this.divideOriginalTreeInt1Int(map, queue2, level + 1, thisServer,
        targetNNGrow, sourceNNFree);

  }

  /**
   * Find start and end point to matches this scenario, the sum of serial inodes size less or equal than start and large or equal than end.
   * @param map
   * @param start
   * @param end
   */
  private void getSum(TreeMap<Integer, INode> map, long start, long end) {
    if (start < 0) {
      System.err.println("Something is wrong, start cannot be " + start);
      start = 0;
    }
    System.out.println("[getSum] Allow to move metadata cannot less than "
        + start + " and cannot large than " + end);
    List<INode> list = new ArrayList<INode>(map.values());
    for (int i = 0; i < list.size(); i++) {
      long sum = 0;
      for (int j = i; j < list.size(); j++) {
        if (sum >= start && sum <= end) {
          System.out.println("begin from " + i + ";end to " + (j - 1) + ";sum="
              + sum);
          for (int k = 0; k < list.size(); k++) {
            System.out.println(k + ":" + this.getINodeSize(list.get(k)));
          }

          System.out.println("---------End!");
          break;
        }
        sum += this.getINodeSize(list.get(j));
      }
    }

  }

  /**
   * Type two, check if can divide a batch of subtree by hash id.
   * @param map
   * @param root
   * @return
   */
  private Pairs divideOriginalTreeInt2(Map<String, NamenodeTable> map,
      INodeDirectory root, String thisServer) {
    System.out.println("Trying way 2 to divide tree...");
    Pairs p = this.divideOriginalTreeInt2IntInt(map, root, thisServer);
    if (p.inode != null)
      return p;
    return this.divideOriginalTreeInt2Int(map, p.queue, 1, thisServer);
  }

  private Pairs divideOriginalTreeInt2IntInt(Map<String, NamenodeTable> map,
      INodeDirectory root, String thisServer) {
    ReadOnlyList<INode> roList =
        root.getChildrenList(Snapshot.CURRENT_STATE_ID);
    Pairs p = new Pairs();
    Queue<INode> queue = new LinkedList<INode>();
    Queue<INodeDirectory> dirQueue = new LinkedList<INodeDirectory>();
    TreeMap<Integer, INode> hashs = new TreeMap<Integer, INode>();
    for (int i = 0; i < roList.size(); i++) {
      INode inode = roList.get(i);
      hashs.put(inode.getLocalName().hashCode(), inode);
      if (inode.isDirectory())
        dirQueue.add(inode.asDirectory());
    }
    /**
     * Check size from left to right
     */
    INode node = this.calculateINodeSide(hashs, queue, map, thisServer);
    if (node != null) {
      p.inode = node;
      p.isStartFromLeft = true;
      p.allQueue = queue;
      dirQueue.clear();
      return p;
    }
    NamenodeTable cur = map.get(thisServer);
    //How much have to move out from source name node
    long start =
        (long) (cur.getTotalCapacity() * FREESPACE) - cur.getFreeCapacity();

    //How much can accept on target name node
    NamenodeTable tar = this.getMaxCapacityNamenode(map);
    long end =
        (long) (tar.getFreeCapacity() -
            tar.getTotalCapacity() * FREESPACE);
    this.getSum(hashs, start, end);
    /**
     * Check size from right to left, don't need for now.
     */
    
    TreeMap<Integer,INode> reversed = new TreeMap<Integer,INode>(Collections.reverseOrder());
    reversed.putAll(hashs);
    System.out.println(" --------------- after reversed the order --------------------- ");
    node = this.calculateINodeSide(reversed, queue, map, thisServer);
    reversed.clear();
    if(node!=null){
    	p.inode = node;
    	p.isStartFromLeft = false;
    	p.allQueue = queue;
    	return p;
    }
    
    p.queue = dirQueue;
    return p;
  }

  /**
   * Calculate the same level files size either from left to right or from right to left.
   * @param hashs
   * @param queue
   * @param map
   * @param thisServer
   * @return
   */
  private INode calculateINodeSide(TreeMap<Integer, INode> hashs,
      Queue<INode> queue, Map<String, NamenodeTable> map,
      String thisServer) {
    long size = 0;
    for (INode inode : hashs.values()) {
      size += this.getINodeSize(inode);
      if (this.ifGoodOnSourceNN(map.get(thisServer), size, FREESPACE)) {
        //if(this.ifGoodOnSourceNN(map.get(thisServer), size, FREESPACE)){
        if (this.ifHasEnoughCapacityOnTargetNamenode(
            this.getMaxCapacityNamenode(map), size, GROWSPACE)) {
          queue.add(inode);
          return inode;
        } else {
          //ifLarge = true;
          queue.add(inode);
          break;
        }
      }
    }
    return null;
  }

  private Pairs divideOriginalTreeInt2Int(Map<String, NamenodeTable> map,
      Queue<INodeDirectory> queue, int level, String thisServer) {
    if (level > MAX_LEVEL) {
      queue.clear();
      return null;
    }

    Queue<INodeDirectory> queue2 = new LinkedList<INodeDirectory>();
    while (queue.peek() != null) {
      INodeDirectory inode = queue.poll();
      Pairs p = this.divideOriginalTreeInt2IntInt(map, inode, thisServer);
      if (p.inode != null) {
        queue.clear();
        queue2.clear();
        return p;
      }
      Queue<INodeDirectory> q = p.queue;
      queue2.addAll(q);
    }
    queue.clear();
    return this.divideOriginalTreeInt2Int(map, queue2, level + 1, thisServer);

  }

  /**
   * Create a entry here, in case will change calculate method in future.
   * Each INode has 150 bytes
   * @param inode
   * @return
   */
  private long getINodeSize(INode inode) throws ConcurrentModificationException {
    long size = -1;
    try {
      ContentSummary cs = inode.computeContentSummary();
      //size = inode.computeQuotaUsage().get(Quota.NAMESPACE);
      // This part didn't calculate blocks size, should fix later.
      long blocks = NameNodeDummy.getNameNodeDummyInstance().numBlocks(inode);
      size = cs.getDirectoryCount() + cs.getFileCount() + blocks;
      System.out.println("INode " + inode.getFullPathName() + ": blocks:"
          + blocks + ": getLength = " + cs.getLength() + ": getQuota = "
          + cs.getQuota() + ": getSpaceConsumed = " + cs.getSpaceConsumed()
          + ": getSpaceQuota = " + cs.getSpaceQuota() + ": getFileCount = "
          + cs.getFileCount() + ": getDirectoryCount = "
          + cs.getDirectoryCount());
      size = (long) (size * EACH_NODE_SIZE / MB);
      System.out.println("Inode " + inode.getFullPathName() + " size(MB) is "
          + size);
    } catch (ConcurrentModificationException e) {
      System.err.println("Cannot getINodeSize. Namenode is busy."
          + e.getMessage());
      throw e;
    } catch (Exception e) {
      System.err.println("Cannot getINodeSize." + e.getMessage());
    }

    return size;
  }

  private NamenodeTable getMaxCapacityNamenode(Map<String, NamenodeTable> map) {
    if (map == null || map.size() == 0)
      return null;
    List<Entry<String, NamenodeTable>> sortedList = this.sortMap(map);
    return sortedList.get(0).getValue();

  }

  public List<Entry<String, NamenodeTable>> sortMap(
      Map<String, NamenodeTable> map) {
    List<Entry<String, NamenodeTable>> list =
        new ArrayList<Entry<String, NamenodeTable>>(map.entrySet());
    Collections.sort(list, new Comparator<Map.Entry<String, NamenodeTable>>() {
      public int compare(Map.Entry<String, NamenodeTable> o1,
          Map.Entry<String, NamenodeTable> o2) {
        long compare =
            o2.getValue().getFreeCapacity() - o1.getValue().getFreeCapacity();
        if (compare == 0)
          return 0;
        else if (compare < 0)
          return -1;
        else
          return 1;
      }
    });
    return list;
  }

  class Pairs {
    INode inode = null;
    Queue<INodeDirectory> queue = null;
    Queue<INode> allQueue = null;
    boolean isStartFromLeft = true;
  }

}
