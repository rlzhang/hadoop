package org.apache.hadoop.hdfs.server.namenode.dummy.partition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.NameNodeDummy;
import org.apache.hadoop.hdfs.server.namenode.dummy.INodeServer;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

public class TestBinaryPartition {

  BinaryPartition bp = new BinaryPartition();
  NamenodeTable nt = new NamenodeTable();

  public void init() {
    nt.setFreeCapacity(30);
    nt.setTotalCapacity(100);
  }

  private static final PermissionStatus PERM = PermissionStatus
      .createImmutable("dummy", "dummy",
          FsPermission.createImmutable((short) 0));

  private static INode newINode(int n, int width) {
    byte[] name = DFSUtil.string2Bytes(String.format("n%0" + width + "d", n));
    name = (INodeServer.PREFIX + "localhost" + n).getBytes();
    return new INodeDirectory(n, name, PERM, 0L);
  }

  private static void create(int startSize, INode inode) {
    int n = 0;
    // initialize previous
    // final List<INode> previous = new ArrayList<INode>();
    for (; n < startSize; n++) {
      inode.asDirectory().addChild(newINode(n, startSize));
    }
    // return previous;
  }

  public void testIfStart() {
    nt.setFreeCapacity(300);
    nt.setTotalCapacity(1000);
    boolean ifStart = bp.ifStart(nt);

    System.out.println("[testIfStart] ifStart = " + ifStart
        + " when capacity is " + nt.getFreeCapacity());

    nt.setFreeCapacity(299);
    ifStart = bp.ifStart(nt);

    System.out.println("[testIfStart] ifStart = " + ifStart
        + " when capacity is " + nt.getFreeCapacity());

    nt.setFreeCapacity(301);
    ifStart = bp.ifStart(nt);
    System.out.println("[testIfStart] ifStart = " + ifStart
        + " when capacity is " + nt.getFreeCapacity());

  }

  public void testSortMap() {
    Map<String, NamenodeTable> map = new HashMap<String, NamenodeTable>();
    this.init();
    nt.setFreeCapacity(200);
    map.put("test1", nt);

    nt = new NamenodeTable();
    nt.setFreeCapacity(1200);
    map.put("test2", nt);

    nt = new NamenodeTable();
    nt.setFreeCapacity(300);
    map.put("test3", nt);

    nt = new NamenodeTable();
    nt.setFreeCapacity(400);
    map.put("test4", nt);

    List<Entry<String, NamenodeTable>> list = bp.sortMap(map);

    for (int i = 0; i < list.size(); i++) {
      System.out.println(list.get(i).getValue());
    }

  }

  Random r = new Random(10000);

  public void testTreeMapSort() {
    Map<Integer, INode> hashs = new TreeMap<Integer, INode>();
    for (int i = 0; i < 10; i++) {
      int key = r.nextInt(100);
      hashs.put(key, null);
      System.out.println(":" + key);
    }

    System.out.println("After sort!");
    for (Integer key : hashs.keySet()) {
      System.out.println(":" + key);
    }
  }

  public void testPreDecision() {
    INode inode = newINode(0, 2);
    create(10, inode);
    create(3, inode.asDirectory().getChildrenList(Snapshot.CURRENT_STATE_ID)
        .get(0));
    Map<String, NamenodeTable> map = new HashMap<String, NamenodeTable>();
    map.put("localhost0", nt);
    String display =
        NameNodeDummy.getNameNodeDummyInstance().printNSInfo(inode, 0, 10);
    display = display.replaceAll("&nbsp;", " ").replaceAll("<br/>", "\n");
    System.out.println(display);
    bp.preDecision(map, inode.asDirectory(), "localhost");
  }

  public void testDivideOriginalTree() {
    INode inode = newINode(0, 2);
    create(10, inode);
    create(15, inode.asDirectory().getChildrenList(Snapshot.CURRENT_STATE_ID)
        .get(1));
    create(2, inode.asDirectory().getChildrenList(Snapshot.CURRENT_STATE_ID)
        .get(6));
    Map<String, NamenodeTable> map = new HashMap<String, NamenodeTable>();

    //other name node server

    nt.setFreeCapacity(49);
    nt.setTotalCapacity(100);
    map.put("localhost0", nt);

    //This name node server
    nt = new NamenodeTable();
    nt.setFreeCapacity(30);
    nt.setTotalCapacity(100);
    map.put("localhost3", nt);
    String display =
        NameNodeDummy.getNameNodeDummyInstance().printNSInfo(inode, 0, 10);
    display = display.replaceAll("&nbsp;", " ").replaceAll("<br/>", "\n");
    System.out.println(display);
    bp.divideOriginalTree(map, inode.asDirectory(), "localhost3");
  }

  public static void main(String[] args) {
    TestBinaryPartition t = new TestBinaryPartition();
    t.init();

    /**
     * Start test
     */
    /**
    t.testIfStart();

    t.testSortMap();

    t.testTreeMapSort();

    t.testPreDecision();
    **/
    t.testDivideOriginalTree();
    //t.testPreDecision();
  }

}
