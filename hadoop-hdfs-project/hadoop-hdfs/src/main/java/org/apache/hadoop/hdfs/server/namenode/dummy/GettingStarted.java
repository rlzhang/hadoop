package org.apache.hadoop.hdfs.server.namenode.dummy;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeDummy;
import org.apache.hadoop.hdfs.server.namenode.dummy.partition.BinaryPartition;
import org.apache.hadoop.hdfs.server.namenode.dummy.partition.NamenodeTable;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class GettingStarted extends Thread {

  final static int MB = 1024 * 1024;
  final static int GB = MB * 1024;

  //Minimal free memory to start moving... , MB.
  final static long MIN_MEMEORY = (long) (INodeServer.WRITE_BUFFER_MB * 1024);
  long freeMem = 0;
  Map<String, NamenodeTable> memoryMap;

  private static NamenodeTable n;

  //NameNode address or server name.
  private NameNode nn;

  public static void main(String[] args) {
    //GettingStarted g = new GettingStarted("localhost");
    //g.start();
  }

  static boolean isRun = false;

  public synchronized static void setRun(boolean run) {
    GettingStarted.isRun = run;
  }

  public GettingStarted(NameNode nn) {
    init();
    this.nn = nn;
    n = new NamenodeTable();
  }

  public static NamenodeTable getThisNamenode() {
    return n;
  }

  @Deprecated
  private static long getFreeMemory() {
    long free = Runtime.getRuntime().freeMemory();
    System.out.println("free memory: " + free / MB);
    return free;
  }

  private void init() {
    HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
    memoryMap = hazelcastInstance.getMap("freeMem");
  }

  void reportMemory() {
    //long free = getFreeMemory() / MB;
    long total = Runtime.getRuntime().maxMemory() / MB;
    MemoryMXBean mem = ManagementFactory.getMemoryMXBean();
    MemoryUsage heap = mem.getHeapMemoryUsage();
    long free = (heap.getMax() / MB) - (heap.getUsed() / MB);

    if (freeMem != free) {
      //System.out.println("Report free memory: " + getFreeMemory() / MB + "; total memory: " + total);
      System.out.println("Report used memory: " + heap.getUsed() / MB
          + " MB; max memory: " + heap.getMax() / GB + "GB, free memory is "
          + free + " MB.");
      n.setFreeCapacity(free);
      n.setTotalCapacity(heap.getMax() / MB);
      memoryMap.put(nn.getNameNodeAddress().getHostName(), n);
      freeMem = free;
    }

  }

  public void run() {
    while (nn.getRpcServer() == null) {
      // Wait namenode start
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    n.setNamenodeServer(nn.getNameNodeAddress().getHostName());
    this.reportMemory();
    Timer timer = new Timer();
    // Every minute run scan
    timer.schedule(new ReportAndMoveNSTask(this), 1000, 60 * 1000);
  }

  void printMap() {
    System.out.println("------------------Overflow Table------------------");
    for (Map.Entry<String, NamenodeTable> entry : memoryMap.entrySet()) {
      System.out.println("Key : " + entry.getKey() + " Value : "
          + entry.getValue());
    }
  }

}

/**
 * Report free memory and trigger namespace movement.
 * @author rzhan33
 *
 */
class ReportAndMoveNSTask extends TimerTask {

  GettingStarted g;
  BinaryPartition bp = new BinaryPartition();
  GettingStartedClient client = new GettingStartedClient();

  public ReportAndMoveNSTask(GettingStarted g) {
    this.g = g;
  }

  public void run() {
    g.reportMemory();
    g.printMap();
    NamenodeTable nt = GettingStarted.getThisNamenode();
    if (NameNodeDummy.isMovingRun()) {
      System.err
      .println("Another process of namespace moving is run, cancel this one! Free memory is "
          + nt.getFreeCapacity());
    }
    if (!GettingStarted.isRun && bp.ifStart(nt)) {
      GettingStarted.isRun = true;
      if (nt.getFreeCapacity() < GettingStarted.MIN_MEMEORY)
        System.err
            .println("Cannot start to move namespace, you have too small memory left. Free memory is "
                + nt.getFreeCapacity());
      else
        System.err
            .println("Low memory, start to move namespace. Free memory is "
                + nt.getFreeCapacity());
      // Pre-decision
      boolean success = this.preDecision();
      if (!success)
        success = this.divideOriginalTree();
      if (!success)
        GettingStarted.isRun = false;
      //isRun = false;
    }
    //NameNodeDummy.debug("Time's up! " + new Date().toLocaleString());
  }

  public boolean preDecision() {
    boolean returnValue = false;
    List<ToMove> moveOut =
        bp.preDecision(client.getMap(), NameNodeDummy
            .getNameNodeDummyInstance().getRoot());
    if (moveOut == null)
      return returnValue;
    for (int i = 0; i < moveOut.size(); i++) {
      ToMove to = moveOut.get(i);
      if (to.getDir() != null && to.getTargetNN() != null) {
        try {
          returnValue =
              NameNodeDummy.getNameNodeDummyInstance().moveNSAutomatically(
                  to.getDir().getFullPathName(),
                  to.getTargetNN().getNamenodeServer());
          // returnValue = true;
        } catch (IOException e) {
          e.printStackTrace();
          System.err.println("Failed to move metadata in pre-decision "
              + to.getDir().getFullPathName());
        }
      } else {
        System.err.println("Failed to move metadata in pre-decision!");
      }
    }
    return returnValue;
  }

  public boolean divideOriginalTree() {
    boolean isSuc = false;
    // Divide original tree
    ToMove tm =
        bp.divideOriginalTree(client.getMap(), NameNodeDummy
            .getNameNodeDummyInstance().getRoot(), NameNodeDummy
            .getNameNodeDummyInstance().getNamenodeAddress().getHostName());

    if (tm == null) {
      System.err
          .println("Cannot find a way to divide namespace tree, try to increase MAX_LEVEL instead !");
      return isSuc;
    }
    if (tm.getType() == 1) {
      try {
        isSuc =
            NameNodeDummy.getNameNodeDummyInstance().moveNSAutomatically(
                tm.getDir().getFullPathName(),
                tm.getTargetNN().getNamenodeServer());
      } catch (IOException e) {
        e.printStackTrace();
        System.err.println("Failed to move metadata "
            + tm.getDir().getFullPathName());
      }
    } else if (tm.getType() == 2) {
      //Queue<INodeDirectory> queue = tm.getQueue();
      Queue<INode> queue = tm.getQueue();
      Iterator<INode> ite = queue.iterator();
      while (ite.hasNext()) {
        INode dir = ite.next();
        try {
          isSuc =
              NameNodeDummy.getNameNodeDummyInstance().moveNSAutomatically(
                  dir.getFullPathName(), tm.getTargetNN().getNamenodeServer());
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      //System.err.println("Not support this type of moving yet!");
    }
    return isSuc;
  }
}