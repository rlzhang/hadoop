package org.apache.hadoop.hdfs.server.namenode.dummy;


import java.util.Queue;

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.dummy.partition.NamenodeTable;

public class ToMove {

  private INode dir;
  private NamenodeTable targetNN;
  private int type = 1; //1, single subtree; 2,hash division
  private Queue<INodeDirectory> queue = null;
  private boolean isStartFromLeft = true;
  
  public INode getDir() {
    return dir;
  }
  public void setDir(INode dir) {
    this.dir = dir;
  }
  public NamenodeTable getTargetNN() {
    return targetNN;
  }
  public void setTargetNN(NamenodeTable targetNN) {
    this.targetNN = targetNN;
  }
  public int getType() {
    return type;
  }
  public void setType(int type) {
    this.type = type;
  }
  public Queue<INodeDirectory> getQueue() {
    return queue;
  }
  public void setQueue(Queue<INodeDirectory> queue) {
    this.queue = queue;
  }
  public boolean isStartFromLeft() {
    return isStartFromLeft;
  }
  public void setStartFromLeft(boolean isStartFromLeft) {
    this.isStartFromLeft = isStartFromLeft;
  }
}
