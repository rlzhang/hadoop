package org.apache.hadoop.hdfs.server.namenode.dummy;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.util.Diff;

public class MultiBranchTreeNode implements Diff.Element<byte[]> {
  private int id;
  //UTF8
  private byte[] name = null;
  private MultiBranchTreeNode parent = null;
  private List<ExternalStorage> partitions = null;
  private List<MultiBranchTreeNode> children = null;

  public MultiBranchTreeNode() {
    
  }
  
  int searchChildren(byte[] name) {
    return children == null ? -1 : Collections.binarySearch(children, name);
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public List<ExternalStorage> getPartitions() {
    return partitions;
  }

  public void setPartitions(List<ExternalStorage> partitions) {
    this.partitions = partitions;
  }

  public byte[] getName() {
    return name;
  }

  public void setName(byte[] name) {
    this.name = name;
  }

  @Override
  public int compareTo(byte[] bytes) {
    return DFSUtil.compareBytes(name, bytes);
  }

  @Override
  public byte[] getKey() {
    return name;
  }

  public MultiBranchTreeNode getParent() {
    return parent;
  }

  public void setParent(MultiBranchTreeNode parent) {
    this.parent = parent;
  }
}