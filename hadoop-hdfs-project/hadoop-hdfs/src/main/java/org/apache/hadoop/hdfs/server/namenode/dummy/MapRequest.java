package org.apache.hadoop.hdfs.server.namenode.dummy;

import org.apache.hadoop.hdfs.server.namenode.INode;

/**
 * Divide a big tree to smaller sub-trees.
 * @author Ray Zhang
 *
 */
public class MapRequest {
  
  private int id;
  private int parentID;
  private INode inode;

  public MapRequest(Integer id, int parentID, INode inode) {
    this.id = id;
    this.parentID = parentID;
    this.inode = inode;
  }

  public Integer getKey() {
    return id;
  }

  public void setKey(Integer id) {
    this.id = id;
  }

  public int getParentID() {
    return parentID;
  }

  public void setParentID(int parentID) {
    this.parentID = parentID;
  }

  public INode getInode() {
    return inode;
  }

  public void setInode(INode inode) {
    this.inode = inode;
  }

}
