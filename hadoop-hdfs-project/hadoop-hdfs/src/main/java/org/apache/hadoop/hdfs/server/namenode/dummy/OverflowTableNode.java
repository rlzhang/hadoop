package org.apache.hadoop.hdfs.server.namenode.dummy;

import org.apache.hadoop.hdfs.server.namenode.NameNodeDummy;

public class OverflowTableNode {

  public String key;
  private ExternalStorage value;

  public OverflowTableNode parent;
  public OverflowTableNode left, right; // children
  private  RedBlackBST rb = null;

  private boolean isRedBlackNode = false;
  
  public OverflowTableNode(String path) {
    this.key = path;
  }
  public OverflowTableNode(String path, ExternalStorage es, OverflowTableNode parent, boolean isRedBlackNode) {
    this(path, es, parent);
    this.isRedBlackNode = isRedBlackNode;  
  }
  public OverflowTableNode(String path, ExternalStorage es, OverflowTableNode parent) { // constructor
    if (NameNodeDummy.DEBUG)
      if (this.key == null || "".equals(key.trim())) {
        NameNodeDummy
            .debug("[OverflowTableNode] why do you want to create a node with empty key? key = "
                + key + "; value = " + value);
      }
    this.key = path;
    this.value = es;
    this.parent = parent;
    if (path == null) {
      this.rb = new RedBlackBST();
    }
  }

  public ExternalStorage getValue() {
    return value;
  }

  public void setValue(ExternalStorage value) {
    this.value = value;
  }
  public void setParent(OverflowTableNode parent) {
    this.parent = parent;
  }

  public RedBlackBST getRb() {
    return rb;
  }

  public boolean isRedBlackNode() {
    return isRedBlackNode;
  }

  public void setRedBlackNode(boolean isRedBlackNode) {
    this.isRedBlackNode = isRedBlackNode;
  }
}
