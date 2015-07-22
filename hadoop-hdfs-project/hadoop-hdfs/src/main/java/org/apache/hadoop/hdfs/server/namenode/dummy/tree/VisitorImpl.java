package org.apache.hadoop.hdfs.server.namenode.dummy.tree;

public abstract class VisitorImpl<T, R> implements Visitor<T, R> {

  protected R result;

  public VisitorImpl() {
    this.result = null;
  }

  public VisitorImpl(R initialValue) {
    this.result = initialValue;
  }

  @Override
  public R getResult() {
    return result;
  }

  @Override
  abstract public void visit(String key, RadixTreeNode<T> parent,
      RadixTreeNode<T> node);

}