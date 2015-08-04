package org.apache.hadoop.hdfs.server.namenode.dummy;

import java.util.List;

import org.apache.hadoop.hdfs.server.namenode.dummy.tree.DuplicateKeyException;
import org.apache.hadoop.hdfs.server.namenode.dummy.tree.RadixTreeImpl;
import org.apache.hadoop.hdfs.server.namenode.dummy.tree.RadixTreeNode;


public class RadixTreeOverflowTable implements IOverflowTable<ExternalStorage, RadixTreeNode<ExternalStorage>>{


 private RadixTreeImpl<ExternalStorage> tree = new RadixTreeImpl<ExternalStorage>();

  @Override
  public void insert(String key, ExternalStorage value) {
    tree.insert(key, value);
  }

  @Override
  public boolean remove(String key) {
    return tree.delete(key);
  }

  @Override
  public RadixTreeNode<ExternalStorage> buildOrAddBST(ExternalStorage[] es) {
    if (es == null || es.length == 0) return null;
    for(int i =0; i < es.length; i++) {
      tree.insert(es[i].getPath(), es[i]);
    }
    return tree.getRoot();
  }

  @Override
  public ExternalStorage findNode(String key) {
    return tree.find(key);
  }

  @Override
  public ExternalStorage findLastMatchedNode(String key) {
    return tree.lastMatchNode(key);
  }

  @Override
  public void display() {
    tree.display(); 
  }

  @Override
  public ExternalStorage[] findAllValues(String key) {
     List<ExternalStorage> list = tree.searchPrefix(key, Integer.MAX_VALUE);
     //ExternalStorage es = tree.find(key);
//     System.out.println("Found es " + list);
//     if (es != null) list.add(es);
     if (list == null) return null;
     ExternalStorage[] temp = new ExternalStorage[list.size()];
     return list.toArray(temp);
  }

}
