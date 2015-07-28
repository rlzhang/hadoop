package org.apache.hadoop.hdfs.dummy;

import junit.framework.TestCase;

import org.apache.hadoop.hdfs.server.namenode.NameNodeDummy;
import org.apache.hadoop.hdfs.server.namenode.dummy.ExternalStorage;

public class TestMultiBranchTreeNode extends TestCase {

  //RadixTreeOverflowTable rt = new RadixTreeOverflowTable();

  public void testBuildOrAddBST() {
    long start = System.currentTimeMillis();
    //ExternalStorage[] es = new ExternalStorage[1000];
    for (int i = 0; i < 1000000; i++) {
      String path = "/nnThroughputBenchmark/mkdirs/ThroughputBenchDir" + i;
      //path = "/a/b/" + i;
      ExternalStorage es = new ExternalStorage(i, "a.com", "poolid", path, "b.com");
      NameNodeDummy.getNameNodeDummyInstance().buildOrAddRadixBSTServer(new ExternalStorage[]{es});
    }
    //RadixTreeNode<ExternalStorage> rtn = 
    //rt.buildOrAddBST(es);
    
    //NameNodeDummy.getNameNodeDummyInstance().buildOrAddRadixBSTServer(es);
    
    System.out.println("Spend " + (System.currentTimeMillis() - start)
        + " milliseconds!");
    //rt.display();
    start = System.currentTimeMillis();
    for (int i = 0; i < 1000000; i++) {
      String path =
          "/nnThroughputBenchmark/mkdirs/ThroughputBenchDir" + i;
              //+ "/abc/a/b/c/d/aabbbccc/ddd";
      //path = "/a/b/" + i/100;
      //path = "/usr/f" + i%1000 + "/tt/abc";
      ExternalStorage found = NameNodeDummy.getNameNodeDummyInstance().findLastMatchedNode(path);
      //ExternalStorage found = NameNodeDummy.getNameNodeDummyInstance().findRadixTreeNodeServer(path);
      
      //System.out.println(path + ", found is " + found);
    }

    System.out.println("Method findLastMatchedNode spend "
        + (System.currentTimeMillis() - start) + " milliseconds!");
  }

  

  public static void main(String[] args) {
    TestMultiBranchTreeNode t = new TestMultiBranchTreeNode();
    t.testBuildOrAddBST();
    //t.testFindAllValues();
    //t.testFindRootValuesServer();
    //t.testFindChildren();
    //t.testFindChildren2();
  }

}
