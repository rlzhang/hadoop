package org.apache.hadoop.hdfs;

import junit.framework.TestCase;

import org.apache.hadoop.hdfs.server.namenode.NameNodeDummy;
import org.apache.hadoop.hdfs.server.namenode.dummy.ExternalStorage;
import org.apache.hadoop.hdfs.server.namenode.dummy.OverflowTableNode;

public class TestNamenodeDummy extends TestCase {

  static ExternalStorage[] es = new ExternalStorage[10];
  static ExternalStorage[] es2 = new ExternalStorage[2];
  static {
    es[0] =
        new ExternalStorage(0, "a1.com", "poolid", "/usr/folder1/s1/s2/s3",
            "b.com");
    es[2] =
        new ExternalStorage(1, "a2.com", "poolid", "/usr/folder1/s1", "b.com");
    es[1] = new ExternalStorage(2, "a3.com", "poolid", "/usr/f1/1/f3", "b.com");
    es[3] = new ExternalStorage(3, "a4.com", "poolid", "/", "b.com");
    es[4] =
        new ExternalStorage(4, "a5.com", "poolid", "/usr/f1/2/aaaa", "b.com");
    es[5] =
        new ExternalStorage(5, "a6.com", "poolid", "/usr/f1/1/f/fa/fb", "b.com");
    es[6] =
        new ExternalStorage(6, "a7.com", "poolid",
            "/usr/f1/1/f/fa/fb/abc/d/e/f/g", "b.com");
    es[7] = new ExternalStorage(7, "a8.com", "poolid", "/usr/f1/1/f4", "b.com");
    es[8] = new ExternalStorage(8, "a9.com", "poolid", "/usr/f1/2/a", "b.com");
    es[9] =
        new ExternalStorage(9, "a10.com", "poolid", "/usr/f3/3/f3", "b.com");
  }

  static {
    es2[0] =
        new ExternalStorage(0, "a11.com", "poolid", "/abc/folder1/s1/s2/s3",
            "b.com");
    es2[1] =
        new ExternalStorage(1, "a22.com", "poolid", "/abc/folder1/s1", "b.com");
  }

  protected void setUp() throws Exception {
    NameNodeDummy.getNameNodeDummyInstance().buildOrAddBST(es);
    NameNodeDummy.getNameNodeDummyInstance().buildOrAddBST(es2);
  }

  public void testGetThefirstNN() {
    String out =
        NameNodeDummy.getNameNodeDummyInstance().getThefirstNN(
            "/usr/f1/1/f/fa/fb/abc/d/e/f/g");
    System.out.println("testGetThefirstNN " + out);
    assertEquals(out, "a7.com");
  }

  public void testCheckPathInServer() {
    OverflowTableNode otn =
        NameNodeDummy.getNameNodeDummyInstance().getPathInServer(
            "/abc/folder1/s1/abc/d/e/f/g/e/test.txt", false);
    assertNull(otn);
    otn =
        NameNodeDummy.getNameNodeDummyInstance().getPathInServer(
            "/usr/f3/3/f3", false);
    assertEquals(otn.getValue().getTargetNNServer(), "a10.com");
  }

  public void testFindLastMatchedPath() {
    OverflowTableNode otn =
        NameNodeDummy.getNameNodeDummyInstance().findLastMatchedPath(
            "/abc/folder1/s1/abc/d/e/f/g/e/test.txt");
    assertNotNull(otn);
    assertEquals(otn.key, "/folder1/s1");

    otn =
        NameNodeDummy.getNameNodeDummyInstance().findLastMatchedPath(
            "/usr/f1/1/f4/f5/test.txt");
    assertNotNull(otn);
    assertEquals(otn.key, "/f4");
  }

  public void testFilterNamespace() {
    String path = "/distr_from_bfd-darkwing0102-00.sv.walmartlabs.com/test2";
    String output =
        NameNodeDummy.getNameNodeDummyInstance().filterNamespace(path);
    assertEquals(output, "/test2");

    path = "/abc/test2";
    output = NameNodeDummy.getNameNodeDummyInstance().filterNamespace(path);
    assertEquals(output, "/abc/test2");
  }

}
