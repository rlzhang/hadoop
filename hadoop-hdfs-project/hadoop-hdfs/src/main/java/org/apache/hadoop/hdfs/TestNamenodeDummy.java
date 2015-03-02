package org.apache.hadoop.hdfs;

import org.apache.hadoop.hdfs.server.namenode.NameNodeDummy;
import org.apache.hadoop.hdfs.server.namenode.dummy.ExternalStorage;

public class TestNamenodeDummy {
	
	static ExternalStorage[] es = new ExternalStorage[10];

	static{
		es[0] = new ExternalStorage(0, "a1.com", "poolid", "/usr/folder1/s1/s2/s3", "b.com");
		es[2] = new ExternalStorage(1, "a2.com", "poolid", "/usr/folder1/s1", "b.com");
		es[1] = new ExternalStorage(2, "a3.com", "poolid", "/usr/f1/1/f3", "b.com");
		es[3] = new ExternalStorage(3, "a4.com", "poolid", "/", "b.com");
		es[4] = new ExternalStorage(4, "a5.com", "poolid", "/usr/f1/2/aaaa", "b.com");
		es[5] = new ExternalStorage(5, "a6.com", "poolid", "/usr/f1/1/f/fa/fb", "b.com");
		es[6] = new ExternalStorage(6, "a7.com", "poolid", "/usr/f1/1/f/fa/fb", "b.com");
		es[7] = new ExternalStorage(7, "a8.com", "poolid", "/usr/f1/1/f4", "b.com");
		es[8] = new ExternalStorage(8, "a9.com", "poolid", "/usr/f1/2/a", "b.com");
		es[9] = new ExternalStorage(9, "a10.com", "poolid", "/usr/f3/3/f3", "b.com");
	}
	public void testGetThefirstNN(){
		NameNodeDummy.getNameNodeDummyInstance().buildOrAddBST(es);
		System.out.println(NameNodeDummy.getNameNodeDummyInstance().getThefirstNN("/usr/f1/2/aaaa"));
	}
	public static void main(String[] args) {
		TestNamenodeDummy t = new TestNamenodeDummy();
		t.testGetThefirstNN();
	}

}
