package org.apache.hadoop.hdfs.server.namenode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.server.namenode.dummy.GettingStarted;
import org.apache.hadoop.hdfs.server.namenode.dummy.INodeClient;

public class RemoveInmemoryNamespace extends Thread {

  INodeClient client = null;
  NameNodeDummy nn = null;
  FSNamesystem fs = null;
  INode inode = null;

  public RemoveInmemoryNamespace(INodeClient client, NameNodeDummy nn,
      FSNamesystem fs, INode subTree) {
    this.client = client;
    this.nn = nn;
    this.fs = fs;
    this.inode = subTree;
  }

  public void run() {
    // Collect blocks information and will notify data node update block
    // pool id.
    Map<String, List<Long>> map;
    try {
      map = nn.getBlockInfos(fs, inode);
      nn.setBlockIds(map);
      client.cleanup();
    } catch (FileNotFoundException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (UnresolvedLinkException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Cannot run clean up!");
    } finally {
      GettingStarted.setRun(false);
    }
  }

}
