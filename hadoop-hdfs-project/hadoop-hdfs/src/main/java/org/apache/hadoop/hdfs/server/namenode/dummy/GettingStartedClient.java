package org.apache.hadoop.hdfs.server.namenode.dummy;

import java.util.Map;

import org.apache.hadoop.hdfs.server.namenode.dummy.partition.NamenodeTable;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class GettingStartedClient {
  HazelcastInstance client;
  IMap<String, NamenodeTable> memoryMap;
  
  public GettingStartedClient() {
    init();
  }
  private void init() {
    ClientConfig clientConfig = new ClientConfig();
    client = HazelcastClient.newHazelcastClient(clientConfig);
    memoryMap = client.getMap("freeMem");
  }
  public Map<String, NamenodeTable> getMap() {
    return this.memoryMap;
  }
  private void printMap() {
    for (Map.Entry<String, NamenodeTable> entry : memoryMap.entrySet()) {
      System.out.println("Key : " + entry.getKey() + " Value : "
          + entry.getValue());
    }
  }
  
  private void shutdown() {
    client.shutdown();
  }
}