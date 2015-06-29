package org.apache.hadoop.hdfs.server.namenode.dummy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class OverflowMap {
  private static AtomicInteger count = new AtomicInteger(0);
  private static Map<String, OverflowTableNode> map =
      new ConcurrentHashMap<String, OverflowTableNode>();

  public static OverflowTableNode getFromMap(String key) {
    //if (map.get(key) != null) System.out.println("Cache matched " + key);
    return map.get(key);
  }

  public static void addToMap(String s, OverflowTableNode o) {
    if (o != null)
      map.put(s, o);
  }
}
