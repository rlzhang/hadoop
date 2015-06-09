package org.apache.hadoop.hdfs.server.namenode.dummy.partition;

public class NamenodeTable implements java.io.Serializable {

  private int id;
  //MB
  private long freeCapacity;
  //MB
  private long totalCapacity;
  private String namenodeServer;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public long getFreeCapacity() {
    return freeCapacity;
  }

  public void setFreeCapacity(long freeCapacity) {
    this.freeCapacity = freeCapacity;
  }

  public String getNamenodeServer() {
    return namenodeServer;
  }

  public void setNamenodeServer(String namenodeServer) {
    this.namenodeServer = namenodeServer;
  }

  public long getTotalCapacity() {
    return totalCapacity;
  }

  public void setTotalCapacity(long totalCapacity) {
    this.totalCapacity = totalCapacity;
  }

  @Override
  public String toString() {
    return this.getNamenodeServer() + ":" + this.getFreeCapacity();

  }

}
