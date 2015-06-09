package org.apache.hadoop.hdfs.server.namenode.dummy.partition;

import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;

public class Knapsack {

	private int weight;

	private int value;

	private INodeDirectory dir;
	
	public Knapsack(int value, int weight, INodeDirectory dir) {
		this.value = value;
		this.weight = weight;
		this.setDir(dir);
	}
	
	public Knapsack(int value, int weight) {
    this.value = value;
    this.weight = weight;
    this.setDir(null);
  }

	public int getWeight() {
		return weight;
	}

	public int getValue() {
		return value;
	}

	public String toString() {
		return "[size: " + weight + " " + "value: " + value + "]";
	}

  public INodeDirectory getDir() {
    return dir;
  }

  public void setDir(INodeDirectory dir) {
    this.dir = dir;
  }
}
