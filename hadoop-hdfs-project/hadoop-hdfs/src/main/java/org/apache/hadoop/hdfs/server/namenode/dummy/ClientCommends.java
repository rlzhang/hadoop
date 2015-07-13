package org.apache.hadoop.hdfs.server.namenode.dummy;


public class ClientCommends {
	// Command: 0, finished. 1, received all MapRequest.
	private int command = -1;
	private long listSize = -1;
	public int getCommand() {
		return command;
	}

	public void setCommand(int command) {
		this.command = command;
	}

  public long getListSize() {
    return listSize;
  }

  public void setListSize(long listSize) {
    this.listSize = listSize;
  }

}
