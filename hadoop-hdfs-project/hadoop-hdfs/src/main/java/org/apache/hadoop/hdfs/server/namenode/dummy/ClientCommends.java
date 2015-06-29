package org.apache.hadoop.hdfs.server.namenode.dummy;


public class ClientCommends {
	// Command: 0, finished. 1, received all MapRequest.
	private int command = -1;
	private int listSize = -1;
	public int getCommand() {
		return command;
	}

	public void setCommand(int command) {
		this.command = command;
	}

  public int getListSize() {
    return listSize;
  }

  public void setListSize(int listSize) {
    this.listSize = listSize;
  }

}
