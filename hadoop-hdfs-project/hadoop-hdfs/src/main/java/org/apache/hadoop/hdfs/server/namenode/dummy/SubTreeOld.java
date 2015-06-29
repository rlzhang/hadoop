package org.apache.hadoop.hdfs.server.namenode.dummy;

import org.apache.hadoop.hdfs.server.namenode.INode;

/**
 * 
 * @author Ray Zhang
 *
 */
public class SubTree {
	private int parentID;
	private INode inode;

	SubTree(int parentID, INode inode) {
		this.setParentID(parentID);
		this.setInode(inode);
	}

	public int getParentID() {
		return parentID;
	}

	public void setParentID(int parentID) {
		this.parentID = parentID;
	}

	public INode getInode() {
		return inode;
	}

	public void setInode(INode inode) {
		this.inode = inode;
	}
}
