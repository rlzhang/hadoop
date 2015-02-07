package org.apache.hadoop.hdfs.server.namenode.dummy;

/**
 * Divide a big tree to smaller sub-trees.
 * @author Ray Zhang
 *
 */
public class MapRequest {
	private int size;
	private Integer key;
	private SubTree subtree;
	
	public MapRequest(Integer key,SubTree subtree,int totalSize){
		this.key = key;
		this.subtree = subtree;
		this.size = totalSize;
	}
	
	public Integer getKey() {
		return key;
	}
	public void setKey(Integer key) {
		this.key = key;
	}
	public SubTree getSubtree() {
		return subtree;
	}
	public void setSubtree(SubTree subtree) {
		this.subtree = subtree;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

}
