package org.apache.hadoop.hdfs.server.namenode.dummy;

import org.apache.hadoop.hdfs.server.namenode.NameNodeDummy;

public class OverflowTableNode {

	public String key;
	private ExternalStorage value;

	public OverflowTableNode parent;
	public OverflowTableNode left, right; // children
	
	public OverflowTableNode(String path){
		this.key = path;
	}
	
	OverflowTableNode(String path, ExternalStorage es,
			OverflowTableNode parent) { // constructor
		if(NameNodeDummy.DEBUG)
		if (this.key == null || "".equals(key.trim())) {
			NameNodeDummy.log("[OverflowTableNode] why do you want to create a node with empty key? key = "+ key + "; value = " + value);
		}
		this.key = path;
		this.value = es;
		this.parent = parent;
	}

	public ExternalStorage getValue() {
		return value;
	}

	public void setValue(ExternalStorage value) {
		this.value = value;
	}
}
