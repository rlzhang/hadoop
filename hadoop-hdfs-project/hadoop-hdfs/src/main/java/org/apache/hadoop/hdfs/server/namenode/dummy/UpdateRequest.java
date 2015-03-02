package org.apache.hadoop.hdfs.server.namenode.dummy;

/**
 * Update overflowing table on source NN.
 * @author Ray Zhang
 *
 */
public class UpdateRequest {
	
	private String sourceNNServer;
	private String[] srcs;
	private String newTargetNN;
	private String oldTargetNN;
	
	public UpdateRequest(String sourceNNServer,String newTargetNN,String oldTargetNN,String[] srcs){
		this.sourceNNServer = sourceNNServer;
		this.newTargetNN = newTargetNN;
		this.oldTargetNN = oldTargetNN;
		this.srcs = srcs;
	}
	
	public String getSourceNNServer() {
		return sourceNNServer;
	}
	public void setSourceNNServer(String sourceNNServer) {
		this.sourceNNServer = sourceNNServer;
	}
	public String[] getSrcs() {
		return srcs;
	}
	public void setSrcs(String[] srcs) {
		this.srcs = srcs;
	}
	public String getNewTargetNN() {
		return newTargetNN;
	}
	public void setNewTargetNN(String newTargetNN) {
		this.newTargetNN = newTargetNN;
	}
	public String getOldTargetNN() {
		return oldTargetNN;
	}
	public void setOldTargetNN(String oldTargetNN) {
		this.oldTargetNN = oldTargetNN;
	}
}
