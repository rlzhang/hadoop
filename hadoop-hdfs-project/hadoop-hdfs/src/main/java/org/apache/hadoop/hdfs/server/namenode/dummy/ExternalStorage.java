package org.apache.hadoop.hdfs.server.namenode.dummy;

import org.apache.hadoop.util.Time;

/**
 * Path -> Namenode
 * The real overflow table
 * @author Ray Zhang
 *
 */
public class ExternalStorage implements Comparable<ExternalStorage>{

	/**
	 * Default not set
	 */
	private int parentId = -1;
	private int id;
	private String sourceNNServer;
	private String targetNNServer;
	private String targetNNPId;
	private String path;
	// 1, move one subtree, 2, hash move.
	private short type = 1;
	private int hashValue = -1;
	private long moveTime;
	
	public ExternalStorage(int id,String targetNNServer,String targetNNPId,String path,String sourceNNServer){
		this.id = id;
		this.targetNNPId = targetNNPId;
		this.targetNNServer = targetNNServer;
		this.path = path;
		this.moveTime = Time.now();
		this.sourceNNServer = sourceNNServer;
	}
	
  public ExternalStorage(int pid,int id,String targetNNServer,String targetNNPId,String path,String sourceNNServer){
		this.parentId = pid;
		this.id = id;
		this.targetNNPId = targetNNPId;
		this.targetNNServer = targetNNServer;
		this.path = path;
		this.moveTime = Time.now();
		this.sourceNNServer = sourceNNServer;
	}

	public int getParentId() {
		return parentId;
	}

	public void setParentId(int parentId) {
		this.parentId = parentId;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getTargetNNServer() {
		return targetNNServer;
	}

	public void setTargetNNServer(String targetNNServer) {
		this.targetNNServer = targetNNServer;
	}

	public String getTargetNNPId() {
		return targetNNPId;
	}

	public void setTargetNNPId(String targetNNPId) {
		this.targetNNPId = targetNNPId;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public long getMoveTime() {
		return moveTime;
	}

	public void setMoveTime(long moveTime) {
		this.moveTime = moveTime;
	}
	
	public String toString(){
		return this.path+":"+this.parentId+":"+this.id+":"+this.targetNNServer+":"+this.targetNNPId+":"+this.sourceNNServer;
	}

	public String getSourceNNServer() {
		return sourceNNServer;
	}

	public void setSourceNNServer(String sourceNNServer) {
		this.sourceNNServer = sourceNNServer;
	}

	@Override
	public int compareTo(ExternalStorage es) {
	  if (this.type == 1)
		  return this.path.compareTo(es.getPath());
	  else
	    return this.hashValue - es.getHashValue();
	}

	public short getType() {
		return type;
	}

	public void setType(short type) {
		this.type = type;
	}

	public int getHashValue() {
		return hashValue;
	}

	public void setHashValue(int hashValue) {
		this.hashValue = hashValue;
	}


}