package org.apache.hadoop.hdfs.server.namenode.dummy;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.Quota;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

/**
 * 
 * @author Ray Zhang
 *
 */
public class SplitTree {
	private int index = 0;
	private Map<Integer,SubTree> map = new HashMap<Integer,SubTree>();

	/**
	 * Split big tree structure to nodes or small sub-tree
	 * @param inode
	 * @param size Decide how and when to split a tree
	 * @param parent
	 */
	public void splitToSmallTree(INode inode, int size, int parent) {
		int myID = this.increaseIndex();
		map.put(new Integer(myID),new SubTree(parent,inode));
		inode.setParent(null);
		if(inode.isFile()) return;
		
		ReadOnlyList<INode> roList = inode.asDirectory().getChildrenList(
				Snapshot.CURRENT_STATE_ID);

		for (int i = 0; i < roList.size(); i++) {			
			splitToSmallTree(roList.get(i),size,myID);
		}
		
		inode.asDirectory().clearChildren();
	}

	/**
	 * Split big tree structure to each nodes or small sub-tree (On building...)
	 * @param inode
	 * @param size Decide how and when to split a tree
	 * @param parent
	 */
	public void intelligentSplitToSmallTree(INode inode, int size, int parent) {
		
		boolean ifSuccess = this.intelligentSplitToSmallTreeBase(inode, size, parent, false);
		if(ifSuccess)
		map.get(new Integer(0)).getInode().asDirectory().clearChildren();
	}
	
	/**
	 * Basic function.
	 * @param inode
	 * @param size
	 * @param parent
	 * @param ifBreak
	 * @return
	 */
	private boolean intelligentSplitToSmallTreeBase(INode inode, int size, int parent,boolean ifBreak) {
		boolean ifSuccess = false;
		int myID = this.increaseIndex();
		System.out.println("localname:"+inode.getLocalName());
		map.put(new Integer(myID),new SubTree(parent,inode));
		long quota = inode.computeQuotaUsage().get(Quota.NAMESPACE);
		int splitSize = (int) ((quota/size)+1);
		System.out.println("Split to "+splitSize+"; metadata size is "+quota);
		if(splitSize>1){
			inode.setParent(null);
		} else return ifSuccess;
		
		if(ifBreak||inode.isFile()) return true;
		
		
		ReadOnlyList<INode> roList = inode.asDirectory().getChildrenList(
				Snapshot.CURRENT_STATE_ID);
		if(checkSize(splitSize,roList.size())){
			for (int i = 0; i < roList.size(); i++) {
				intelligentSplitToSmallTreeBase(roList.get(i),size,myID,true);
			}
		}
		
		return true;
	}
	
	/**
	 * Decide how to split the tree.
	 * @param expectSize
	 * @param currentSize
	 * @return
	 */
	private boolean checkSize(int expectSize,int currentSize){
			boolean pass = false;
			int minimum = (int) (expectSize*0.8);
			System.out.println(currentSize+"=currentSize;Minimum match "+minimum);
			if(minimum<=currentSize) pass=true;
			System.out.println(minimum+"checkSize:"+pass);
			return pass;
	}
	
	public void printMap(Map<Integer,SubTree> map){
		Iterator<Entry<Integer, SubTree>> iter = map.entrySet().iterator(); 
		while (iter.hasNext()) {
			Entry<Integer, SubTree> entry = iter.next(); 
		    Integer id = entry.getKey(); 
		    SubTree sub = entry.getValue();
			System.out.println(id+":"+sub.getParentID()+":"+sub.getInode().getLocalName());
		}
	}
	
	/**
	 * Merge back the divided sub-tree on the target namenode.
	 * @param map
	 * @return
	 */
	public INode mergeListToINode(Map<Integer,SubTree> map){
		if(map.isEmpty()) return null;
		INode root = map.get(new Integer(0)).getInode();
		System.out.println("Root dir:"+root.getFullPathName());
		Iterator<Entry<Integer, SubTree>> iter = map.entrySet().iterator(); 
		while (iter.hasNext()) {
			Entry<Integer, SubTree> entry = iter.next(); 
		    Integer id = entry.getKey();
		    SubTree sub = entry.getValue();
		    if(id.intValue()!=0)
		    map.get(new Integer(sub.getParentID())).getInode().asDirectory().addChild(sub.getInode());
		   System.out.println("Merge file "+sub.getInode().getLocalName());
		   if(sub.getInode().isFile()){
			   System.out.println("File name is "+sub.getInode().asFile().getFullPathName());
		   }
		}
		return root;
	}
	
	public Map<Integer,SubTree> getMap() {
		return map;
	}

	public int getIndex() {
		return index;
	}

	public synchronized int increaseIndex() {
		return this.index++;
	}

}
