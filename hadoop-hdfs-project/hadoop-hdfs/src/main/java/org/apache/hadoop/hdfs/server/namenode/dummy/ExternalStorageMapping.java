package org.apache.hadoop.hdfs.server.namenode.dummy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeExternalLink;

/**
 * Path -> Namenode Mapping
 * 
 * @author Ray Zhang
 *
 */
public class ExternalStorageMapping {
	private static AtomicInteger id = new AtomicInteger(0);
	private List<ExternalStorage> list = new ArrayList<ExternalStorage>();
	private final static Map<String,ExternalStorage> linkMap= new HashMap<String,ExternalStorage>();
	private ExternalStorage root;
	
	public static void addToMap(ExternalStorage[] es){
		if(es == null) return;
		System.out.println("[ExternalStorageMapping]es length = "+es.length);
		for(int i=0;i<es.length;i++){
			System.out.println("[ExternalStorageMapping]es"+es[i]);
			linkMap.put(es[i].getPath(), es[i]);
		}
	}
	public static ExternalStorage[] findAll(){
		ExternalStorage[] t = new ExternalStorage[linkMap.size()];
		return linkMap.values().toArray(t);
	}
	public static ExternalStorage[] findByParentId(int pid){
		List<ExternalStorage> temp = new ArrayList<ExternalStorage>();
		for (ExternalStorage es : linkMap.values()) {
			if(es.getParentId() == pid) temp.add(es);
		}
		ExternalStorage[] t = new ExternalStorage[temp.size()];
		return temp.toArray(t);
	}
	
	public static ExternalStorage removeExternalStorage(String key){
		return linkMap.remove(key);
	}
	
	public static ExternalStorage getExternalStorage(String key){
		return linkMap.get(key);
	}
	
	public ExternalStorage getRoot() {
		return root;
	}

	public void setRoot(ExternalStorage root) {
		this.root = root;
	}

	public ExternalStorageMapping(String targetNNServer,String targetNNPId,String path,String sourceNNServer){
		root = new ExternalStorage(this.getId(),targetNNServer,targetNNPId,path,sourceNNServer);
		list.add(root);
	}
	
	public void add(ExternalStorage es){
		list.add(es);
	}
	
	public void addAll(ExternalStorageMapping esm){
		if(esm!=null){
			for(int i=0;i<esm.getList().size();i++){
				ExternalStorage es = esm.getList().get(i);
				es.setId(getId());
				list.add(es);
			}
		}
	}
	public ExternalStorage[] toArray(){
		ExternalStorage[] temp = new ExternalStorage[this.list.size()];
		return this.list.toArray(temp);
	}
	public int getId(){
		return id.getAndIncrement();
	}
	
	public void remove(ExternalStorage es){
		list.remove(es);
	}
	
	public void setList(List<ExternalStorage> list){
		this.list.addAll(list);
	}
	
	public List<ExternalStorage> getList(){
		return this.list;
	}

}