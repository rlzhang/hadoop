package org.apache.hadoop.hdfs.server.namenode;

import java.util.List;

import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.Quota.Counts;
import org.apache.hadoop.hdfs.server.namenode.dummy.ExternalStorage;
import org.apache.hadoop.hdfs.server.namenode.dummy.ExternalStorageMapping;
import org.apache.hadoop.util.Time;

public class INodeExternalLink extends INodeWithAdditionalFields {
	public final static String PREFIX = "INodeExternal_";
	//private ExternalStorageMapping esMap;
	private ExternalStorage[] esMap;

	INodeExternalLink(long id, byte[] name, PermissionStatus permissions,
			long modificationTime, long accessTime) {
		super(id, name, permissions, modificationTime, accessTime);
	}

	public static INodeExternalLink getInstanceFromExisting(long id, byte[] name, PermissionStatus permissions,
			long modificationTime, long accessTime,
			ExternalStorage[] esMap) {
		INodeExternalLink ie = new INodeExternalLink(id,
				name, permissions, modificationTime, accessTime);
		ie.setEsMap(esMap);
		return ie;
	}
	// INodeExternalLink(INode parent) {
	// super(parent);
	// TODO Auto-generated constructor stub
	// }

	public static INodeExternalLink getInstance(INode inode,
			ExternalStorageMapping esMap,String name) {
		PermissionStatus perm = new PermissionStatus(inode.getUserName(),
				inode.getGroupName(), inode.getFsPermission());
		//String name = PREFIX + esMap.getId();
		//String name = PREFIX + inode.getLocalName();
		INodeExternalLink ie = new INodeExternalLink(esMap.getId(),
				name.getBytes(), perm, Time.now(), Time.now());
		ie.setEsMap(esMap);
		return ie;
	}

	public void addToEsMap(ExternalStorage es){
		if(esMap!=null){
			synchronized(esMap){
				ExternalStorage[] newMap = new ExternalStorage[esMap.length+1];
				newMap[esMap.length] = es;
				System.arraycopy(esMap, 0, newMap, 0, esMap.length);
			}
		}
	}
	public ExternalStorage[] getEsMap() {
		return esMap;
	}

	public void setEsMap(ExternalStorage[] esMap) {
		this.esMap = esMap;
	}
	
	public void setEsMap(ExternalStorageMapping esMap) {
		this.esMap = new ExternalStorage[esMap.getList().size()];
		esMap.getList().toArray(this.esMap);
	}

	@Override
	void recordModification(int latestSnapshotId) throws QuotaExceededException {
		// TODO Auto-generated method stub

	}

	@Override
	public Counts cleanSubtree(int snapshotId, int priorSnapshotId,
			BlocksMapUpdateInfo collectedBlocks, List<INode> removedINodes,
			boolean countDiffChange) throws QuotaExceededException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void destroyAndCollectBlocks(BlocksMapUpdateInfo collectedBlocks,
			List<INode> removedINodes) {
		// TODO Auto-generated method stub

	}

	@Override
	public ContentSummaryComputationContext computeContentSummary(
			ContentSummaryComputationContext summary) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Counts computeQuotaUsage(Counts counts, boolean useCache,
			int lastSnapshotId) {
		counts.add(Quota.NAMESPACE, 1);
		return counts;
	}

	@Override
	public byte getStoragePolicyID() {
		throw new UnsupportedOperationException(
		        "Storage policy are not supported on external links");
	}

	@Override
	public byte getLocalStoragePolicyID() {
		throw new UnsupportedOperationException(
		        "Storage policy are not supported on external links");
	}

	@Override
	public INodeExternalLink asExternalLink() {
		return this;
	}

	@Override
	public boolean isExternalLink() {
		return true;
	}

}
