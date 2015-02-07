package org.apache.hadoop.hdfs.server.namenode.dummy;

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.Quota;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

public class Tools {

	public static String display(INode root, int max, boolean showFile) {

		return printNSInfo(root, 0, max, showFile);
	}

	/**
	 * 
	 * @param root
	 * @param level
	 * @param max
	 * @param showFile
	 * @return
	 */
	private synchronized static String printNSInfo(INode root, int level,
			int max, boolean showFile) {
		StringBuilder sb = new StringBuilder();
		if ((!showFile && root.isFile()) || (level > max)) {
			return "";
		}
		if (root != null) {

			for (int i = 0; i < level; i++) {
				sb.append("  ");
			}
			sb.append("-");
			sb.append(new String(root.getLocalNameBytes()));
			sb.append(" (" + root.computeQuotaUsage().get(Quota.NAMESPACE)
					+ ")");
			sb.append("\n");
		}
		if (root.isFile())
			return sb.toString();
		ReadOnlyList<INode> roList = root.asDirectory().getChildrenList(
				Snapshot.CURRENT_STATE_ID);

		for (int i = 0; i < roList.size(); i++) {
			INode inode = roList.get(i);
			sb.append(printNSInfo(inode, level + 1, max, showFile));
		}
		return sb.toString();
	}

}
