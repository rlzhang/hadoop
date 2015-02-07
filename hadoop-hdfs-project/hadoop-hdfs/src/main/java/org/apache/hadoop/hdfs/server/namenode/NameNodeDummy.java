package org.apache.hadoop.hdfs.server.namenode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.jsp.JspWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.dummy.INodeClient;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

/**
 * Main entry of display and move namespace tree.
 * 
 * @author Ray Zhang
 *
 */
public class NameNodeDummy {

	public static final Log LOG = LogFactory.getLog(NameNodeDummy.class
			.getName());
	// How many levels of namespace tree
	private final static int DEFAULT_LEVEL = 10;
	private final static String NEWLINE = "<br/>";
	private final static String SPACE = "&nbsp;&nbsp;";
	private final static int TCP_PORT = 8019;
	private final static int UDP_PORT = 18019;
	private final static Object obj = new Object();
	private static NameNodeDummy nameNodeDummy = null;
	private FSNamesystem fs;
	private NameNode nn;

	private boolean isNotifyDatanode = false;
	
	// For block report during heartbeat period. Namenode notifys all datanodes add new block pool ids.
	private Map<String, List<Long>> blockIds;
	private String originalBpId;
	
	/**
	 * Server side
	 */
	private String newBpId;
	private boolean isReportToNewNN = false;

	public static NameNodeDummy getNameNodeDummyInstance() {
		if (nameNodeDummy != null)
			return nameNodeDummy;
		synchronized (obj) {
			if (nameNodeDummy == null)
				nameNodeDummy = new NameNodeDummy();
		}
		return nameNodeDummy;
	}

	private NameNodeDummy() {

	}
	
	public FSNamesystem getFSNamesystem() {
		if(fs==null&&nn!=null) fs = nn.getNamesystem();
		return fs;
	}

	public void setFSNamesystem(FSNamesystem fs) {
		this.fs = fs;
	}
	
	public void setNameNode(NameNode nn) {
		this.nn = nn;
	}
	/**
	 * Print out logs to web page
	 * 
	 * @param out
	 * @param logs
	 * @throws IOException
	 */
	public void logs(JspWriter out, String logs) {

		try {
			out.println(logs);
			out.println(NEWLINE);
		} catch (IOException e) {
			System.err.println("Cannot write logs! " + logs);
		}
	}

	/**
	 * Move namespace sub-tree to different name node.
	 * 
	 * @param fs
	 * @param path
	 * @param server
	 * @param out
	 * @throws IOException
	 */
	public void moveNS(FSNamesystem fs, String path, String server,
			JspWriter out) throws IOException {
		long start = System.currentTimeMillis();
		logs(out, "Starting moving process..." + ", moving namespace " + path
				+ " to server" + server);
		if (path == null || "".equals(path.trim())) {
			logs(out, "Path cannot be empty!");
		}

		if (fs == null) {
			logs(out, "Namenode not ready yet!!!");
			return;
		}

		this.fs = fs;

		// Get sub-tree from root dir
		INode subTree = fs.dir.getINode(path);

		if (subTree == null || subTree.isRoot() || !subTree.isDirectory()) {
			logs(out, "Invalidate path!");
			return;
		}

		logs(out, "Found path " + subTree.getFullPathName());
		logs(out, " Display namespace (maximum 10 levels) :");
		logs(out, this.printNSInfo(subTree, 0, DEFAULT_LEVEL));

		try {
			INodeClient client = new INodeClient(server,
					NameNodeDummy.TCP_PORT, NameNodeDummy.UDP_PORT);

			// Send sub-tree to another name node
			client.sendINode(subTree, out, subTree.getParent().isRoot());

			// Collect blocks information and will notify data node update block
			// pool id.
			Map<String, List<Long>> map = getBlockInfos(fs, subTree);

			this.setBlockIds(map);

		} catch (Exception e) {
			e.printStackTrace();
			out.println("Namenode server not ready, please try again later ... "+e.getMessage());
		}

		logs(out, "Spend " + (System.currentTimeMillis() - start)
				+ " milliseconds!");
	}

	/**
	 * Block ids to long
	 * 
	 * @param hostname
	 * @return
	 */
	public long[] blockIdsToLong(String hostname) {
		List<Long> list = getBlockIds().get(hostname);
		if (list == null)
			return null;
		long[] ids = new long[list.size()];
		for (int i = 0; i < ids.length; i++) {
			ids[i] = list.get(i).longValue();
		}
		return ids;
	}

	public String humanReadableByteCount(long bytes) {
		return humanReadableByte(bytes, true);
	}

	/**
	 * Find the root dir.
	 * @param nn
	 * @return
	 */
	public INode getRoot() {
		if (this.getFSNamesystem() == null) {
			LOG.error("Namenode not ready yet!!!");
			return null;
		}
		return fs.dir.getRoot();
	}

	private String humanReadableByte(long bytes, boolean si) {
		int unit = si ? 1000 : 1024;
		if (bytes < unit)
			return bytes + " B";
		int exp = (int) (Math.log(bytes) / Math.log(unit));
		String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1)
				+ (si ? "" : "i");
		return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
	}

	/**
	 * Display namespace tree structure.
	 * 
	 * @param max
	 *            Maximum depth.
	 * @param fs
	 *            Context.
	 * @return
	 */
	public String displayNS(int max, FSNamesystem fs) {

		if (this.fs == null)
			this.fs = fs;

		if (this.fs == null) {
			return "Namenode not ready yet! System is still initializing, please wait...";
		}

		INodeDirectory root = null;
		try {
			root = (INodeDirectory) fs.getFSDirectory().getINode("/");
		} catch (UnresolvedLinkException e) {
			return "Cannot find root dir on namenode!";
		}

		return printNSInfo(root, 0, max);
	}

	private final static long prefetchSize = 10 * 64 * 1024 * 1024;

	private static Map<String, List<Long>> getBlockInfos(FSNamesystem fs,
			String path, Map<String, List<Long>> blockIds)
			throws FileNotFoundException, UnresolvedLinkException, IOException {

		// Get block locations within the specified range.
		LocatedBlocks blocks = fs.getBlockLocations(path, 0, prefetchSize,
				true, true, true);
		List<LocatedBlock> list = blocks.getLocatedBlocks();
		for (int i = 0; i < list.size(); i++) {
			LOG.info("Found block informations for moving namespace: blockid="
					+ list.get(i).getBlock().getBlockId() + ";getBlockPoolId="
					+ list.get(i).getBlock().getBlockPoolId() + ";");
			DatanodeInfo[] di = list.get(i).getLocations();
			for (int j = 0; j < di.length; j++) {
				List<Long> ids = blockIds.get(di[j].getHostName());
				if (ids == null) {
					ids = new ArrayList<Long>();
					blockIds.put(di[j].getHostName(), ids);
				}
				ids.add(Long.valueOf(list.get(i).getBlock().getBlockId()));
				LOG.info("Found datanode include moving namespace blocks:" + di[j].getHostName());
			}
		}

		return blockIds;
	}

	private List<String> getFilesFromINode(INode inode, List<String> list) {

		if (inode.isFile()) {
			list.add(inode.asFile().getFullPathName());
			return list;
		}
		ReadOnlyList<INode> roList = inode.asDirectory().getChildrenList(
				Snapshot.CURRENT_STATE_ID);
		for (int i = 0; i < roList.size(); i++) {
			getFilesFromINode(roList.get(i), list);
		}
		return list;

	}

	/**
	 * Collect block informations and ready for notify datanode change their
	 * block pool id.
	 * 
	 * @param fs
	 * @param inode
	 * @return
	 * @throws FileNotFoundException
	 * @throws UnresolvedLinkException
	 * @throws IOException
	 */
	private Map<String, List<Long>> getBlockInfos(FSNamesystem fs, INode inode)
			throws FileNotFoundException, UnresolvedLinkException, IOException {
		List<String> paths = new ArrayList<String>();
		Map<String, List<Long>> blockIds = new HashMap<String, List<Long>>();
		getFilesFromINode(inode, paths);
		for (int i = 0; i < paths.size(); i++)
			getBlockInfos(fs, paths.get(i), blockIds);
		this.setNotifyDatanode(true);
		return blockIds;
	}

	/**
	 * Display namespace tree structure
	 * 
	 * @param root
	 * @param startLevel
	 * @param max
	 * @return
	 */
	private String printNSInfo(INode root, int startLevel, int max) {
		StringBuilder sb = new StringBuilder();
		if (startLevel > max || !root.isDirectory())
			return "";
		if (root != null) {

			for (int i = 0; i < startLevel; i++) {
				sb.append(SPACE);
			}
			sb.append("-");
			sb.append(new String(root.getLocalNameBytes()));
			sb.append(" (" + root.computeQuotaUsage().get(Quota.NAMESPACE)
					+ ")");
			sb.append("<br/>");
		}

		ReadOnlyList<INode> roList = root.asDirectory().getChildrenList(
				Snapshot.CURRENT_STATE_ID);

		for (int i = 0; i < roList.size(); i++) {
			INode inode = roList.get(i);
			// sb.append(new String(inode.getLocalNameBytes()) + ",");
			sb.append(printNSInfo(inode, startLevel + 1, max));
		}
		return sb.toString();
	}

	public BlockManager getBlockManager() {
		return fs.getBlockManager();
	}

	public String getOriginalBpId() {
		return originalBpId;
	}

	public void setOriginalBpId(String originalBpId) {
		this.originalBpId = originalBpId;
	}

	public Map<String, List<Long>> getBlockIds() {
		return blockIds;
	}

	public void setBlockIds(Map<String, List<Long>> blockIds) {
		this.blockIds = blockIds;
	}

	public String getNewBpId() {
		return newBpId;
	}

	public void setNewBpId(String newBpId) {
		this.newBpId = newBpId;
	}

	public boolean isReportToNewNN() {
		return isReportToNewNN;
	}

	public void setReportToNewNN(boolean isReportToNewNN) {
		this.isReportToNewNN = isReportToNewNN;
	}

	public boolean isNotifyDatanode() {
		return isNotifyDatanode;
	}

	public void setNotifyDatanode(boolean isNotifyDatanode) {
		this.isNotifyDatanode = isNotifyDatanode;
	}

}
