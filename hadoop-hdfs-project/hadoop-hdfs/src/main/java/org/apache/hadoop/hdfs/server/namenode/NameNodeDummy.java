package org.apache.hadoop.hdfs.server.namenode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.jsp.JspWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.dummy.ExternalStorage;
import org.apache.hadoop.hdfs.server.namenode.dummy.ExternalStorageMapping;
import org.apache.hadoop.hdfs.server.namenode.dummy.INodeClient;
import org.apache.hadoop.hdfs.server.namenode.dummy.INodeServer;
import org.apache.hadoop.hdfs.server.namenode.dummy.OverflowTable;
import org.apache.hadoop.hdfs.server.namenode.dummy.OverflowTableNode;
import org.apache.hadoop.hdfs.server.namenode.dummy.PrettyPrintBST;
import org.apache.hadoop.hdfs.server.namenode.dummy.UpdateRequest;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ChunkedArrayList;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Time;

/**
 * Main entry of display and move namespace tree.
 * 
 * @author Ray Zhang
 *
 */
public class NameNodeDummy {

  public static final Log LOG = LogFactory
      .getLog(NameNodeDummy.class.getName());
  // How many levels of namespace tree
  private final static int DEFAULT_LEVEL = 10;
  private final static String NEWLINE = "<br/>";
  private final static String SPACE = "&nbsp;&nbsp;";
  public final static int TCP_PORT = 8019;
  public final static int UDP_PORT = 18019;
  private final static Object obj = new Object();
  private static NameNodeDummy nameNodeDummy = null;
  private FSNamesystem fs;
  private NameNode nn;
  INodeDirectory root = null;
  private boolean isNotifyDatanode = false;
  // For block report during heartbeat period. Namenode notifys all datanodes
  // add new block pool ids.
  private Map<String, List<Long>> blockIds;
  private String originalBpId;
  public static boolean useDistributedNN = true;
  public final static boolean DEBUG = false;
  public final static boolean INFOR = true;
  public final static boolean WARN = true;
  // private static Map<String, OverflowTable> ROOT = new
  // ConcurrentHashMap<String, OverflowTable>();
  private Map<String, OverflowTable> ROOT =
      new ConcurrentHashMap<String, OverflowTable>();
  private Map<String, String> map = new HashMap<String, String>();
  /**
   * Server side
   */
  private String newBpId;
  private boolean isReportToNewNN = false;

  public boolean isMapEmpty() {
    return ROOT.size() == 0 ? true : false;
  }

  private ExternalStorage[] getExternalStorageFromRoot() {
    List<ExternalStorage> temp = new ArrayList<ExternalStorage>();
    for (OverflowTable ot : ROOT.values()) {
      // log("[nameNodeDummy] getExternalStorageFromRoot: In memory map key "+
      // ROOT.);
      temp.addAll(Arrays.asList(ot.getAllChildren(ot.getRoot())));
    }
    return temp.toArray(new ExternalStorage[0]);
  }

  public static NameNodeDummy getNameNodeDummyInstance() {
    if (nameNodeDummy != null)
      return nameNodeDummy;
    synchronized (obj) {
      if (nameNodeDummy == null)
        nameNodeDummy = new NameNodeDummy();
    }
    return nameNodeDummy;
  }

  public NameNodeDummy() {

  }

  public static boolean isNullOrBlank(Object[] obj) {
    return obj == null || obj.length == 0;
  }

  public static boolean isNullOrBlank(String str) {
    return str == null || str.length() == 0;
  }

  public FSNamesystem getFSNamesystem() {
    if (fs == null && nn != null)
      fs = nn.getNamesystem();
    return fs;
  }

  public InetSocketAddress getNamenodeAddress() {
    return this.nn.getNameNodeAddress();
  }

  public static String getExceptionsFullStack(Exception ex) {
    StringWriter errors = new StringWriter();
    ex.printStackTrace(new PrintWriter(errors));
    return errors.toString();
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

  public static void warn(String str) {
    if (NameNodeDummy.WARN)
      System.err.println(str);
  }

  public static void info(String str) {
    if (NameNodeDummy.INFOR)
      System.out.println(str);
  }

  public static void debug(String str) {
    if (NameNodeDummy.DEBUG)
      System.out.println(str);
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
  public synchronized void moveNS(FSNamesystem fs, String path, String server,
      JspWriter out) throws IOException {
    long start = System.currentTimeMillis();
    logs(out, "Starting moving process, moving namespace " + path
        + " to server " + server);
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

    logs(out, " Found path " + subTree.getFullPathName());
    logs(out, " Display namespace (maximum 10 levels) :");
    logs(out, this.printNSInfo(subTree, 0, DEFAULT_LEVEL));

    try {
      INodeClient client =
          INodeClient.getInstance(server, NameNodeDummy.TCP_PORT,
              NameNodeDummy.UDP_PORT);
      // INodeClient client = new INodeClient(server,
      // NameNodeDummy.TCP_PORT, NameNodeDummy.UDP_PORT);
      // Send sub-tree to another name node
      client.sendINode(subTree, out, subTree.getParent().isRoot());

      // Collect blocks information and will notify data node update block
      // pool id.
      Map<String, List<Long>> map = getBlockInfos(fs, subTree);
      this.setBlockIds(map);
      // client.cleanup();
    } catch (Exception e) {
      e.printStackTrace();
      out.println("Namenode server not ready, please try again later ... "
          + e.getMessage());
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
   * 
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
    String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
    return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
  }

  /**
   * Display namespace tree structure.
   * 
   * @param max Maximum depth.
   * @param fs Context.
   * @return
   */
  public String displayNS(int max, FSNamesystem fs) {

    if (this.fs == null)
      this.fs = fs;

    if (this.fs == null) {
      return "Namenode not ready yet! System is still initializing, please wait...";
    }

    if (root == null)
      root = (INodeDirectory) fs.getFSDirectory().rootDir;

    // .getINode("/");

    return printNSInfo(root, 0, max);
  }

  private final static long prefetchSize = 10 * 64 * 1024 * 1024;

  private synchronized static Map<String, List<Long>> getBlockInfos(
      FSNamesystem fs, String path, Map<String, List<Long>> blockIds)
      throws FileNotFoundException, UnresolvedLinkException, IOException {

    // Get block locations within the specified range.
    LocatedBlocks blocks =
        fs.getBlockLocations(path, 0, prefetchSize, true, true, true);
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
        LOG.info("Found datanode include moving namespace blocks:"
            + di[j].getHostName());
      }
    }

    return blockIds;
  }

  private List<String> getFilesFromINode(INode inode, List<String> list) {

    if (inode.isFile()) {
      list.add(inode.asFile().getFullPathName());
      return list;
    }
    if (!inode.isDirectory())
      return list;
    ReadOnlyList<INode> roList =
        inode.asDirectory().getChildrenList(Snapshot.CURRENT_STATE_ID);
    for (int i = 0; i < roList.size(); i++) {
      getFilesFromINode(roList.get(i), list);
    }
    return list;

  }

  /**
   * Collect block informations and ready for notify datanode change their block
   * pool id.
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
  public synchronized String printNSInfo(INode root, int startLevel, int max) {
    StringBuilder sb = new StringBuilder();
    if (startLevel > max || !root.isDirectory())
      return "";
    if (root != null) {

      for (int i = 0; i < startLevel; i++) {
        sb.append(SPACE);
      }
      sb.append("-");
      sb.append(new String(root.getLocalNameBytes()));
      sb.append(" (" + root.computeQuotaUsage().get(Quota.NAMESPACE) + ")");
      sb.append("<br/>");
    }

    ReadOnlyList<INode> roList =
        root.asDirectory().getChildrenList(Snapshot.CURRENT_STATE_ID);

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

  public void addINode(INodeFile inode) {
    if (inode == null) {
      System.out.println("Cannot add file null!");
      return;
    }
    try {
      inode =
          this.addFile(inode.getFullPathName(), inode.getPermissionStatus(),
              inode.getFileReplication(), inode.getPreferredBlockSize(), inode
                  .getFileUnderConstructionFeature().getClientName(), inode
                  .getFileUnderConstructionFeature().getClientMachine());
      System.out.println("Successfully add file " + inode.getFullPathName()
          + ";clientName="
          + inode.getFileUnderConstructionFeature().getClientName()
          + ";clientMachine="
          + inode.getFileUnderConstructionFeature().getClientMachine());
      this.getFSNamesystem().getFSDirectory().addToInodeMap(inode);
    } catch (SnapshotAccessControlException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (FileAlreadyExistsException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (QuotaExceededException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (UnresolvedLinkException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (AclException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private INodeFile addFile(String path, PermissionStatus permissions,
      short replication, long preferredBlockSize, String clientName,
      String clientMachine) throws SnapshotAccessControlException,
      FileAlreadyExistsException, QuotaExceededException,
      UnresolvedLinkException, AclException {
    if (this.getFSNamesystem() != null) {
      return this.fs.dir.addFile(path, permissions, replication,
          preferredBlockSize, clientName, clientMachine);
    }
    return null;
  }

  public void saveNamespace() throws AccessControlException, IOException {
    if (this.getFSNamesystem() != null) {
      this.fs.enterSafeMode(Boolean.FALSE);
      this.fs.saveNamespace();
      this.fs.leaveSafeMode();
    }
  }

  /**
   * After moved namespace, add overflowing table to that position
   * 
   * @param ie
   */
  public void addExternalNode(INodeExternalLink ie, INodeDirectory parent) {
    // System.out.println(ie+"Try to add INodeExternalLink to "+(parent==null)+";"+(parent.getLocalNameBytes().length==0)+";"+parent.isDirectory());
    if (parent == null) {
      this.getRoot().asDirectory().addChild(ie);

    } else {
      parent.addChild(ie);

    }
    this.fs.dir.addToInodeMap(ie);
  }

  /**
   * Temporary store INodeExternalLink
   */
  private Map<INode, INodeExternalLink> tempMap =
      new HashMap<INode, INodeExternalLink>();

  /**
   * Add the INodeExternalLink to top node
   * 
   * @param child
   * @param link
   * @param parent
   */
  public void filterExternalLink(INode child, INodeExternalLink link,
      INode parent) {

    if (child instanceof INodeExternalLink) {
      INodeExternalLink existingOne = (INodeExternalLink) child;
      List<ExternalStorage> esMap =
          new ArrayList<ExternalStorage>(Arrays.asList(link.getEsMap()));
      esMap.addAll(Arrays.asList(existingOne.getEsMap()));
      ExternalStorage[] temp = new ExternalStorage[esMap.size()];
      esMap.toArray(temp);
      link.setEsMap(temp);
      if (parent != null)
        parent.asDirectory().removeChild(child);
      tempMap.put(parent, existingOne);
      return;
    }
    if (!child.isDirectory())
      return;
    parent = child;
    ReadOnlyList<INode> roList =
        child.asDirectory().getChildrenList(Snapshot.CURRENT_STATE_ID);

    for (int i = 0; i < roList.size(); i++) {
      filterExternalLink(roList.get(i), link, parent);

    }
  }

  /**
   * Try to recover after divorce INodeExternalLink
   */
  public void recoverExternalLink() {
    if (tempMap != null) {
      Iterator<Entry<INode, INodeExternalLink>> iter =
          tempMap.entrySet().iterator();
      while (iter.hasNext()) {
        Entry<INode, INodeExternalLink> entry = iter.next();
        INode parent = entry.getKey();
        INodeExternalLink iel = entry.getValue();
        parent.asDirectory().addChild(iel);
      }
    }
  }

  public boolean deletePath(String path) {
    boolean temp = false;
    if (this.getFSNamesystem() != null) {
      this.fs.writeLock();
      try {
        List<INode> removedINodes = new ChunkedArrayList<INode>();
        BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
        long mtime = Time.now();
        long del =
            this.fs.getFSDirectory().delete(path, collectedBlocks,
                removedINodes, mtime);
        System.out.println("Delete file from path " + path
            + ";Succeed delete files count is " + del);
        if (del < 0) {
          return temp;
        }
        this.fs.getEditLog().logDelete(path, mtime, Boolean.FALSE);

        // Blocks/INodes will be handled later
        this.fs.removePathAndBlocks(path, null, removedINodes, true);
        this.fs.getEditLog().logSync();
        this.fs.removeBlocks(collectedBlocks); // Incremental deletion
        // of blocks
        collectedBlocks.clear();

        this.fs.incrDeletedFileCount(del);
        temp = true;
      } catch (AccessControlException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        System.out.println("AccessControlException=" + e.getMessage());
      } catch (SafeModeException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        System.out.println("SafeModeException=" + e.getMessage());
      } catch (UnresolvedLinkException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        System.out.println("UnresolvedLinkException=" + e.getMessage());
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        System.out.println("IOException=" + e.getMessage());
      } finally {
        this.fs.writeUnlock();
      }
    }
    return temp;
  }

  // private OverflowTable overflowTable;

  public OverflowTable buildOrAddBST(ExternalStorage[] es) {
    String key = OverflowTable.getNaturalRootFromFullPath(es[0].getPath());
    debug("[NamenodeDummy] buildOrAddBST: Get key " + key);
    if (ROOT.get(key) == null)
      ROOT.put(key, OverflowTable.buildOrAddBST(es, null));
    else
      ROOT.put(key, OverflowTable.buildOrAddBST(es, ROOT.get(key)));
    return ROOT.get(key);
  }

  public String getThefirstNN(String key) {
    OverflowTable ot = ROOT.get(OverflowTable.getNaturalRootFromFullPath(key));
    if (ot == null)
      return null;
    OverflowTableNode o;

    return (o = ot.findNode(key, false, false)) == null ? null
        : (o.getValue() == null ? null : o.getValue().getTargetNNServer());

  }

  public String getThefirstSourceNN(String key) {
    OverflowTable ot = ROOT.get(OverflowTable.getNaturalRootFromFullPath(key));
    if (ot == null)
      return null;
    OverflowTableNode o;

    return (o = ot.findNode(key, false, false)) == null ? null
        : (o.getValue() == null ? null : o.getValue().getSourceNNServer());

  }

  /**
   * If path in overflow table, return it.
   * 
   * @param key
   * @return
   */
  public OverflowTableNode getPathInServer(String key,
      boolean alwaysReturnParent) {
    OverflowTable ot = ROOT.get(OverflowTable.getNaturalRootFromFullPath(key));
    // Logs only, have to remove.
    if (ot != null)
      this.printlnOverflowTable(ot.getRoot());
    return ot == null ? null : ot.findNode(key, false, alwaysReturnParent);

  }

  /**
   * Set quota.
   * 
   * @param dir
   */
  public INodeDirectory addQuota(INodeDirectory dir) {
    dir.addDirectoryWithQuotaFeature(
        DirectoryWithQuotaFeature.DEFAULT_NAMESPACE_QUOTA,
        DirectoryWithQuotaFeature.DEFAULT_DISKSPACE_QUOTA);
    dir.addSnapshottableFeature();
    dir.setSnapshotQuota(0);
    return dir;
  }

  public INodeDirectory addQuota(INodeDirectory dir, long nsQuota, long dsQuota) {
    dir.addDirectoryWithQuotaFeature(nsQuota, dsQuota);
    dir.addSnapshottableFeature();
    dir.setSnapshotQuota(0);
    return dir;
  }

  public INodeDirectory setQota(INodeDirectory dir, long namespace,
      long diskspace) {
    dir.getDirectoryWithQuotaFeature().setSpaceConsumed(namespace, diskspace);
    return dir;
  }

  /**
   * If path not in source NN, do it best to check if bellow to any other NN
   * 
   * @param key
   * @param alwaysReturnParent
   * @return
   */
  public OverflowTableNode findLastMatchedPath(String key) {
    OverflowTable ot = ROOT.get(OverflowTable.getNaturalRootFromFullPath(key));
    // Logs only, have to remove.
    this.printlnOverflowTable(ot.getRoot());
    return ot == null ? null : ot.findNode(key, false, true);

  }

  private void printlnOverflowTable(OverflowTableNode o) {
    PrettyPrintBST.prettyPrintTree(o);
    debug(PrettyPrintBST.sb.toString());
    PrettyPrintBST.sb.setLength(0);
  }

  public void buildExternalLinkMap_OLD(ExternalStorage[] es) {
    ExternalStorageMapping.addToMap(es);
  }

  public ExternalStorage findExternalNN_OLD(String key, boolean ifRecursive) {

    if (key == null || key.length() == 0)
      return null;
    ExternalStorage temp = ExternalStorageMapping.getExternalStorage(key);
    if (temp != null) {
      System.out.println("[NameNodeDummy]Found path from overflowing table = "
          + key + ";" + temp);
      return temp;
    }
    if (key.indexOf('/') < 0)
      return null;
    if (ifRecursive) {
      key = key.substring(0, key.lastIndexOf('/'));
      return findExternalNN_OLD(key, ifRecursive);
    }
    return null;
  }

  public boolean removeExternalNN(String key) {
    OverflowTable ot = ROOT.get(OverflowTable.getNaturalRootFromFullPath(key));
    if (ot == null)
      return false;
    return ot.remove(key) == null ? false : true;
  }

  public static boolean removeExternalNN_OLD(String key) {
    if (ExternalStorageMapping.removeExternalStorage(key) != null)
      return true;
    return false;
  }

  public String filterNamespace(String path) {
    if (path.startsWith("/") && path.indexOf(INodeServer.PREFIX) == 1) {
      int index = -1;
      for (int i = 0; i < path.length(); i++) {
        if (path.charAt(i) == '/') {
          index = i;
          if (index > 0)
            break;
        }
      }
      path = path.substring(index, path.length());
    }
    return path;
  }

  /**
   * Check if path bellow to this overflow table record.
   * 
   * @param es
   * @param path
   * @return
   */
  public ExternalStorage findBestMatchInOverflowtable(ExternalStorage[] ess,
      String path) {
    boolean temp = false;
    int len = 0;
    ExternalStorage match = null;
    for (int i = 0; i < ess.length; i++) {
      ExternalStorage es = ess[i];
      NameNodeDummy
          .debug("[NameNodeDummy]findBestMatchInOverflowtable: ES Path :"
              + es.getPath());
      if (es.getType() == 1) {
        temp = path.startsWith(es.getPath());
      } else if (es.getType() == 2) {
        int index = path.lastIndexOf('/');
        String parent = path.substring(0, index);
        if (parent.equals(es.getPath())) {
          String name = path.substring(index + 1, path.length());
          if (name.hashCode() > es.hashCode())
            temp = true;
        }

      }
      if (temp && es.getPath().length() > len) {
        match = es;
        len = es.getPath().length();
      }
    }

    return match;
  }

  /**
   * If path outside current NN?
   * 
   * @param path
   * @return
   */
  public ExternalStorage[] findExternalNN(String path) {
    path = this.filterNamespace(path);
    debug("[NameNodeDummy] findExternalNN: Try to find " + path);
    if (NameNodeDummy.isNullOrBlank(path))
      return null;
    path = path.trim();
    OverflowTable ot = ROOT.get(OverflowTable.getNaturalRootFromFullPath(path));
    if ("/".equals(path))
      return getExternalStorageFromRoot();
    debug("[NameNodeDummy] findExternalNN: Cannot find "
        + OverflowTable.getNaturalRootFromFullPath(path));
    if (ot == null)
      return null;
    OverflowTableNode found = ot.findNode(path, false, true);
    debug("[NameNodeDummy] findExternalNN: Found path in other namenode "
        + found.key);
    return found == null ? null : ot.getAllChildren(found);
  }

  /**
   * If path outside current NN?
   * 
   * @param path
   * @return
   */
  public HdfsFileStatus findOverflowTableByPath(String path) {
    ExternalStorage[] ess = findExternalNN(path);
    return ess == null ? null : new HdfsFileStatus(ess, path);
  }

  public ExternalStorage[] findExternalNN_OLD(String key) {
    debug("[NameNodeDummy] findExternalNN_OLD: Try to find " + key);
    if (key == null || key.length() == 0)
      return null;
    if ("/".equals(key))
      return ExternalStorageMapping.findAll();
    ExternalStorage es = this.findExternalNN_OLD(key, false);
    /**
     * If path existing in overflowing table, might have possible in other NNs;
     * otherwise only on one single NN.
     */
    if (es != null) {
      ExternalStorage[] ess = ExternalStorageMapping.findByParentId(es.getId());
      ExternalStorage[] ess2 = new ExternalStorage[ess.length + 1];
      System.arraycopy(ess, 0, ess2, 1, ess.length);
      ess2[0] = es;
      return ess2;
    } else {
      es = findExternalNN_OLD(key, true);
      return es == null ? null : new ExternalStorage[] { es };
    }
  }

  /**
   * Send update information to namenode in order to update overflowing table.
   * 
   * @param sourceNN
   * @param out
   * @param newTargetNN
   * @param oldTargetNN
   * @param srcs
   */
  public void sendToNN(String sourceNN, JspWriter out, String newTargetNN,
      String oldTargetNN, String[] srcs) {

    INodeClient client =
        INodeClient.getInstance(sourceNN, NameNodeDummy.TCP_PORT,
            NameNodeDummy.UDP_PORT);
    try {
      client.connect();
    } catch (IOException e) {
      e.printStackTrace();
    }
    UpdateRequest u =
        new UpdateRequest(sourceNN, newTargetNN, oldTargetNN, srcs);
    client.sendTCP(u, out);

  }

  public static void main(String[] args) {
    System.out.println(NameNodeDummy.getNameNodeDummyInstance()
        .findExternalNN_OLD("/data1/test/tt", true));
    System.out.println(NameNodeDummy.isNullOrBlank(new Object[0]));
    System.out.println("a".equals(null));
  }

  public Map<String, String> getMap() {
    return map;
  }

  public void setMap(Map<String, String> map) {
    this.map = map;
  }

  public void setQuota(String src, long nsQuota, long dsQuota) {
    try {
      System.out.println(src + " = src ;[setQuota] nsQuota = " + nsQuota);
      getFSNamesystem().setQuota(src, nsQuota, dsQuota);
    } catch (UnresolvedLinkException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
