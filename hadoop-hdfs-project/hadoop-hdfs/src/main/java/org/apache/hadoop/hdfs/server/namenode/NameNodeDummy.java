package org.apache.hadoop.hdfs.server.namenode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.jsp.JspWriter;

import org.apache.commons.collections.map.LRUMap;
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
import org.apache.hadoop.hdfs.server.namenode.dummy.ExternalStorage;
import org.apache.hadoop.hdfs.server.namenode.dummy.ExternalStorageMapping;
import org.apache.hadoop.hdfs.server.namenode.dummy.INodeClient;
import org.apache.hadoop.hdfs.server.namenode.dummy.INodeServer;
import org.apache.hadoop.hdfs.server.namenode.dummy.IOverflowTable;
import org.apache.hadoop.hdfs.server.namenode.dummy.OverflowTable;
import org.apache.hadoop.hdfs.server.namenode.dummy.OverflowTableNode;
import org.apache.hadoop.hdfs.server.namenode.dummy.RadixTreeOverflowTable;
import org.apache.hadoop.hdfs.server.namenode.dummy.UpdateRequest;
import org.apache.hadoop.hdfs.server.namenode.dummy.tree.RadixTreeNode;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.security.AccessControlException;

/**
 * Main entry of display and move namespace tree.
 * 
 * @author Ray Zhang
 *
 */
public class NameNodeDummy {

  public static final String PRE = "/";
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
  //indicate HdfsFileStatus is empty.
  public static int EMPTY_HdfsFileStatus = -111;
  private String originalBpId;
  public static boolean useDistributedNN = true;
  public static boolean useCache = true;
  public static boolean TEST = false;
  public final static boolean DEBUG = false;
  public final static boolean INFOR = true;
  public final static boolean WARN = true;
  public final static boolean CreateInMemoryTable = true;
  public final static boolean PROCESSBLOCKREPORT = false;
  // private static Map<String, OverflowTable> ROOT = new
  // ConcurrentHashMap<String, OverflowTable>();
  private Map<String, OverflowTable> ROOT =
      new ConcurrentHashMap<String, OverflowTable>();
  //private Map<String, String> map = new HashMap<String, String>();

  // Mainly for client use
  private static Map<String, OverflowTable> staticRoot =
      new ConcurrentHashMap<String, OverflowTable>();

  private Map<String, IOverflowTable<ExternalStorage, RadixTreeNode>> RADIX_ROOT =
      new ConcurrentHashMap<String, IOverflowTable<ExternalStorage, RadixTreeNode>>();
  //private Map<String, String> map = new HashMap<String, String>();

  // Mainly for client use
  // private static Map<String, IOverflowTable<ExternalStorage, RadixTreeNode>> RADIX_STATIC_ROOT =
  //   new ConcurrentHashMap<String, IOverflowTable<ExternalStorage, RadixTreeNode>>();

  /**
   * For client use, store path => namenode address
   * Fix size is 1000.
   */

  public static Map<String, OverflowTableNode> findCache =
      java.util.Collections.synchronizedMap(new LRUMap(1000));

  private static Map<String, ExternalStorage> allPathCache =
      java.util.Collections.synchronizedMap(new LRUMap(1000));
  //private static LRUMap lru = new LRUMap(10);
  // path => ExternalStorage mapping. this path is exactly same as in ExternalStorage.
  public static Map<String, ExternalStorage> overflowPathCache =
      java.util.Collections.synchronizedMap(new LRUMap(2000));
  public static Set<ExternalStorage> overflowSet = java.util.Collections
      .synchronizedSet(new HashSet());

  /**
   * key is path hashcode, value is namenode hostname
   * @param key
   * @param value
   */
  public static void addToLRUMap(String key, ExternalStorage value) {
    allPathCache.put(key, value);
  }

  public static int lruMapSize() {
    return allPathCache.size();
  }

  public static Object removeFromLRUMap(String key) {
    return allPathCache.remove(key);
  }

  public static ExternalStorage getValueFromLRUMap(String key) {
    //Object ob = lru.get(key);
    //return ob == null ? null : ob.toString();
    return allPathCache.get(key);
  }

  public static void printLRUMap() {
    if (allPathCache.size() == 0)
      return;
    Iterator<String> keys = allPathCache.keySet().iterator();
    while (keys.hasNext()) {
      System.out.println(keys.next());
    }
  }

  /**
   * Server side
   */
  private String newBpId;
  private boolean isReportToNewNN = false;

  public boolean isMapEmpty() {
    return ROOT.size() == 0 ? true : false;
  }

  public boolean isClientMapEmpty() {
    return staticRoot.size() == 0 ? true : false;
  }

  public boolean isRadixMapEmpty() {
    return RADIX_ROOT.size() == 0 ? true : false;
  }

  private synchronized ExternalStorage[] getExternalStorageFromRoot(
      Map<String, OverflowTable> root) {
    List<ExternalStorage> temp = new ArrayList<ExternalStorage>();
    for (OverflowTable ot : root.values()) {
      // log("[nameNodeDummy] getExternalStorageFromRoot: In memory map key "+
      // ROOT.);
      temp.addAll(Arrays.asList(ot.getAllChildren(ot.getRoot())));
    }
    return temp.toArray(new ExternalStorage[0]);
  }

  public static NameNodeDummy getNameNodeDummyInstance() {
    if (nameNodeDummy != null)
      return nameNodeDummy;
    if (nameNodeDummy == null)
      synchronized (obj) {
        if (nameNodeDummy == null)
          nameNodeDummy = new NameNodeDummy();
      }
    return nameNodeDummy;
  }

  public NameNodeDummy() {

  }

  public synchronized static boolean isNullOrBlank(Object[] obj) {
    return obj == null || obj.length == 0;
  }

  public static void assertEqual(Object a, Object b) {
    if (!a.equals(b))
      System.err.println(a + " not equals " + b);
  }

  public synchronized static boolean isNullOrBlank(String str) {
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

  public void logs(String logs) {
    System.out.println(logs);
    System.out.println(NEWLINE);
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

  private static AtomicBoolean isRun = new AtomicBoolean(false);

  public static boolean isMovingRun() {
    return isRun.get();
  }

  public synchronized boolean moveNS(FSNamesystem fs, String path,
      String server, JspWriter out) throws IOException {
    if (isRun.get()) {
      System.err.println("Moving namespace is running..., cancel moving "
          + path);
      return false;
    }
    isRun.set(Boolean.TRUE);
    boolean isSuc = false;
    try {
       isSuc = this.moveNSBase(fs, path, server, out, false);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      System.err.println(e.getMessage());
    } finally {
      isRun.set(Boolean.FALSE);
    }
    return isSuc;
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
  public synchronized boolean moveNSBase(FSNamesystem fs, String path,
      String server, JspWriter out, boolean auto) throws IOException {
    boolean isSuc = false;
    if (nn.isInSafeMode()) {
      System.out
          .println("Namenode in safe mode, cancel namespace moving for path "
              + path);
      return isSuc;
    }
    long start = System.currentTimeMillis();
    if (!auto)
      logs(out, "Starting moving process, moving namespace " + path
          + " to server " + server);
    else {
      System.out.println("Starting moving process, moving namespace " + path
          + " to server " + server);
    }
    if (path == null || "".equals(path.trim())) {
      if (!auto)
        logs(out, "Path cannot be empty!");
      return isSuc;
    }

    if (fs == null) {
      if (!auto)
        logs(out, "Namenode not ready yet!!!");
      else
        System.out.println("Namenode not ready yet!!!");
      return isSuc;
    }

    this.fs = fs;

    // Get sub-tree from root dir
    INode subTree = fs.dir.getINode(path);

    if (subTree == null || subTree.isRoot() || !subTree.isDirectory()) {
      if (!auto)
        logs(out, "Invalidate path!");
      else
        System.out.println("Invalidate path!");
      return isSuc;
    }
    if (!auto)
      logs(out, "Found path " + subTree.getFullPathName());
    else
      System.out.println("Found path " + subTree.getFullPathName());
    //logs(out, " Display namespace (maximum 10 levels) :");
    //logs(out, this.printNSInfo(subTree, 0, DEFAULT_LEVEL));
    boolean suc = false;
    INodeClient client = null;
    try {
      client =
          INodeClient.getInstance(server, NameNodeDummy.TCP_PORT,
              NameNodeDummy.UDP_PORT);
      //client = new INodeClient(server,
      //NameNodeDummy.TCP_PORT, NameNodeDummy.UDP_PORT);
      // Send sub-tree to another name node
      if (client == null)
        System.err.println("Client should not be null!" + subTree);
      suc = client.sendINode(subTree, out, subTree.getParent().isRoot());
      System.out.println("(2)Client finished waiting server to response!");
      if (!auto)
        logs(
            out,
            (suc == true ? "Success!" : "Failed!") + ", spend "
                + (System.currentTimeMillis() - start) + " milliseconds!");
      else {
        System.out.println((suc == true ? "Success!" : "Failed!") + ", spend "
            + (System.currentTimeMillis() - start) + " milliseconds!");
      }
      if (suc) {
        isSuc = true;
        //System.out.println("Success! Clean source namenode and schedule block reports in background!");
        //new RemoveInmemoryNamespace(client, this, fs, subTree).start();
      }
      // client.cleanup();
    } catch (Exception e) {
      e.printStackTrace();
      if (!auto)
        out.println("Namenode server not ready, please try again later ... "
            + e.getMessage());
      else {
        System.out
            .println("Namenode server not ready, please try again later ... "
                + e.getMessage());
      }
    }

    if (!auto)
      logs(out, "spend " + (System.currentTimeMillis() - start)
          + " milliseconds!");
    System.out.println("Spend " + (System.currentTimeMillis() - start)
        + " milliseconds!");
    return isSuc;
  }

  public synchronized boolean moveNSAutomatically(String path, String server)
      throws IOException {
    if (isRun.get()) {
      System.err.println("Moving namespace is running..., cancel moving "
          + path);
      return false;
    }
    isRun.set(Boolean.TRUE);
    boolean isSuc = false;
    try {
      isSuc = this.moveNSBase(this.getFSNamesystem(), path, server, null, true);
    } catch (IOException e){
      throw e;
    } catch (Exception e) {
      System.err.println(e.getMessage());
    } finally {
      isRun.set(Boolean.FALSE);
    }       
    
    return isSuc;
  }

  public synchronized void moveNSOLD(String path, String server)
      throws IOException {
    if (nn.isInSafeMode()) {
      System.out
          .println("Namenode is in safe mode, cancel namespace moving for path "
              + path);
      return;
    }
    long start = System.currentTimeMillis();
    logs("Starting moving process, moving namespace " + path + " to server "
        + server);
    if (path == null || "".equals(path.trim())) {
      logs("Path cannot be empty!");
    }

    if (fs == null) {
      logs("Namenode not ready yet!!!");
      return;
    }
    fs = this.getFSNamesystem();

    // Get sub-tree from root dir
    INode subTree = fs.dir.getINode(path);

    if (subTree == null || subTree.isRoot() || !subTree.isDirectory()) {
      logs("Invalidate path!");
      return;
    }

    logs(" Found path " + subTree.getFullPathName());
    //logs(" Display namespace (maximum 10 levels) :");
    //logs(this.printNSInfo(subTree, 0, DEFAULT_LEVEL));

    try {
      INodeClient client =
          INodeClient.getInstance(server, NameNodeDummy.TCP_PORT,
              NameNodeDummy.UDP_PORT);
      // INodeClient client = new INodeClient(server,
      // NameNodeDummy.TCP_PORT, NameNodeDummy.UDP_PORT);
      // Send sub-tree to another name node
      client.sendINode(subTree, null, subTree.getParent().isRoot());

      System.out.println("(2)Client finished waiting server to response!");
      // Collect blocks information and will notify data node update block
      // pool id.
      Map<String, List<Long>> map = getBlockInfos(fs, subTree);
      this.setBlockIds(map);
      // client.cleanup();
    } catch (Exception e) {
      e.printStackTrace();
      System.out
          .println("Namenode server not ready, please try again later ... "
              + e.getMessage());
    }

    logs("Spend " + (System.currentTimeMillis() - start) + " milliseconds!");
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
    Iterator<Long> ite = list.iterator();
    //for (int i = 0; i < ids.length; i++) {
    int i = 0;
    while (ite.hasNext()) {
      //ids[i] = list.get(i).longValue();
      ids[i++] = ite.next();
    }
    list.clear();
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
  public INodeDirectory getRoot() {
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
    Iterator<LocatedBlock> ite = list.iterator();
    while (ite.hasNext()) {
      //for (int i = 0; i < list.size(); i++) {
      LocatedBlock locatedBlock = ite.next();
      LOG.info("Found block informations for moving namespace: blockid="
          + locatedBlock.getBlock().getBlockId() + ";getBlockPoolId="
          + locatedBlock.getBlock().getBlockPoolId() + ";");
      DatanodeInfo[] di = locatedBlock.getLocations();
      for (int j = 0; j < di.length; j++) {
        List<Long> ids = blockIds.get(di[j].getHostName());
        if (ids == null) {
          ids = new ArrayList<Long>();
          blockIds.put(di[j].getHostName(), ids);
        }
        ids.add(Long.valueOf(locatedBlock.getBlock().getBlockId()));
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
    Iterator<INode> ite = roList.iterator();
    //for (int i = 0; i < roList.size(); i++) {
    while (ite.hasNext()) {
      getFilesFromINode(ite.next(), list);
    }
    return list;

  }

  /**
   * Get total blocks in path.
   * @param inode
   * @return
   */
  public long numBlocks(INode inode) {

    if (inode.isFile()) {
      return inode.asFile().numBlocks();
    }
    if (!inode.isDirectory())
      return 0;
    ReadOnlyList<INode> roList =
        inode.asDirectory().getChildrenList(Snapshot.CURRENT_STATE_ID);
    //Iterator<INode> ite = roList.iterator();
    long total = 0;
    for (int i = 0; i < roList.size(); i++) {
      //while (ite.hasNext()) {
      total += numBlocks(roList.get(i));
    }
    return total;

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
  Map<String, List<Long>> getBlockInfos(FSNamesystem fs, INode inode)
      throws FileNotFoundException, UnresolvedLinkException, IOException {
    List<String> paths = new ArrayList<String>();
    Map<String, List<Long>> blockIds = new HashMap<String, List<Long>>();
    getFilesFromINode(inode, paths);
    Iterator<String> ite = paths.iterator();
    while (ite.hasNext())
      //for (int i = 0; i < paths.size(); i++)
      getBlockInfos(fs, ite.next(), blockIds);
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
    Iterator<INode> ite = roList.iterator();
    while (ite.hasNext()) {
      //for (int i = 0; i < roList.size(); i++) {
      INode inode = ite.next();
      // sb.append(new String(inode.getLocalNameBytes()) + ",");
      sb.append(printNSInfo(inode, startLevel + 1, max));
    }
    return sb.toString();
  }

  public void printOverflowTable(OverflowTableNode root) {
    if (root == null)
      return;
    System.out.println(root.key);
    List<OverflowTableNode> l = new ArrayList<OverflowTableNode>();
    l.add(root.left);
    l.add(root.right);
    printOverflowTable(l);
    System.out.println("--------------End");
  }

  public void printOverflowTable(List<OverflowTableNode> nodes) {

    List<OverflowTableNode> l = new ArrayList<OverflowTableNode>();
    Iterator<OverflowTableNode> ite = nodes.iterator();
    int i = 0;
    while (ite.hasNext()) {
      //for (int i = 0; i < nodes.size(); i++) {
      OverflowTableNode o = ite.next();
      if (o != null) {
        //if (!("null").equals (o.parent.key)) System.out.print("     ");
        System.out.print((((i++) % 2 == 1) ? " - " : " ") + o.key + "("
            + o.parent.key + ")");
        l.add(o.left);
        l.add(o.right);
      }
    }
    nodes.clear();
    System.out.println();
    if (l.size() > 0)
      printOverflowTable(l);
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
                  .getFileUnderConstructionFeature() == null ? "" : inode
                  .getFileUnderConstructionFeature().getClientName(), inode
                  .getFileUnderConstructionFeature() == null ? "" : inode
                  .getFileUnderConstructionFeature().getClientMachine());
      System.out.println("Successfully add file " + inode.getFullPathName());
      //+ ";clientName="
      //+ inode.getFileUnderConstructionFeature().getClientName()
      //+ ";clientMachine="
      //+ inode.getFileUnderConstructionFeature().getClientMachine());
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
   * Get moved namespace after insert overflow table.
   * @return
   */
  public INodeDirectory getMovedNamespace(INode inode) {
    INodeDirectory dir = inode.getParent();
    if (dir.isRoot()) {
      if (NameNodeDummy.DEBUG)
        System.out.println("[getMovedNamespace] Found namespace: "
            + inode.getFullPathName());
      return inode.asDirectory();
    }
    return getMovedNamespace(dir);
  }

  /**
   * Temporary store INodeExternalLink
   */
  //private Map<INode, INodeExternalLink> tempMap =
  //  new HashMap<INode, INodeExternalLink>();

  /**
   * Add the INodeExternalLink to top node
   * 
   * @param child
   * @param link
   * @param parent
   */
  public synchronized void filterExternalLink(INode child,
      INodeExternalLink link, INode parent) {

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
      //tempMap.put(parent, existingOne);
      return;
    }
    if (!child.isDirectory())
      return;
    parent = child;
    ReadOnlyList<INode> roList =
        child.asDirectory().getChildrenList(Snapshot.CURRENT_STATE_ID);

    //Iterator<INode> ite = roList.iterator();
    //while (ite.hasNext()) {
    for (int i = 0; i < roList.size(); i++) {
      //filterExternalLink(ite.next(), link, parent);
      filterExternalLink(roList.get(i), link, parent);
    }
  }

  /**
   * Try to recover after divorce INodeExternalLink
   */
  //  private void recoverExternalLink() {
  //    if (tempMap != null) {
  //      Iterator<Entry<INode, INodeExternalLink>> iter =
  //          tempMap.entrySet().iterator();
  //      while (iter.hasNext()) {
  //        Entry<INode, INodeExternalLink> entry = iter.next();
  //        INode parent = entry.getKey();
  //        INodeExternalLink iel = entry.getValue();
  //        parent.asDirectory().addChild(iel);
  //      }
  //    }
  //  }

  /**
   * Remove INode and all reference.
   * @param path
   * @return
   */
  public boolean deletePath(String path) {

    boolean d = false;
    try {
      System.out.println("Try to delete directory " + path);
      d = this.nn.getRpcServer().delete(path, true);
      System.out.println(d + ",deleted directory " + path);
    } catch (SnapshotAccessControlException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (AccessControlException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (SafeModeException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (UnresolvedLinkException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return d;
    /**
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
    **/
  }

  // private OverflowTable overflowTable;

  private boolean verifyOverflowTable(ExternalStorage[] es, String rootPath) {
    boolean returnValue = true;
    for (int i = 1; i < es.length; i++) {
      if (!es[i].getPath().startsWith(rootPath)) {
        returnValue = false;
      }
    }
    return returnValue;
  }

  public void buildOrAddBSTServer2(ExternalStorage[] es) {
    if (es == null)
      return;
    for (int i = 0; i < es.length; i++) {
      this.buildOrAddBST(es, ROOT, false);
    }
  }

  public OverflowTable buildOrAddBST(ExternalStorage[] es, boolean isClient) {
    return this.buildOrAddBST(es, ROOT, isClient);
  }

  public IOverflowTable buildOrAddRadixBSTServer(ExternalStorage[] es) {
    return this.buildOrAddBRadixBST(es, RADIX_ROOT);
  }

  //  public IOverflowTable buildOrAddRadixBSTClient(ExternalStorage[] es) {
  //    return this.buildOrAddBRadixBST(es, RADIX_STATIC_ROOT);
  //  }

  public void buildOrAddRadixAllBST(ExternalStorage[] es) {
    if (es == null)
      return;
    for (int i = 0; i < es.length; i++) {
      //synchronized(obj) {
      if (NameNodeDummy.overflowSet.contains(es[i])) {
        continue;
      }
      this.buildOrAddBRadixBST(new ExternalStorage[] { es[i] }, RADIX_ROOT);
      NameNodeDummy.overflowSet.add(es[i]);
      // }

    }
  }

  private IOverflowTable buildOrAddBRadixBST(ExternalStorage[] es,
      Map<String, IOverflowTable<ExternalStorage, RadixTreeNode>> root) {
    //long start = System.currentTimeMillis();
    String key = OverflowTable.getNaturalRootFromFullPath(es[0].getPath());
    if (!this.verifyOverflowTable(es, key)) {
      System.err
          .println("Verify failed, the root path not match in ExternalStorage array!");
      new Exception("Wrong path set!").printStackTrace();
      return null;
    }
    if (NameNodeDummy.DEBUG) {
      for (int i = 0; i < es.length; i++)
        System.out.println("[NamenodeDummy] buildOrAddBST: Get key " + key
            + "; value " + es[i]);
    }

    IOverflowTable node = root.get(key);
    if (node == null) {
      node = new RadixTreeOverflowTable();
      node.buildOrAddBST(es);
      //System.out.println("[NamenodeDummy] buildOrAddBST:" + key);
      root.put(key, node);
    } else {
      node.buildOrAddBST(es);
    }
    return node;
  }

  public void buildOrAddBSTAllClient(ExternalStorage[] es) {
    if (es == null)
      return;
    for (int i = 0; i < es.length; i++) {
      buildOrAddBSTClient(new ExternalStorage[] { es[i] });
    }
  }

  public OverflowTable buildOrAddBSTClient(ExternalStorage[] es) {
    return this.buildOrAddBST(es, staticRoot, true);
  }

  /**
   * To make this API works, es array should have the same root path and es[0] is the top or same level path.
   * @param es
   * @return
   */
  public OverflowTable buildOrAddBST(ExternalStorage[] es,
      Map<String, OverflowTable> root, boolean isClient) {
    //long start = System.currentTimeMillis();
    String key = OverflowTable.getNaturalRootFromFullPath(es[0].getPath());
    if (!this.verifyOverflowTable(es, key)) {
      System.err
          .println("Verify failed, the root path not match in ExternalStorage array!");
      new Exception("Wrong path set!").printStackTrace();
      return null;
    }
    if (NameNodeDummy.DEBUG)
      System.out.println("[NamenodeDummy] buildOrAddBST: Get key " + key);
    if (root.get(key) == null)
      root.put(key, OverflowTable.buildOrAddBST(es, null, isClient));
    else
      root.put(key, OverflowTable.buildOrAddBST(es, root.get(key), isClient));
    //System.out.println("[buildOrAddBST] Take " + (System.currentTimeMillis() - start) + " milliseconds.");
    //System.out.println("[buildOrAddBST] Overflow table depth is " + OverflowTable.treeDepth(root.get(key).getRoot()) + " in root dir " + key);
    return root.get(key);
  }

  public String getThefirstNN(String key, boolean isClient) {
    OverflowTable ot = ROOT.get(OverflowTable.getNaturalRootFromFullPath(key));
    if (ot == null)
      return null;
    OverflowTableNode o;
    return (o = ot.findNode(key, false, false, isClient)) == null ? null : (o
        .getValue() == null ? null : o.getValue().getTargetNNServer());

  }

  public String getThefirstSourceNN(String key, boolean isClient) {
    OverflowTable ot = ROOT.get(OverflowTable.getNaturalRootFromFullPath(key));
    if (ot == null)
      return null;
    OverflowTableNode o;

    return (o = ot.findNode(key, false, false, isClient)) == null ? null : (o
        .getValue() == null ? null : o.getValue().getSourceNNServer());

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
    //if (ot != null)
    //this.printlnOverflowTable(ot.getRoot());
    return ot == null ? null : ot.findNode(key, false, alwaysReturnParent,
        false);

  }

  /**
   * If path in overflow table, return full path.
   * 
   * @param key
   * @return
   */

  public Object[] getFullPathInServer(String key, boolean alwaysReturnParent) {
    return this.getFullPathInServer(key, alwaysReturnParent, ROOT, false);
  }

  public ExternalStorage findRadixTreeNodeServer(String key) {
    IOverflowTable<ExternalStorage, RadixTreeNode> ot =
        RADIX_ROOT.get(OverflowTable.getNaturalRootFromFullPath(key));
    ExternalStorage es = null;
    if (ot != null)
      es = ot.findNode(key);
    return es;
  }

  //  public ExternalStorage findLastMatchedNodeClient(String key) {
  //    return this.findLastMatchedNode(key, RADIX_STATIC_ROOT);
  //  }

  public ExternalStorage findLastMatchedNode(String key) {
    return this.findLastMatchedNode(key, RADIX_ROOT);
  }

  public void removeFromRadixTree(String key) {
    IOverflowTable<ExternalStorage, RadixTreeNode> ot =
        RADIX_ROOT.get(OverflowTable.getNaturalRootFromFullPath(key));
    if (ot != null)
      ot.remove(key);
  }

  public ExternalStorage findLastMatchedNode(String key,
      Map<String, IOverflowTable<ExternalStorage, RadixTreeNode>> root) {
    IOverflowTable<ExternalStorage, RadixTreeNode> ot =
        root.get(OverflowTable.getNaturalRootFromFullPath(key));
    if (ot == null) return null;
    RadixTreeNode<ExternalStorage> rt = ot.findLastMatchedNodeInTree(key);
    //System.out.println(key + " [::] " + (rt == null ? "null" : rt.getValue()));
    ExternalStorage es = null;
    if (rt != null) {

      while (true) {
         es = rt.getValue();
         if (es == null) break;
        if (es.getPath().equals(key)) {
          //Good to go
          break;
        } else if (es.getPath().length() > key.length()) {
          //Search path smaller than real path
          rt = rt.getParent();
        } else {
          //Search path (key) longer than real path (es.getPath)

          if (key.startsWith(es.getPath())
              && key.charAt(es.getPath().length()) == '/') {
            break;
          }
          rt = rt.getParent();
        }
      }

    }
    return es;
  }

  private ExternalStorage findLastMatchedNodeOld(String key,
      Map<String, IOverflowTable<ExternalStorage, RadixTreeNode>> root) {
    IOverflowTable<ExternalStorage, RadixTreeNode> ot =
        root.get(OverflowTable.getNaturalRootFromFullPath(key));
    ExternalStorage es = (ot == null ? null : ot.findLastMatchedNode(key));
    System.out.println(key + " :: " + es);
    //Avoid this type of match: /a/b/c => /a or /a/b, /a/1201 => /a/120 or /a/1
    int len = es == null ? 0 : es.getPath().length();
    //if (es != null && len <= key.length()
    //    && (es.getPath().equals(key) || (key.length() > len && key.charAt(len) == '/'))) {

    if (es != null
        && es.getPath().length() <= key.length()
        && (es.getPath().equals(key) || (key.length() > len
            && key.startsWith(es.getPath()) && key.charAt(len) == '/'))) {

      return es;
    }
    return null;
  }

  public Object[] getFullPathInServerClient(String key,
      boolean alwaysReturnParent) {
    return this.getFullPathInServer(key, alwaysReturnParent, staticRoot, true);
  }

  /**
   * This method should for client use only, don't try to use in server side!
   * Remember this method will create new node, to avoid general radix tree problem.
   * such like /root/a/b has nodes /root and /a/b, if we query /root/a/c, it should return /root/a not /root.
   * @param key
   * @param alwaysReturnParent
   * @return object[0]: node as OverflowTableNode; object[1]: Full path as String.
   */
  public Object[] getFullPathInServer(String key, boolean alwaysReturnParent,
      Map<String, OverflowTable> root, boolean isClient) {
    Object[] obj = new Object[2];
    OverflowTable ot = root.get(OverflowTable.getNaturalRootFromFullPath(key));

    if (ot == null)
      return null;
    OverflowTableNode found =
        ot.findNode(key, true, alwaysReturnParent, isClient);
    obj[0] = found;
    obj[1] = ot.getFullPath(found);
    return obj;
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
  public OverflowTableNode findLastMatchedPath(String key, boolean isClient) {
    OverflowTable ot = ROOT.get(OverflowTable.getNaturalRootFromFullPath(key));
    // Logs only, have to remove.
    //this.printlnOverflowTable(ot.getRoot());
    return ot == null ? null : ot.findNode(key, false, true, isClient);

  }

  /**
  private void printlnOverflowTable(OverflowTableNode o) {
    PrettyPrintBST.prettyPrintTree(o);
    debug(PrettyPrintBST.sb.toString());
    PrettyPrintBST.sb.setLength(0);
  }
  **/
  public void buildExternalLinkMap_OLD(ExternalStorage[] es) {
    ExternalStorageMapping.addToMap(es);
  }

  /**
   * If found matched path in overflow table, and current found node is dummy node, then find its parent which has target namenode host should be the right one.
   * Otherwise return the current value.
   * @param found
   * @return if return null, which means on the default server, the relative path not move to other namenode at all.
   */
  public ExternalStorage findHostInPath(OverflowTableNode found) {
    if (found == null)
      return null;
    ExternalStorage es = found.getValue();
    if (es != null)
      return es;
    //System.out.println("P: " + found.parent.key);
    return findHostInPath(found.parent);
  }

  public ExternalStorage findValideChild(OverflowTableNode found) {
    if (found == null)
      return null;
    ExternalStorage es = found.getValue();
    if (es != null)
      return es;
    return findHostInPath(found.right);
  }

  public ExternalStorage[] getRootExternalStorages(String path) {
    if (!path.equals("/"))
      return null;
    if (ROOT.size() == 0) {
      return null;
    }
    List<ExternalStorage> list = new ArrayList<ExternalStorage>();
    Collection<OverflowTable> collection = ROOT.values();
    for (OverflowTable ot : collection) {
      list.addAll(Arrays.asList(ot.getAllChildren(ot.getRoot())));
    }
    return (list.size() == 0 ? null : listToArray(list));
  }

  private ExternalStorage[] listToArray(List<ExternalStorage> list) {
    if (list.size() == 0)
      return null;
    ExternalStorage[] es = new ExternalStorage[list.size()];
    for (int i = 0; i < es.length; i++) {
      es[i] = list.get(i);
    }
    return es;
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

  public boolean removeExternalNN(String key, boolean isClient) {
    OverflowTable ot = ROOT.get(OverflowTable.getNaturalRootFromFullPath(key));
    if (ot == null)
      return false;
    return ot.remove(key, isClient) == null ? false : true;
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
   * Check if path belong to this overflow table record.
   * 
   * @param es
   * @param path
   * @return
   */
  public synchronized ExternalStorage findBestMatchInOverflowtable(
      ExternalStorage[] ess, String path) {
    boolean temp = false;
    int len = 0;
    ExternalStorage match = null;
    for (int i = 0; i < ess.length; i++) {
      ExternalStorage es = ess[i];
      if (DEBUG)
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
   * For server side use, get matched node or parent from overflow table(Binary Radix Tree).
   * @param path
   * @return
   */
  public synchronized OverflowTableNode findNode(String path, boolean isClient) {
    path = this.filterNamespace(path);
    if (DEBUG)
      debug("[NameNodeDummy] findNode: Try to find " + path);
    if (isNullOrBlank(path) || "/".equals(path))
      return null;
    path = path.trim();
    OverflowTable ot = ROOT.get(OverflowTable.getNaturalRootFromFullPath(path));
    if (ot == null)
      return null;
    OverflowTableNode found = ot.findNode(path, false, true, isClient);
    if (DEBUG)
      debug("[NameNodeDummy] findExternalNN: Found path in other namenode "
          + found.key);
    return found;
  }

  public synchronized ExternalStorage[] findExternalNNServer(String path) {
    return this.findExternalNN(path, ROOT, false);
  }

  //  public synchronized ExternalStorage[] findAllValuesClient(String path) {
  //    return this.findAllValuesInRadixTree(path, RADIX_STATIC_ROOT);
  //  }

  public synchronized ExternalStorage[] findAllValues(String path) {
    return this.findAllValuesInRadixTree(path, RADIX_ROOT);

  }

  /**
   * Remove the last /, as like /a/b/.
   * @param path
   * @return
   */
  public int calculateSlashCount(String path) {
    int count = 0;
    for (int i = 0; i < path.length() - 1; i++) {
      if (path.charAt(i) == '/')
        count++;
    }
    return count;
  }

  public long getCountOfFilesDirectoriesAndBlocks() {
    this.getFSNamesystem();
    long inodes = fs.dir.totalInodes();
    long blocks = fs.getBlocksTotal();
    return inodes + blocks;
  }

  /**
   * Find the children list for the giving path.
   * As like /a/b, return all /a/b/c1, /a/b/c2, but don't return /a/b/c/d1
   * @param path
   * @return
   */
  public synchronized ExternalStorage[] findChildren(String parent,
      boolean includeSelf) {
    ExternalStorage[] ess = this.findAllValuesInRadixTree(parent, RADIX_ROOT);
    if (ess == null)
      return null;
    List<ExternalStorage> list = new ArrayList<ExternalStorage>();
    int count = this.calculateSlashCount(parent);
    count++;
    for (int i = 0; i < ess.length; i++) {
      int splash = calculateSlashCount(ess[i].getPath());
      if (includeSelf) {
        if (splash == count || splash == count - 1) {
          list.add(ess[i]);
        }
      } else {
        if (splash == count) {
          list.add(ess[i]);
        }
      }

    }
    ExternalStorage[] es = new ExternalStorage[list.size()];
    return list.toArray(es);
  }

  public synchronized ExternalStorage[] findRootValuesServer(String path) {
    path = this.filterNamespace(path);
    if (isNullOrBlank(path) || !PRE.equals(path) || RADIX_ROOT.isEmpty())
      return null;
    ExternalStorage[] es = null;
    Iterator<IOverflowTable<ExternalStorage, RadixTreeNode>> ite =
        RADIX_ROOT.values().iterator();
    while (ite.hasNext()) {
      IOverflowTable<ExternalStorage, RadixTreeNode> ot = ite.next();
      ExternalStorage[] tmp = ot.findAllValues(PRE);
      //System.out.println("[findRootValuesServer]tmp is " + tmp.length);
      if (es != null) {
        ExternalStorage[] ess = new ExternalStorage[es.length + tmp.length];

        System.arraycopy(es, 0, ess, 0, es.length);
        if (tmp != null)
          System.arraycopy(tmp, 0, ess, es.length, tmp.length);
        es = ess;
        tmp = null;
      } else {
        es = tmp;
      }

    }
    return es;
  }

  public synchronized ExternalStorage[] findAllValuesInRadixTree(String path,
      Map<String, IOverflowTable<ExternalStorage, RadixTreeNode>> root) {
    path = this.filterNamespace(path);
    if (isNullOrBlank(path))
      return null;
    IOverflowTable<ExternalStorage, RadixTreeNode> ot =
        root.get(OverflowTable.getNaturalRootFromFullPath(path));
    if (ot == null)
      return null;
    return ot.findAllValues(path);
  }

  public synchronized ExternalStorage[] findExternalNNClient(String path) {
    return this.findExternalNN(path, staticRoot, true);
  }

  /**
   * If path outside current NN?
   * 
   * @param path
   * @return
   */
  private synchronized ExternalStorage[] findExternalNN(String path,
      Map<String, OverflowTable> root, boolean isClient) {
    path = this.filterNamespace(path);
    if (DEBUG)
      debug("[NameNodeDummy] findExternalNN: Try to find " + path);
    if (isNullOrBlank(path))
      return null;
    path = path.trim();
    OverflowTable ot = root.get(OverflowTable.getNaturalRootFromFullPath(path));
    if ("/".equals(path))
      return getExternalStorageFromRoot(root);
    if (DEBUG)
      debug("[NameNodeDummy] findExternalNN: Cannot find "
          + OverflowTable.getNaturalRootFromFullPath(path));
    if (ot == null)
      return null;
    OverflowTableNode found = ot.findNode(path, false, true, isClient);
    if (DEBUG)
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
    ExternalStorage[] ess = findExternalNNServer(path);
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
      client.connect(INodeServer.WRITE_BUFFER_MB);
    } catch (IOException e) {
      e.printStackTrace();
    }
    UpdateRequest u =
        new UpdateRequest(sourceNN, newTargetNN, oldTargetNN, srcs);
    try {
      client.sendTCP(u, out);
    } catch (Exception e) {
      System.err.println("[NameNodeDummy] Send UpdateRequest failed!"
          + e.getMessage());
    }

  }

  private int getSecondSplashIndex(String path) {
    //if (path == null || path.length() == 0) return -1;
    int index = 0;
    for (int i = 0; i < path.length(); i++) {
      if (path.charAt(i) == '/') {
        index++;
        if (index == 2)
          return i;
      }
    }
    return -1;
  }

  public String removeNamespace(String path) {
    if (path.startsWith(INodeServer.PREFIX_SLASH)) {
      return path.substring(getSecondSplashIndex(path), path.length());
    }
    return path;
  }

  public static void main(String[] args) {
    System.out.println(NameNodeDummy.getNameNodeDummyInstance()
        .findExternalNN_OLD("/data1/test/tt", true));
    System.out.println(NameNodeDummy.isNullOrBlank(new Object[0]));
    System.out.println("a".equals(null));
  }

  //  public Map<String, String> getMap() {
  //    return map;
  //  }

  //public void setMap(Map<String, String> map) {
  //this.map = map;
  //}

  public void setQuota(String src, long nsQuota, long dsQuota) {
    try {
      System.out.println(src + " = src ;[setQuota] nsQuota = " + nsQuota);
      getFSNamesystem().setQuota(src, nsQuota, dsQuota);
    } catch (UnresolvedLinkException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
