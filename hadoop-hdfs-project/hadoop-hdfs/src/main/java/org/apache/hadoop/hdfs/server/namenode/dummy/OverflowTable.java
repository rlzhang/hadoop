package org.apache.hadoop.hdfs.server.namenode.dummy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdfs.server.namenode.NameNodeDummy;

public class OverflowTable {
  //private final static String NULL = "null";
  private final static String S = "/";
  private OverflowTableNode root;

  // private int count = 0;

  //private final Pattern pattern = Pattern.compile("/.+");

  public OverflowTableNode getRoot() {
    return root;
  }

  public boolean isNullOrBlank(String str) {
    return str == null || str.length() == 0;
  }

  public static void logs(String str) {
    if (NameNodeDummy.DEBUG)
      NameNodeDummy.debug(str);
  }

  public static void logs() {
    System.out.println();
  }

  public OverflowTable(OverflowTableNode root) {
    this.root = root;
  }

  /**
   * As designed, always get right node first
   * @return
   */
  public OverflowTableNode getFirstNotNullNode(OverflowTableNode o) {
    if (o != null && o.getValue() != null)
      return o;
    return getFirstNotNullNode(o.right);
  }

  //Record last found path in findNode method.
  private String lastFoundPath = null;
  private OverflowTableNode lastFoundNode = null;
  public OverflowTableNode findNodeClient(String path, boolean createIfNothere,
      boolean alwaysReturnParent) {
    return this.findNode(path, createIfNothere, alwaysReturnParent,null, true);
  }
  
  public OverflowTableNode findNode(String path, boolean createIfNothere,
      boolean alwaysReturnParent, boolean isClient) {
    return this.findNode(path, createIfNothere, alwaysReturnParent,null, isClient);
  }
  /**
   * Always return an insert point, only if path belong to the source name node.
   * 
   * @param path
   * @return
   */
  public OverflowTableNode findNode(String path, boolean createIfNothere,
      boolean alwaysReturnParent, ExternalStorage es, boolean isClient) {
    if (NameNodeDummy.DEBUG)
      logs("[OverflowTable] findNode: Try to find ----------" + path + " from "
          + root.key);

    if (this.isNullOrBlank(path)) {
      if (NameNodeDummy.INFOR)
        logs("[OverflowTable] findNode: Path cannot be empty " + path);
      return null;
    }
    path = path.trim();
    //Remove last separator
    if (path.charAt(path.length() - 1) == '/')
      path = path.substring(0, path.length() - 1);
    // Return root node.
    if (path.equals(root.key)) {
      return root;
    }

    String p = getRootFromFullPath(path, root);
    if (this.isNullOrBlank(p) || !root.key.equals(p)) {
      if (NameNodeDummy.INFOR)
        logs("[findNode] Path not existing!!!" + path);
      return null;
    }
    OverflowTableNode start = root;
    //Use last recorded path to short query depth of binary tree
    if (recordLastFound) {
      if (lastFoundPath != null && path.startsWith(lastFoundPath)) {
        p = lastFoundPath;
        start = lastFoundNode;
        //System.out.println(path + ": short the depth, found last matched path is " + p);
      }
    }

    /**
     * If root matched, almost guarantee return an insert point.
     */
    String remain = path.substring(p.length(), path.length());
    //System.out.println("Remain is " + remain);
    OverflowTableNode o =
        findNodeInt(start, remain, createIfNothere, alwaysReturnParent, es, isClient);
    if (recordLastFound) {
      if (this.findPathCount(remain) > 1 && o != null
          && (o.key != null || o.parent.key != null)) {
        //System.out.println(path + " : " + o.key + ( o.parent == null ? "" : o.parent.key));
        lastFoundPath =
            (o.key == null ? this.getFullPath(o.parent) : this.getFullPath(o));
        lastFoundNode = (o.key == null ? o.parent : o);
        System.out.println(path + ": the last found path is " + lastFoundPath);
      }
    }

    //System.out.println(path + ": the last found path is " + lastFoundPath);
    return o;
  }

  //Find method optimization. Can make insert and find 3 times faster.
  private final static boolean recordLastFound = false;

  /**
   * When do insert, create the dummy node first.
   * @param cur
   * @param path
   * @param es
   */
  private void createNotExistingNode(OverflowTableNode cur, String path,
      ExternalStorage es) {
    if (NameNodeDummy.DEBUG)
      logs("[OverflowTable] createNotExistingNode: " + path + " from "
          + cur.key);
    if (cur.right != null) {
      cur.right.setValue(es);
    } else {
      cur.left = new OverflowTableNode(null, null, cur);
      cur.right = new OverflowTableNode(path, es, cur);
    }
  }
  
  private void createNotExistingNodeForInsert(OverflowTableNode cur, String path,
      ExternalStorage es, boolean isClient) {
//    if(this.findPathCount(path)!=1) new Exception("longpath").printStackTrace();
//    else new Exception("good path!").printStackTrace();
    if (NameNodeDummy.DEBUG)
      logs("[OverflowTable] createNotExistingNode: " + path + " from "
          + cur.key);
    if (cur.right != null) {
      cur.right.setValue(es);
    } else {
      if (!isClient) {
        cur.left = new OverflowTableNode(null, null, cur);
        cur.right = new OverflowTableNode(path, es, cur);
        return;
      }
      // Create a red black tree
      boolean isRedBlack = this.createRedBlackTree(cur, path, es);
      if (!isRedBlack) {
        cur.left = new OverflowTableNode(null, null, cur);
        cur.right = new OverflowTableNode(path, es, cur);
      }
      
    }
  }

  private boolean createRedBlackTree(OverflowTableNode cur, String path, ExternalStorage es) {
    //Is parent a Null node.
    boolean isRedBlack = false;
    if (cur.parent != null && cur.parent.key == null) {
      RedBlackBST rb = cur.parent.getRb();
      if (rb.getLevel() == -1) {
        rb.setLevel(this.findPathCount(path));
      } else if (this.findPathCount(path) != rb.getLevel()) {
        //Not the same level, return;
        return false;
      }
      rb.put(path, es);
      //System.out.println(path + "[:]" + cur.key);
      isRedBlack = true;
//      PrettyPrintBST2.prettyPrintTree(rb.getRoot());
//      System.out.println(PrettyPrintBST2.sb.toString());
//      PrettyPrintBST2.sb.setLength(0);
    }
    return isRedBlack;
  }
  /**
   * Create a new node on current position right node, then link previous node on the same position to cur.right.right.
   * @param cur
   * @param or
   * @param c
   * @param curPath
   */
  private void createParentNodeInt(OverflowTableNode cur, OverflowTableNode or,
      String c, String curPath) {
    cur.right = new OverflowTableNode(c, null, cur);
    //this.createNotExistingNode(cur.right, c, null);
    cur.right.left = new OverflowTableNode(null, null, cur.right);
    cur.right.right = or;
    or.setParent(cur.right);
    // update key
    cur.right.right.key = curPath.substring(c.length(), curPath.length());
  }

  /**
   *
   Split long path to two part , for example /usr/folder1/s1/s2/s3 vs
   * /usr/folder1/s1 -> /usr/folder1/s1, /s2/s3 or /usr/folder1/s1/s2 vs
   * /usr/folder1/s1/s3 -> /usr/folder1/s1, /s2,/s3 When try to insert node,
   * make sure create it on right location.
   * 
   * @param cur
   * @param c
   * @param newPath
   * @param curPath
   */
  private OverflowTableNode createNodeDuringFind(OverflowTableNode cur,
      String c, String newPath, String curPath, ExternalStorage es) {

    OverflowTableNode or = cur.right;
    if (newPath.length() > c.length() && curPath.length() > c.length()) {
      this.createParentNodeInt(cur, or, c, curPath);
      logs("[OverflowTable] findNodeInt: Update key to " + cur.right.right.key);

      /**
       * Create new node for new path.
       */

      this.createNotExistingNode(cur.right.left,
          newPath.substring(c.length(), newPath.length()), es);
      return cur.right.left;

    } else if (newPath.length() == c.length()) {
      this.createParentNodeInt(cur, or, c, curPath);
      return cur.right;

    } else if (cur.right.key.length() == c.length()) {
      OverflowTableNode o =
          this.findLeftNodeInt(cur.right,
              newPath.substring(c.length(), newPath.length()));
      this.createNotExistingNode(o,
          newPath.substring(c.length(), newPath.length()), null);
      return o.right;
    }
    return null;
  }

  /**
   * Node might is a long path, if query path is not exactly match query condition, return the closest one.
   * For example, if try to query "/folder1" , but "/folder1/s1" is the only closest single node, then return it.
   * @param cur
   * @param c
   * @param newPath
   * @param curPath
   * @return
   */
  private OverflowTableNode findPartialMatchedNode(OverflowTableNode cur,
      String c, String queryPath, String curPath, boolean alwaysReturnParent) {

    if (queryPath.length() == c.length()) {
      return cur.right;
    }
    if (alwaysReturnParent)
      return cur;
    return null;
  }

  /**
   * Find the last node matches this path.
   * @param cur
   * @param path
   * @return
   */
  public OverflowTableNode findLastMatchNode(OverflowTableNode cur, String path) {
    return null;
  }

  /**
   * Internal method, recursively find node.
   * @param cur
   * @param path
   * @param createIfNothere
   * @return
   */
  private OverflowTableNode findNodeInt(OverflowTableNode cur, String path,
      boolean createIfNothere, boolean alwaysReturnParent, ExternalStorage es, boolean isClient) {
    //if (NameNodeDummy.TEST && (count++)%1000 == 0)
    //System.out.println("[OverflowTable] findNodeInt: Try to find " + path);
    //if (NameNodeDummy.isNullOrBlank(path) || path.length() < 2 || path.charAt(0) != '/') {
    // Length should euqal or more than 2. Remove this part make it 50% fast.
    /**
    if (this.isNullOrBlank(path) || path.length() < 2
        || path.charAt(0) != '/') {
      if (NameNodeDummy.INFOR)
        System.out.println("[OverflowTable] findNodeInt: Invalidate path!!! " + path);
      return null;
    } **/
    /**
     * This one a little bit faster
     */
    /**
    if (!pattern.matcher(path).matches()) {
      return null;
    }
    **/
    //    if (NameNodeDummy.DEBUG)
    //      logs("[OverflowTable] findNodeInt: cur " + cur.key + "; cur.right "
    //        + cur.right + "; alwaysReturnParent " + alwaysReturnParent);
    if (cur.right == null) {
      /**
       * If path not match, always return the last matched node.
       */

      if (alwaysReturnParent) {

        //if (path.equals(cur.right.key) || (c = this.getFirstCommonString(cur.right.key, path)) != null
        //	&& path.length() < cur.right.key.length()) {
        //logs("[OverflowTable] findNodeInt: Found parent to return now, node is " + cur.right.key);
        // How to deal if parent is NULL as well?
        return cur.key == null ? cur.parent : cur;

        //ifCut = false;
      }
      /** How to handle if no children there? **/
      if (createIfNothere) {
        this.createNotExistingNodeForInsert(cur, path, es, isClient);
        return cur;
      }
      // If cur is left node, return null
      return cur.key == null ? null : cur;
    }
    String p = getRootFromFullPath(path, cur.right);
    boolean isSame;
    if (p == null)
      isSame = false;
    else
      isSame = cur.right.key.equals(p);
    /**
     * Logic to merge the same path; like sub-path /user/test and path /user
     * should under the same branch.
     */
    if (NameNodeDummy.DEBUG)
      logs("[OverflowTable] findNodeInt: Decide if have to split tree (add one more parent node) here, right node is "
          + cur.right.key + " VS query path " + path);
    String c = null;
    //boolean ifCut = true;
    logs("[OverflowTable] findNodeInt: alwaysReturnParent "
        + alwaysReturnParent);

    if (this.findPathCount(cur.right.key) > 1
        && (c = this.getFirstCommonString(cur.right.key, path)) != null
        //&& !path.equals(cur.right.key) && !path.startsWith(cur.right.key)) {
        && !path.startsWith(cur.right.key)) {
      if (NameNodeDummy.DEBUG)
        logs("[OverflowTable] findNodeInt: Get common parent " + c);
      // Check if try to create node
      if (createIfNothere) {
        return this.createNodeDuringFind(cur, c, path, cur.right.key, es);
      } else {
        return this.findPartialMatchedNode(cur, c, path, cur.right.key,
            alwaysReturnParent);
      }
    }

    // Logic End
    if (!isSame) {
      // Should return parent?
      if (NameNodeDummy.DEBUG)
        System.out
            .println("[OverflowTable] findNodeInt: Not match, recursively find path "
                + path);

      //return findNodeInt(cur.left, path, createIfNothere, alwaysReturnParent);
      //This change make it three times faster.
      // if (recordLastFound) {
      //if (this.findPathCount(path) == 1) {
        //If this is the last path, make a quick search.
        if (isClient) {
          OverflowTableNode ot = findLeftNodeOnRedblack(cur.left, path);
          if (ot != null) return ot;
        }
        return findNodeInt(cur.left, path, createIfNothere, alwaysReturnParent, es, isClient);
        //return findNodeInt(findLeftNodeInt(cur.left, path), path,
          //  createIfNothere, alwaysReturnParent, es, isClient);
        //Retrun a virtual overflowtable node.  
//      } else {
////        if (NameNodeDummy.TEST)
////          System.out
////              .println("[OverflowTable] findNodeInt: Not match, recursively find right path "
////                  + path);
//        return findNodeInt(cur.left, path, createIfNothere, alwaysReturnParent,es);
//      }
      //      } else {
      //        return findNodeInt(cur.left, path, createIfNothere, alwaysReturnParent);
      //      }

    } else {
      if (NameNodeDummy.DEBUG)
        logs("[OverflowTable] findNodeInt: Path matched! Matched path is " + p
            + " , full path is " + path);
      //if (root.equals(path)) {
      if (p.equals(path)) {
        return cur.right;
      } else {
        String remain = path.substring(p.length(), path.length());
        return findNodeInt(cur.right, remain, createIfNothere,
            alwaysReturnParent,es, isClient);
      }
    }
  }

  private OverflowTableNode findLeftNodeOnRedblack(OverflowTableNode cur, String path) {
    RedBlackBST rb = cur.getRb();
    if (cur.parent.parent != null && cur.parent.parent.getRb()!=null && cur.parent.parent.getRb().getRoot() != null){
      NameNodeDummy.debug("[findLeftNodeOnRedblack] - Start" + cur.parent.parent.getRb().getRoot() + ":::" + cur.parent.right.key);
      rb = cur.parent.parent.getRb();
    }
    OverflowTableNode ot = null;
    // If not the same level , don't bother.
    //if (rb !=null && rb.getRoot() != null && this.findPathCount(path) == rb.getLevel()) {
    if (rb !=null && rb.getRoot() != null) {
      if (this.findPathCount(path) > 1) {
        path = this.getNaturalRootFromFullPath(path);
      }
      ExternalStorage es = rb.get(path);
      NameNodeDummy.debug("[findLeftNodeOnRedblack]" + this.getFullPath(cur) +", [findLeftNodeOnRedblack] found " + (es ==null ? "":es.getPath()));
      if (es != null)
        ot = new OverflowTableNode(path, es, cur, true);
    }
    //System.out.println(path + "::findLeftNodeOnRedblack::" + cur.key);
    return ot;
  }
  /**
   * Recursively find bottom left node.
   * @param cur
   * @param path
   * @return
   */
  private OverflowTableNode findLeftNodeInt(OverflowTableNode cur, String path) {
    if (cur.right == null)
      return cur;
    if (cur.right.key.toString().equals(path))
      return cur.right;
    else
      return findLeftNodeInt(cur.left, path);
  }

  //private int count = 0;
  private OverflowTableNode findLeftNodeNonrecursiveInt(OverflowTableNode cur,
      String path) {
    OverflowTableNode t = cur;
    //long start = System.currentTimeMillis();
    //count = 0;
    while (t.right != null) {
      //count++;
      //if (t.right.key.startsWith(path) || path.startsWith(t.right.key)) {
      if (t.right.key.equals(path)) {
        NameNodeDummy.debug("[findLeftNodeNonrecursiveInt] Matched " + (t == null ? "" : this.getFullPath(t)));
        
        return t.right;
      }
      t = t.left;
    }
    //System.out.println(path + ", count: " + count);
    NameNodeDummy.debug("[findLeftNodeNonrecursiveInt] found " + (t == null ? "" : this.getFullPath(t)));
    //System.out.println("findLeftNodeNonrecursiveInt spend " + (System.currentTimeMillis() - start));
    return t;
  }

  /**
   * If current node key is /a, full path is /a/e, return /a;
   * If current node key is /a, full path is /b/e, return null;
   * @param fullPath
   * @param cur
   * @return
   */
  private String getRootFromFullPath(String fullPath, OverflowTableNode cur) {
    if (NameNodeDummy.DEBUG)
      logs("[OverflowTable] getRootFromFullPath: Find root from " + fullPath
          + " by checking node path " + cur.key);
    if (fullPath == null)
      return null;
    /**
     * Avoid partial name matches, but not whole name matches.
     * For example, /f1 will match /f112, but you cannot return /f1 since that's not the case we want.
     */
    //if (fullPath.startsWith(cur.key + S) || fullPath.equals(cur.key)) //This one has bad performance
    if (fullPath.equals(cur.key)
        || (fullPath.startsWith(cur.key) && fullPath.charAt(cur.key.length()) == '/'))
      //if (Pattern.matches(cur.key + "/*", fullPath))
      return cur.key;
    return null;
  }

  /**
   * Get the first matched common path
   * Must avoid match string like bellow
   * /a/f1 vs /a/f1a, should return /a not /a/f1
   * /a/f1/ vs /a/f1, should return /a/f1 not /a/f1/
   * @param p1
   * @param p2
   * @return
   */
  public String getFirstCommonString(String p1, String p2) {
    if (p1 == null || p2 == null)
      return null;
    if (p1.equals(p2))
      return p1;
    int p1l = p1.length();
    int p2l = p2.length();
    if (p1.charAt(p1l - 1) != '/')
      p1 += S;
    if (p2.charAt(p2l - 1) != '/')
      p2 += S;
    p1l = p1.length();
    p2l = p2.length();
    int len = p1l > p2l ? p2l : p1l;
    //String p = p1.length() > p2.length() ? p1 : p2;
    int index = len - 1;
    int mark = -1;
    for (int i = 0; i < len; i++) {
      if (p1.charAt(i) == p2.charAt(i)) {
        if (p1.charAt(i) == '/')
          mark = i;
      } else {
        index = i - 1;
        break;
      }
    }
    if (p1.charAt(index) == p2.charAt(index) && p2.charAt(index) == '/') {
      // do nothing
    } else
      index = mark;
    if (NameNodeDummy.DEBUG)
      logs("[OverflowTable] getFirstCommonString: index = " + index);
    if (index > 0)
      return p1.substring(0, index);
    return null;
  }

  /**
   * Get natural matched first node.
   * For example: /usr/abc return /usr; /data/user/a/b return /data
   * @param fullPath
   * @return
   */
  public synchronized static String getNaturalRootFromFullPath(String fullPath) {
    if (fullPath == null)
      return null;
    String t = "";
    boolean start = false;
    for (int i = 0; i < fullPath.length(); i++) {
      if (fullPath.charAt(i) == '/')
        start = (!start);
      if (start) {
        t += fullPath.charAt(i);
      } else
        break;
    }
    return t;
  }

  /**
   * Find how many '/' in the path.
   * @param path
   * @return
   */
  private int findPathCount(String path) {
    int count = 0;
    if (path == null)
      return count;

    for (int i = 0; i < path.length(); i++) {
      if (path.charAt(i) == '/')
        count++;
    }
    return count;
  }

  /**
   * Always split last directory out of fullPath
   * 
   * @param path
   */
  public String[] splitPath(String path) {
    if (path == null || this.findPathCount(path) < 3)
      return null;
    String[] t = new String[2];
    t[0] = path.substring(0, path.lastIndexOf('/'));
    t[1] = path.substring(t[0].length(), path.length());
    return t;
  }

  public OverflowTableNode buildBST(ExternalStorage[] es, boolean ifSplit, boolean isClient) {
    return buildBSTInt(es, ifSplit, this.root, isClient);
  }
  
  /**
   * Maximum 1000 layers.
   * @param es
   * @return
   */
  public OverflowTableNode buildBSTByLevel(ExternalStorage[] es, Map<String, OverflowTable> root) {
    if (NameNodeDummy.isNullOrBlank(es)) return null;
    for(int i=0;i<es.length;i++) {
      String[] paths = es[i].getPath().split(S);
      OverflowTable ot = root.get(paths[0]);
      
    }
    return null;
  }

  private void insertByLevel(String[] paths) {
    int level = paths.length;
    int height = treeDepth(this.root);
    if (level > height) {
      this.createBranches(paths);
    }
  }
  
  /**
   * Create all the branches in paths.
   * @param paths
   */
  private void createBranches(String[] paths) {
    if (paths[0] != this.root.key) {
      System.err.println("Error! Not the same tree!" + root.key + ":" + paths[0]);
      return;
    }
    OverflowTableNode o = this.root;
    for(int i = 0; i < paths.length; i++) {
      if (o.right == null){
        o.left = new OverflowTableNode(null, null, o);
        o.right = new OverflowTableNode(paths[i], null, o);
      }
    } 
  }
  
  private static OverflowTable ot;
  private static Object ob = new Object();

  public static OverflowTable getInstance(OverflowTableNode o) {
    if (ot == null) {
      synchronized (ob) {
        if (ot == null)
          ot = new OverflowTable(o);
      }
    }
    return ot;
  }

  /**
   * 
   * @param es
   * @return
   */
  /**
  public static OverflowTable buildBSTFromScratch(ExternalStorage[] es) {
  	if(NameNodeDummy.isNullOrBlank(es)) return null;
  	String r = getNaturalRootFromFullPath(es[0].getPath());
  	if(NameNodeDummy.isNullOrBlank(r)) return null;
  	OverflowTableNode root = new OverflowTableNode(r);
  	getInstance(root);
  	ot.buildBSTInt(es, false, root);
  	return ot;
  }
  **/
  public static OverflowTable buildBSTFromScratch(ExternalStorage[] es, boolean isClient) {
    if (NameNodeDummy.isNullOrBlank(es))
      return null;
    String r = getNaturalRootFromFullPath(es[0].getPath());
    if (NameNodeDummy.isNullOrBlank(r))
      return null;
    OverflowTableNode root = new OverflowTableNode(r);
    getInstance(root);
    ot.buildBSTInt(es, false, root, isClient);
    return ot;
  }

  /**
   * Either build a new one if no existing, or add nodes to existing one.
   * @param es
   * @return
   */
  public synchronized static OverflowTable buildOrAddBST(ExternalStorage[] es,
      OverflowTable ot, boolean isClient) {
    long s = System.currentTimeMillis();
    if (NameNodeDummy.isNullOrBlank(es))
      return null;
    if (ot == null) {
      String r = getNaturalRootFromFullPath(es[0].getPath());
      //System.out.println("=====Build root key is " + r);
      if (NameNodeDummy.isNullOrBlank(r))
        return null;
      OverflowTableNode root = new OverflowTableNode(r);
      ot = new OverflowTable(root);
    }
    ot.buildBSTInt(es, false, ot.getRoot(), isClient);
    if (NameNodeDummy.DEBUG)
      System.out.println("[OverflowTable] The API buildOrAddBST spend "
          + (System.currentTimeMillis() - s));
    return ot;
  }

  public static int treeDepth(OverflowTableNode node) {
    if (node == null)
      return 0;
    int left = treeDepth(node.left);
    int right = treeDepth(node.right);

    int x = left > right ? left + 1 : right + 1;
    return x;
  }

  /**
   * Build binary tree from the array.
   * @param es
   * @param ifSplit
   * @return
   */
  private OverflowTableNode buildBSTInt(ExternalStorage[] es, boolean ifSplit,
      OverflowTableNode root, boolean isClient) {
    if (es == null)
      return null;
    //Arrays.sort(es);

    // Create subroot
    // this.insert(this.getNaturalRootFromFullPath(es[0].getPath()),null);
    // this.key = this.getNaturalRootFromFullPath(es[0].getPath());
    // this.value = null;
    // = new
    // OverflowTableNode(this.getNaturalRootFromFullPath(es[0].getPath()),
    // null, null);
    logs("[buildBSTInt] Build tree from root " + root.key + ", root has value "
        + root.getValue());

    long s = System.currentTimeMillis();
    for (int i = 0; i < es.length; i++) {

      String path = es[i].getPath();
      ExternalStorage cacheEs = NameNodeDummy.overflowPathCache.get(path);
      /**
       * If path already in binary tree, ignore; otherwise continue insert to binary tree.
       */
      if (cacheEs != null) {
        if (!es[i].getTargetNNServer().equalsIgnoreCase(
            cacheEs.getTargetNNServer())) {
          //If overflow table be updated in source name node server, update the reference.
          System.out
              .println("This barely happen, but seems overflow table has be updated! "
                  + path);
          cacheEs.setTargetNNServer(es[i].getTargetNNServer());
        }
        if (NameNodeDummy.DEBUG)
          System.out.println(es[i].getSourceNNServer()
              + ", overflow table existing, cancel insert: " + path);
        continue;
      } else {
        NameNodeDummy.overflowPathCache.put(path, es[i]);
      }

      if (NameNodeDummy.DEBUG)
        System.out.println("Full path is " + path);
      OverflowTableNode o;
      //if ((o = OverflowMap.getFromMap(path)) != null) return o;

      // testSplitPath(path);
      if (ifSplit) {
        String[] p = splitPath(path);
        if (p != null) {
          o = this.insert(p[0], null, isClient);
          if (o != null && o.right != null) {
            // logs("[buildBST] Is split to null ?"+p[1]);
            this.createNotExistingNode(o.right, p[1], es[i]);
          }
        } else {
          logs("[buildBSTInt] Don't have to split, directly insert " + path);
          o = this.insert(path, es[i], isClient);
        }
      } else {
        o = this.insert(path, es[i], isClient);
      }
      //if (o != null)
      //OverflowMap.addToMap(path, o);
      if (NameNodeDummy.DEBUG)
        System.out.println("Full path is " + (o != null ? o.key : null));
    }

    if (NameNodeDummy.DEBUG)
      System.out.println(es.length
          + "[OverflowTable] The API buildBSTInt spend "
          + (System.currentTimeMillis() - s));

    return root;
  }

  public ExternalStorage[] getAllChildren(OverflowTableNode root) {
    if (root == null)
      return null;
    logs("[getAllChildren] Try to get children from " + root.key);
    List<ExternalStorage> children = new ArrayList<ExternalStorage>();
    getAllChildrenInt(root, children);
    logs("[getAllChildren] Found children size is " + children.size());
    return children == null ? null : children.toArray(new ExternalStorage[0]);
  }

  private void getAllChildrenInt(OverflowTableNode root,
      List<ExternalStorage> children) {
    logs("[getAllChildrenInt] ---- Recursively get children from "
        + (root == null ? "" : root.key));
    if (root == null)
      return;
    if (root.getValue() != null) {
      logs("[getAllChildrenInt] Found child " + root.key);
      children.add(root.getValue());
    }
    getAllChildrenInt(root.left, children);
    getAllChildrenInt(root.right, children);
  }

  /**
   * Build binary search tree from external link node
   * 
   * @param es
   */
  private void buildBST(ExternalStorage[] es, String parentSrc) {
    if (es == null)
      return;
    Arrays.sort(es);
    OverflowTableNode subroot = new OverflowTableNode(parentSrc, null, null);
    subroot.left = new OverflowTableNode("", null, subroot);
    subroot.right = new OverflowTableNode(es[0].getPath(), es[0], subroot);
    int cur = 2;
    for (int i = 1; i < es.length; i++) {
      String path = es[i].getPath();
      int level = es[i].getParentId();
      if (level == cur) {
        subroot.right = new OverflowTableNode(path, es[i], subroot);
        subroot.left = new OverflowTableNode("", null, subroot);
      } else {
        cur++;
        String parentPath =
            es[i].getPath().substring(0, es[i].getPath().lastIndexOf(S));
        // BSTNode r r=
        // this.lookups(parentPath, elc);
        // r.right = new BSTNode<String,
        // ExternalStorage>(es[0].getPath(), es[i]);
        // r.left = new BSTNode("", null);
      }
    }
  }

  public String getFullPath(OverflowTableNode o) {
    if (o == null)
      return "";
    if (NameNodeDummy.DEBUG)
      logs("[getFullPath] on node " + o.key);
    return getFullPath(o.parent) + (o.key == null ? "" : o.key);
  }

  /**
   * Also set parent in this method.
   * 
   * @param key
   * @param value
   * @return
   */
  public OverflowTableNode insert(String key, ExternalStorage value, boolean isClient) {

    if (NameNodeDummy.DEBUG)
      System.out.println("[insert] Try to insert node " + key);
    OverflowTableNode otn = this.findNode(key, true, false, value, isClient);
    if (otn == null)
      return null;
    if (NameNodeDummy.DEBUG)
      logs("[insert] found " + otn.key + " from query path " + key);
    if (this.getFullPath(otn).equals(key)) {
      if (otn.getValue() == null)
        otn.setValue(value);
      if (NameNodeDummy.INFOR)
        logs("[insert] Node exist!!! Found exactly matched node "
            + this.getFullPath(otn));
      // Prevent return inserted one;
      return null;
      // getFullPath
      // otn.value = value;
      // If update value here?
    } else {
      if (otn.right != null && otn.right.getValue() != null) {
        if (NameNodeDummy.INFOR)
          logs("[insert] Node exist!!! Path = " + key + ";");
        return otn;
      }
      //this.createNotExistingNodeForInsert(otn, key, value);
      //seems don't need this step
      //this.createNotExistingNodeForInsert(otn, key, value);
    }
    return otn;
  }

  /**
   * Only one scenario to move path, if the path has been moved back to the
   * source namenode. In this case, we have to move the binary tree to the
   * bottom left null node of root node.
   * 
   * @param fullPath
   * @return
   */
  public OverflowTableNode remove(String fullPath, boolean isClient) {
    OverflowTableNode otn = this.findNode(fullPath, false, false, isClient);
    if (otn == null) {
      logs("Node not existing " + fullPath);
      return null;
    }
    if (otn.right != null) {
      OverflowTableNode o = moveCurser(root);
      o.left = otn.left;
      o.right = otn.right;
      // Recursively update key
      this.recursivelyUpdatePath(o.left, otn.key);
      this.recursivelyUpdatePath(o.right, otn.key);
      logs("Checking parent " + fullPath);

      logs(otn.parent.key + "===Found parent " + ";child=" + otn.key);

      logs(otn.parent.parent.key + "===Found parent " + ";child="
          + otn.parent.key);

      /**
       * Remove found tree
       */
      OverflowTableNode left = otn.parent.left.left;
      OverflowTableNode right = otn.parent.left.right;
      otn.parent.left = left;
      otn.parent.right = right;

      return o;
    }
    return null;

  }

  private OverflowTableNode moveCurser(OverflowTableNode o) {
    if (o.left == null)
      return o;
    return moveCurser(o.left);
  }

  private void recursivelyUpdatePath(OverflowTableNode o, String path) {
    if (o == null)
      return;
    if (o.key != null)
      o.key = path + o.key;
    recursivelyUpdatePath(o.left, path);
    recursivelyUpdatePath(o.right, path);
  }
}