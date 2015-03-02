package org.apache.hadoop.hdfs.server.namenode.dummy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hdfs.server.namenode.NameNodeDummy;

public class OverflowTable {
	//private final static String NULL = "null";
	private final static String S = "/";
	private OverflowTableNode root;
	
	public OverflowTableNode getRoot() {
		return root;
	}

	static void logs(String str) {
		NameNodeDummy.log(str);
	}

	static void logs() {
		System.out.println();
	}

	public OverflowTable(OverflowTableNode root){
		this.root = root;
	}
	
	/**
	 * As designed, always get right node first
	 * @return
	 */
	public OverflowTableNode getFirstNotNullNode(OverflowTableNode o){
		if(o!=null && o.getValue()!=null) return o;
		return getFirstNotNullNode(o.right);
	}

	/**
	 * Always return an insert point, if path belong to the source name node.
	 * 
	 * @param path
	 * @return
	 */
	public OverflowTableNode findNode(String path, boolean createIfNothere,boolean alwaysReturnParent) {
		if(NameNodeDummy.DEBUG)
		  logs("[OverflowTableNode] findNode: Try to find ----------" + path + " from " + root.key);

		if (NameNodeDummy.isNullOrBlank(path)) {
			if(NameNodeDummy.INFOR)
			  logs("[OverflowTableNode] findNode: Path cannot be empty " + path);
			return null;
		}
		path = path.trim();
		//Remove last separator
		if(path.charAt(path.length()-1)=='/') path = path.substring(0,path.length()-1);
		// Return root node.
		if (path.equals(root.key)) {
			return root;
		}

		String p = getRootFromFullPath(path, root);
		if (NameNodeDummy.isNullOrBlank(p) || !root.key.equals(p)) {
			if(NameNodeDummy.INFOR)
			  logs("[findNode] Path not existing!!!" + path);
			return null;
		}
		/**
		 * If root matched, almost guarantee return an insert point.
		 */
		String remain = path.substring(p.length(), path.length());
		return findNodeInt(root, remain, createIfNothere,alwaysReturnParent);
	}
	
	/**
	 * When do insert, create the dummy node first.
	 * @param cur
	 * @param path
	 * @param es
	 */
	private void createNotExistingNode(OverflowTableNode cur, String path,
			ExternalStorage es) {
		if(NameNodeDummy.DEBUG)
		  logs("[OverflowTableNode] createNotExistingNode: " + path + " from " + cur.key);
		if (cur.right != null) {
			cur.right.setValue(es);
		} else {
			cur.left = new OverflowTableNode(null, null, cur);
			cur.right = new OverflowTableNode(path, es, cur);
		}
	}

	/**
	 * Create a new node on current position right node, then link previous node on the same position to cur.right.right.
	 * @param cur
	 * @param or
	 * @param c
	 * @param curPath
	 */
	private void createParentNodeInt(OverflowTableNode cur,
			OverflowTableNode or, String c, String curPath) {
		cur.right =  new OverflowTableNode(c, null, cur);
		this.createNotExistingNode(cur.right, c, null);
		cur.right.right = or;

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
	private OverflowTableNode createNodeDuringFind(OverflowTableNode cur, String c,
			String newPath, String curPath) {

		OverflowTableNode or = cur.right;

		if (newPath.length() > c.length() && curPath.length() > c.length()) {
			this.createParentNodeInt(cur, or, c, curPath);
			logs("[OverflowTableNode] findNodeInt: Update key to " + cur.right.right.key);

			/**
			 * Create new node for new path.
			 */
			
			this.createNotExistingNode(cur.right.left, newPath.substring(
					c.length(), newPath.length()), null);
			return cur.right.left;

		} else if (newPath.length() == c.length()) {
			this.createParentNodeInt(cur, or, c, curPath);
			return cur.right;

		} else if (cur.right.key.length() == c.length()) {
			OverflowTableNode o = this.findLeftNodeInt(cur.right, newPath.substring(
					c.length(), newPath.length()));
			this.createNotExistingNode(o, newPath.substring(
					c.length(), newPath.length()), null);
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
	private OverflowTableNode findPartialMatchedNode(OverflowTableNode cur, String c,
			String queryPath, String curPath) {
		
		if (queryPath.length() == c.length()) {
			return cur.right;
		}
		 return null;
	}
	
	/**
	 * Find the last node matches this path.
	 * @param cur
	 * @param path
	 * @return
	 */
	public OverflowTableNode findLastMatchNode(OverflowTableNode cur, String path){
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
			boolean createIfNothere, boolean alwaysReturnParent) {
		if(NameNodeDummy.DEBUG)
		  logs("[OverflowTableNode] findNodeInt: Try to find " + path);
		//if (NameNodeDummy.isNullOrBlank(path) || path.length() < 2 || path.charAt(0) != '/') {
		if (NameNodeDummy.isNullOrBlank(path) || path.length() < 2 || path.charAt(0) != '/') {
			if(NameNodeDummy.INFOR)
			  logs("[OverflowTableNode] findNodeInt: Invalidate path!!! " + path);
			return null;
		}
		if (cur.right == null) {
			/** How to handle if no children there? **/
			if (createIfNothere) {
				this.createNotExistingNode(cur, path, null);
			}
			return cur;
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
		if(NameNodeDummy.DEBUG)
		  logs("[OverflowTableNode] findNodeInt: Decide if have to split tree (add one more parent node) here, right node is "
				+ cur.right.key + " VS query path " + path);
		String c = null;
		boolean ifCut = true;
		if(alwaysReturnParent){
			if (path.equals(cur.right.key) || (c = this.getFirstCommonString(cur.right.key, path)) != null
					&& path.length() < cur.right.key.length()) {
				return cur.right;
			}
			ifCut = false;
		} else if (this.findPathCount(cur.right.key) > 1
				&& (c = this.getFirstCommonString(cur.right.key, path)) != null
				&& !path.equals(cur.right.key)) {

			// Check if try to create node
			if (createIfNothere) {
				if(NameNodeDummy.DEBUG)
				  logs("[OverflowTableNode] findNodeInt: Get common parent " + c);
				return this.createNodeDuringFind(cur, c, path, cur.right.key);
			} else {
				return this.findPartialMatchedNode(cur, c, path, cur.right.key);
			}
		}
		
		// Logic End
		if (!isSame) {
			// Should return parent?
			if(NameNodeDummy.DEBUG)
			  logs("[OverflowTableNode] findNodeInt: Not match, recursively find path " + path);

			return findNodeInt(cur.left, path, createIfNothere,alwaysReturnParent);

		} else {
			if(NameNodeDummy.DEBUG)
			  logs("[OverflowTableNode] findNodeInt: Path matched! Matched path is " + root + " , full path is " + path);
			if (root.equals(path)) {
				return cur.right;
			} else {
				String remain = path.substring(p.length(), path.length());
				return findNodeInt(cur.right, remain, createIfNothere,alwaysReturnParent);
			}
		}
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

	/**
	 * If current node key is /a, full path is /a/e, return /a;
	 * If current node key is /a, full path is /b/e, return null;
	 * @param fullPath
	 * @param cur
	 * @return
	 */
	private String getRootFromFullPath(String fullPath, OverflowTableNode cur) {
        if(NameNodeDummy.DEBUG)
		  logs("[OverflowTableNode] getRootFromFullPath: Find root from " + fullPath
				+ " by checking node path " + cur.key);
		if (fullPath == null)
			return null;
		/**
		 * Avoid partial name matches, but not whole name matches.
		 * For example, /f1 will match /f112, but you cannot return /f1 since that's not the case we want.
		 */
		//if (fullPath.startsWith(cur.key + S) || fullPath.equals(cur.key)) //This one has bad performance
		if (fullPath.equals(cur.key) || (fullPath.startsWith(cur.key) && fullPath.charAt(cur.key.length())=='/'))
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
		if (p1.charAt(p1l-1) != '/')
			p1 += S;
		if (p2.charAt(p2l-1) != '/')
			p2 += S;
		int len = p1l > p2l ? p2l : p1l;
		//String p = p1.length() > p2.length() ? p1 : p2;
		int index = len - 1;
		int mark = -1;
		for (int i = 0; i < len; i++) {
			if (p1.charAt(i) == p2.charAt(i)) {
				if (p1.charAt(i) == '/')
					mark = i;
				continue;
			} else {
				index = i - 1;
				break;
			}
		}

		if (p1.charAt(index) == p2.charAt(index) && p2.charAt(index) == '/') {
			// do nothing
		} else
			index = mark;
        if(NameNodeDummy.DEBUG)
		  logs("[OverflowTableNode] getFirstCommonString: index = " + index);
		if (index > 0)
			return p1.substring(0, index);
		return null;
	}

	/**
	 * Get natural matched first node.
	 * @param fullPath
	 * @return
	 */
	public static String getNaturalRootFromFullPath(String fullPath) {
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
	
	public OverflowTableNode buildBST(ExternalStorage[] es, boolean ifSplit) {
		return buildBSTInt(es, ifSplit,this.root);
	}
	
	private static OverflowTable ot;
	private static Object ob = new Object();
	public static OverflowTable getInstance(OverflowTableNode o){
		if(ot == null){
			synchronized(ob) {
			  if(ot == null)
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
	public static OverflowTable buildBSTFromScratch(ExternalStorage[] es) {
		if(NameNodeDummy.isNullOrBlank(es)) return null;
		String r = getNaturalRootFromFullPath(es[0].getPath());
		if(NameNodeDummy.isNullOrBlank(r)) return null;
		OverflowTableNode root = new OverflowTableNode(r);
		getInstance(root);
		ot.buildBSTInt(es, false, root);
		return ot;
	}
	/**
	 * Either build a new one if no existing, or add nodes to existing one.
	 * @param es
	 * @return
	 */
	public static OverflowTable buildOrAddBST(ExternalStorage[] es , OverflowTable ot) {
		if(NameNodeDummy.isNullOrBlank(es)) return null;
		if(ot == null){
		  String r = getNaturalRootFromFullPath(es[0].getPath());
		  if(NameNodeDummy.isNullOrBlank(r)) return null;
		  OverflowTableNode root = new OverflowTableNode(r);
		  ot = new OverflowTable(root);
		}
		ot.buildBSTInt(es, false, ot.getRoot());
		return ot;
	}

	/**
	 * Build binary tree from the array.
	 * @param es
	 * @param ifSplit
	 * @return
	 */
	private OverflowTableNode buildBSTInt(ExternalStorage[] es, boolean ifSplit,OverflowTableNode root) {
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
		logs("[buildBSTInt] Build tree from root " + root.key
				+ ", root has value " + root.getValue());
		for (int i = 0; i < es.length; i++) {
			String path = es[i].getPath();
			// testSplitPath(path);
			if (ifSplit) {
				String[] p = splitPath(path);
				if (p != null) {
					OverflowTableNode o = this.insert(p[0], null);
					if (o != null && o.right != null) {
						// logs("[buildBST] Is split to null ?"+p[1]);
						this.createNotExistingNode(o.right, p[1], es[i]);
					}
				} else {
					logs("[buildBSTInt] Don't have to split, directly insert "
							+ path);
					this.insert(path, es[i]);
				}
			} else {
				this.insert(path, es[i]);
			}
		}
		return root;
	}

	public ExternalStorage[] getAllChildren(OverflowTableNode root) {
		if (root == null)
			return null;
		logs("[getAllChildren] Try to get children from " + root.key);
		List<ExternalStorage> children = new ArrayList<ExternalStorage>();
		getAllChildrenInt(root, children);
		logs("[getAllChildren]  Found children size is " + children.size());
		return children == null ? null : children
				.toArray(new ExternalStorage[0]);
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
				String parentPath = es[i].getPath().substring(0,
						es[i].getPath().lastIndexOf(S));
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
		if(NameNodeDummy.DEBUG)
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
	public OverflowTableNode insert(String key, ExternalStorage value) {
		if(NameNodeDummy.DEBUG)
		  logs("[insert] Try to insert node " + key);
		OverflowTableNode otn = this.findNode(key, true,false);
		if (otn == null)
			return null;
		if(NameNodeDummy.DEBUG)
		  logs("[insert] found " + otn.key + " from query path " + key);
		if (this.getFullPath(otn).equals(key)) {
			if(otn.getValue() == null) otn.setValue(value);
			if(NameNodeDummy.INFOR)
			  logs("[insert] Node exist!!! Found exactly matched node " + this.getFullPath(otn));
			// Prevent return inserted one;
			return null;
			// getFullPath
			// otn.value = value;
			// If update value here?
		} else {
			if (otn.right != null && otn.right.getValue() != null) {
				if(NameNodeDummy.INFOR)
				  logs("[insert] Node exist!!! Path = " + key +";found "+otn.left.right.key);
				return otn;
			}
			this.createNotExistingNode(otn, key, value);
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
	public OverflowTableNode remove(String fullPath) {
		OverflowTableNode otn = this.findNode(fullPath, false,false);
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

class BSTPrinter {

	/**
	 * 
	 * @param root
	 */

	public void printLevelOrder(OverflowTableNode root) {
		int height = getMaximumHeight(root);
		for (int i = 1; i <= height; i++) {
			printLevel(root, i);
			OverflowTable.logs();
		}
	}

	private void printLevel(OverflowTableNode root, int level) {
		if (root == null)
			return;
		if (level == 1) {
			System.out.print(root.key + " ");
		} else {
			printLevel(root.left, level - 1);
			printLevel(root.right, level - 1);
		}
	}

	public void printNode(OverflowTableNode root) {
		int maxLevel = this.maxLevel(root);
		this.printNodeInternal(Collections.singletonList(root), 1, maxLevel,
				false);
	}

	public void printNode(OverflowTableNode root, boolean showValue) {
		int maxLevel = this.maxLevel(root);
		this.printNodeInternal(Collections.singletonList(root), 1, maxLevel,
				showValue);
	}

	private static int getMaximumHeight(OverflowTableNode node) {
		if (node == null)
			return 0;
		int leftHeight = getMaximumHeight(node.left);
		int rightHeight = getMaximumHeight(node.right);
		return (leftHeight > rightHeight) ? leftHeight + 1 : rightHeight + 1;
	}

	private static String multiplyString(String string, int times) {
		StringBuilder builder = new StringBuilder(string.length() * times);
		for (int i = 0; i < times; ++i) {
			builder.append(string);
		}
		return builder.toString();
	}

	public static String getStartingSpace(int height) {
		return multiplyString("  ", ((int) Math.pow(2, height - 1)) / 2);
	}

	public static String getUnderScores(int height) {
		int noOfElementsToLeft = ((int) Math.pow(2, height) - 1) / 2;
		int noOfUnderScores = noOfElementsToLeft
				- ((int) Math.pow(2, height - 1) / 2);

		return multiplyString("__", noOfUnderScores);
	}

	public static String getSpaceBetweenTwoNodes(int height) {
		if (height == 0)
			return "";

		int noOfNodesInSubTreeOfNode = ((int) Math.pow(2, height - 1)) / 2;
		/** Sum of spaces of the subtrees of nodes + the parent node */
		int noOfSpacesBetweenTwoNodes = noOfNodesInSubTreeOfNode * 2 + 1;

		return multiplyString("  ", noOfSpacesBetweenTwoNodes);
	}

	public static void printNodes(List<OverflowTableNode> queueOfNodes,
			int noOfNodesAtCurrentHeight, int height) {
		StringBuilder nodesAtHeight = new StringBuilder();

		String startSpace = getStartingSpace(height);
		String spaceBetweenTwoNodes = getSpaceBetweenTwoNodes(height);

		String underScore = getUnderScores(height);
		String underScoreSpace = multiplyString(" ", underScore.length());

		nodesAtHeight.append(startSpace);
		for (int i = 0; i < noOfNodesAtCurrentHeight; i++) {
			OverflowTableNode node = (OverflowTableNode) queueOfNodes.get(i);
			if (node == null) {
				nodesAtHeight.append(underScoreSpace).append("  ")
						.append(underScoreSpace).append(spaceBetweenTwoNodes);
			} else {
				nodesAtHeight
						.append(node.left != null ? underScore
								: underScoreSpace)
						.append(node.key)
						.append(node.right != null ? underScore
								: underScoreSpace).append(spaceBetweenTwoNodes);
			}
		}

		OverflowTable.logs(nodesAtHeight.toString().replaceFirst("\\s+$",
				""));
	}

	public static String getSpaceBetweenLeftRightBranch(int height) {
		int noOfNodesBetweenLeftRightBranch = ((int) Math.pow(2, height - 1) - 1);

		return multiplyString("  ", noOfNodesBetweenLeftRightBranch);
	}

	public static String getSpaceBetweenRightLeftBranch(int height) {
		int noOfNodesBetweenLeftRightBranch = (int) Math.pow(2, height - 1);

		return multiplyString("  ", noOfNodesBetweenLeftRightBranch);
	}

	public static void printBranches(List<OverflowTableNode> queueOfNodes,
			int noOfNodesAtCurrentHeight, int height) {
		if (height <= 1)
			return;
		StringBuilder brachesAtHeight = new StringBuilder();

		String startSpace = getStartingSpace(height);
		String leftRightSpace = getSpaceBetweenLeftRightBranch(height);
		String rightLeftSpace = getSpaceBetweenRightLeftBranch(height);

		brachesAtHeight
				.append(startSpace.substring(0, startSpace.length() - 1));

		for (int i = 0; i < noOfNodesAtCurrentHeight; i++) {
			OverflowTableNode node = queueOfNodes.get(i);
			if (node == null) {
				brachesAtHeight.append(" ").append(leftRightSpace).append(" ")
						.append(rightLeftSpace);
			} else {
				brachesAtHeight.append(node.left != null ? "/" : " ")
						.append(leftRightSpace)
						.append(node.right != null ? "\\" : " ")
						.append(rightLeftSpace);
			}
		}

		System.out
				.println(brachesAtHeight.toString().replaceFirst("\\s+$", ""));
	}

	public void prettyPrintTree(OverflowTableNode root) {
		LinkedList<OverflowTableNode> queueOfNodes = new LinkedList<OverflowTableNode>();
		int height = getMaximumHeight(root);
		int level = 0;
		int noOfNodesAtCurrentHeight = 0;

		queueOfNodes.add(root);

		while (!queueOfNodes.isEmpty() && level < height) {
			noOfNodesAtCurrentHeight = ((int) Math.pow(2, level));

			printNodes(queueOfNodes, noOfNodesAtCurrentHeight, height - level);
			printBranches(queueOfNodes, noOfNodesAtCurrentHeight, height
					- level);

			for (int i = 0; i < noOfNodesAtCurrentHeight; i++) {
				OverflowTableNode currNode = queueOfNodes.peek();
				queueOfNodes.remove();
				if (currNode != null) {
					queueOfNodes.add(currNode.left);
					queueOfNodes.add(currNode.right);
				} else {
					queueOfNodes.add(null);
					queueOfNodes.add(null);
				}
			}
			level++;
		}
	}

	private void printNodeInternal(List<OverflowTableNode> nodes, int level,
			int maxLevel, boolean showValue) {
		if (nodes.isEmpty() || this.isAllElementsNull(nodes))
			return;

		int floor = maxLevel - level;
		int endgeLines = (int) Math.pow(2, (Math.max(floor - 1, 0)));
		int firstSpaces = (int) Math.pow(2, (floor)) - 1;
		int betweenSpaces = (int) Math.pow(2, (floor + 1)) - 1;
		// firstSpaces /= 2;
		// endgeLines /= 2;
		// betweenSpaces /= 2;
		this.printWhitespaces(firstSpaces);

		List<OverflowTableNode> newNodes = new ArrayList<OverflowTableNode>();
		for (OverflowTableNode node : nodes) {
			if (node != null) {
				if (showValue)
					System.out.print(node.key + "; value = " + node.getValue());
				else
					System.out.print(node.key);
				newNodes.add(node.left);
				newNodes.add(node.right);
			} else {
				newNodes.add(null);
				newNodes.add(null);
				System.out.print(" ");
			}

			this.printWhitespaces(betweenSpaces);
		}
		OverflowTable.logs("");

		for (int i = 1; i <= endgeLines; i++) {
			for (int j = 0; j < nodes.size(); j++) {
				this.printWhitespaces(firstSpaces - i);
				if (nodes.get(j) == null) {
					this.printWhitespaces(endgeLines + endgeLines + i + 1);
					continue;
				}

				if (nodes.get(j).left != null)
					System.out.print("/");
				else
					this.printWhitespaces(1);

				this.printWhitespaces(i + i - 1);

				if (nodes.get(j).right != null)
					System.out.print("\\");
				else
					this.printWhitespaces(1);

				this.printWhitespaces(endgeLines + endgeLines - i);
			}

			OverflowTable.logs("");
		}

		printNodeInternal(newNodes, level + 1, maxLevel, showValue);
	}

	private void printWhitespaces(int count) {
		for (int i = 0; i < count; i++)
			System.out.print(" ");
	}

	public int maxLevel(OverflowTableNode node) {
		if (node == null)
			return 0;

		return Math.max(this.maxLevel(node.left), this.maxLevel(node.right)) + 1;
	}

	private boolean isAllElementsNull(List<OverflowTableNode> list) {
		for (Object object : list) {
			if (object != null)
				return false;
		}

		return true;
	}

}