package org.apache.hadoop.hdfs.server.namenode.dummy.tree;

import java.util.ArrayList;
import java.util.Formattable;
import java.util.Formatter;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.hdfs.server.namenode.NameNodeDummy;

public class RadixTreeImpl<T> implements RadixTree<T>, Formattable {

  protected RadixTreeNode<T> root;

  protected long size;

  /**
   * Create a Radix Tree with only the default node root.
   */
  public RadixTreeImpl() {
    root = new RadixTreeNode<T>(null);
    root.setKey("");
    size = 0;
  }

  public RadixTreeNode<T> getRoot() {
    return this.root;
  }

  public T find(String key) {
    Visitor<T, T> visitor = new VisitorImpl<T, T>() {

      public void visit(String key, RadixTreeNode<T> parent,
          RadixTreeNode<T> node) {
        if (node.isReal())
          result = node.getValue();
      }
    };

    visit(key, visitor);

    return visitor.getResult();
  }

  public boolean delete(String key) {
    Visitor<T, Boolean> visitor = new VisitorImpl<T, Boolean>(Boolean.FALSE) {
      public void visit(String key, RadixTreeNode<T> parent,
          RadixTreeNode<T> node) {
        result = node.isReal();

        // if it is a real node
        if (result) {
          // If there no children of the node we need to
          // delete it from the its parent children list
          if (node.getChildren().size() == 0) {
            Iterator<RadixTreeNode<T>> it = parent.getChildren().iterator();
            while (it.hasNext()) {
              if (it.next().getKey().equals(node.getKey())) {
                it.remove();
                break;
              }
            }

            // if parent is not real node and has only one child
            // then they need to be merged.
            if (parent.getChildren().size() == 1 && parent.isReal() == false) {
              mergeNodes(parent, parent.getChildren().get(0));
            }
          } else if (node.getChildren().size() == 1) {
            // we need to merge the only child of this node with
            // itself
            mergeNodes(node, node.getChildren().get(0));
          } else { // we jus need to mark the node as non real.
            node.setReal(false);
          }
        }
      }

      /**
       * Merge a child into its parent node. Operation only valid if it is
       * only child of the parent node and parent node is not a real node.
       * 
       * @param parent
       *            The parent Node
       * @param child
       *            The child Node
       */
      private void mergeNodes(RadixTreeNode<T> parent, RadixTreeNode<T> child) {
        parent.setKey(parent.getKey() + child.getKey());
        parent.setReal(child.isReal());
        parent.setValue(child.getValue());
        parent.setChildren(child.getChildren());
      }

    };

    visit(key, visitor);

    if (visitor.getResult()) {
      size--;
    }
    return visitor.getResult().booleanValue();
  }

  /*
   * (non-Javadoc)
   * @see ds.tree.RadixTree#insert(java.lang.String, java.lang.Object)
   */
  //public void insert(String key, T value) throws DuplicateKeyException {
  public void insert(String key, T value) {
    try {
      insert(key, root, value);
    } catch (DuplicateKeyException e) {
      // re-throw the exception with 'key' in the message
      //throw new DuplicateKeyException("Duplicate key: '" + key + "'");
      
      System.err.println("Duplicate key: '" + key + "'");
      e.printStackTrace();
    }
    size++;
  }

  /**
   * Recursively insert the key in the radix tree.
   * 
   * @param key The key to be inserted
   * @param node The current node
   * @param value The value associated with the key 
   * @throws DuplicateKeyException If the key already exists in the database.
   */
  private void insert(String key, RadixTreeNode<T> node, T value)
      throws DuplicateKeyException {

    int numberOfMatchingCharacters = node.getNumberOfMatchingCharacters(key);

    // we are either at the root node
    // or we need to go down the tree
    if (node.getKey().equals("") == true
        || numberOfMatchingCharacters == 0
        || (numberOfMatchingCharacters < key.length() && numberOfMatchingCharacters >= node
            .getKey().length())) {
      boolean flag = false;
      String newText = key.substring(numberOfMatchingCharacters, key.length());
      for (RadixTreeNode<T> child : node.getChildren()) {
        if (child.getKey().startsWith(newText.charAt(0) + "")) {
          flag = true;
          insert(newText, child, value);
          break;
        }
      }

      // just add the node as the child of the current node
      if (flag == false) {
        RadixTreeNode<T> n = new RadixTreeNode<T>(node);
        n.setKey(newText);
        n.setReal(true);
        n.setValue(value);
        node.getChildren().add(n);
      }
    }
    // there is a exact match just make the current node as data node
    else if (numberOfMatchingCharacters == key.length()
        && numberOfMatchingCharacters == node.getKey().length()) {
      if (node.isReal() == true) {
        throw new DuplicateKeyException("Duplicate key");
      }

      node.setReal(true);
      node.setValue(value);
    }
    // This node need to be split as the key to be inserted
    // is a prefix of the current node key
    else if (numberOfMatchingCharacters > 0
        && numberOfMatchingCharacters < node.getKey().length()) {
      RadixTreeNode<T> n1 = new RadixTreeNode<T>(node);
      n1.setKey(node.getKey().substring(numberOfMatchingCharacters,
          node.getKey().length()));
      n1.setReal(node.isReal());
      n1.setValue(node.getValue());
      n1.setChildren(node.getChildren());

      node.setKey(key.substring(0, numberOfMatchingCharacters));
      node.setReal(false);
      node.setChildren(new ArrayList<RadixTreeNode<T>>());
      node.getChildren().add(n1);

      if (numberOfMatchingCharacters < key.length()) {
        RadixTreeNode<T> n2 = new RadixTreeNode<T>(node);
        n2.setKey(key.substring(numberOfMatchingCharacters, key.length()));
        n2.setReal(true);
        n2.setValue(value);

        node.getChildren().add(n2);
      } else {
        node.setValue(value);
        node.setReal(true);
      }
    }
    // this key need to be added as the child of the current node
    else {
      RadixTreeNode<T> n = new RadixTreeNode<T>(node);
      n.setKey(node.getKey().substring(numberOfMatchingCharacters,
          node.getKey().length()));
      n.setChildren(node.getChildren());
      n.setReal(node.isReal());
      n.setValue(node.getValue());

      node.setKey(key);
      node.setReal(true);
      node.setValue(value);

      node.getChildren().add(n);
    }
  }

  public ArrayList<T> searchPrefix(String key, int recordLimit) {
    ArrayList<T> keys = new ArrayList<T>();

    RadixTreeNode<T> node = searchPefix(key, root);

    if (node != null) {
      if (node.isReal()) {
        keys.add(node.getValue());
      }
      getNodes(node, keys, recordLimit);
    }

    return keys;
  }

  private void getNodes(RadixTreeNode<T> parent, ArrayList<T> keys, int limit) {
    Queue<RadixTreeNode<T>> queue = new LinkedList<RadixTreeNode<T>>();

    queue.addAll(parent.getChildren());

    while (!queue.isEmpty()) {
      RadixTreeNode<T> node = queue.remove();
      if (node.isReal() == true) {
        keys.add(node.getValue());
      }

      if (keys.size() == limit) {
        break;
      }

      queue.addAll(node.getChildren());
    }
  }

  private RadixTreeNode<T> searchPefix(String key, RadixTreeNode<T> node) {
    RadixTreeNode<T> result = null;

    int numberOfMatchingCharacters = node.getNumberOfMatchingCharacters(key);

    if (numberOfMatchingCharacters == key.length()
        && numberOfMatchingCharacters <= node.getKey().length()) {
      result = node;
    } else if (node.getKey().equals("") == true
        || (numberOfMatchingCharacters < key.length() && numberOfMatchingCharacters >= node
            .getKey().length())) {
      String newText = key.substring(numberOfMatchingCharacters, key.length());
      for (RadixTreeNode<T> child : node.getChildren()) {
        if (child.getKey().startsWith(newText.charAt(0) + "")) {
          result = searchPefix(newText, child);
          break;
        }
      }
    }

    return result;
  }

  public boolean contains(String key) {
    Visitor<T, Boolean> visitor = new VisitorImpl<T, Boolean>(Boolean.FALSE) {
      public void visit(String key, RadixTreeNode<T> parent,
          RadixTreeNode<T> node) {
        result = node.isReal();
      }
    };

    visit(key, visitor);

    return visitor.getResult().booleanValue();
  }

  /**
   * visit the node those key matches the given key
   * @param key The key that need to be visited
   * @param visitor The visitor object
   */
  public <R> void visit(String key, Visitor<T, R> visitor) {
    if (root != null) {
      visit(key, visitor, null, root);
    }
  }

  /**
   * recursively visit the tree based on the supplied "key". calls the Visitor
   * for the node those key matches the given prefix
   * 
   * @param prefix
   *            The key o prefix to search in the tree
   * @param visitor
   *            The Visitor that will be called if a node with "key" as its
   *            key is found
   * @param node
   *            The Node from where onward to search
   */
  private <R> void visit(String prefix, Visitor<T, R> visitor,
      RadixTreeNode<T> parent, RadixTreeNode<T> node) {

    int numberOfMatchingCharacters = node.getNumberOfMatchingCharacters(prefix);

    // if the node key and prefix match, we found a match!
    if (numberOfMatchingCharacters == prefix.length()
        && numberOfMatchingCharacters == node.getKey().length()) {
      visitor.visit(prefix, parent, node);
    } else if (node.getKey().equals("") == true // either we are at the
        // root
        || (numberOfMatchingCharacters < prefix.length() && numberOfMatchingCharacters >= node
            .getKey().length())) { // OR we need to
      // traverse the childern
      String newText =
          prefix.substring(numberOfMatchingCharacters, prefix.length());
      for (RadixTreeNode<T> child : node.getChildren()) {
        // recursively search the child nodes
        if (child.getKey().startsWith(newText.charAt(0) + "")) {
          visit(newText, visitor, node, child);
          break;
        }
      }
    }
  }

  public long getSize() {
    return size;
  }

  /**
   * Display the Trie on console.
   * 
   * WARNING! Do not use this for a large Trie, it's for testing purpose only.
   * @see formatTo
   */
  @Deprecated
  public void display() {
    formatNodeTo(new Formatter(System.out), 0, root);
  }

  @Deprecated
  private void display(int level, RadixTreeNode<T> node) {
    formatNodeTo(new Formatter(System.out), level, node);
  }

  /**
   * WARNING! Do not use this for a large Trie, it's for testing purpose only.
   */
  private void formatNodeTo(Formatter f, int level, RadixTreeNode<T> node) {
    for (int i = 0; i < level; i++) {
      f.format(" ");
    }
    f.format("|");
    for (int i = 0; i < level; i++) {
      f.format("-");
    }

    if (node.isReal() == true)
      f.format("%s[%s]*%n", node.getKey(), node.getValue());
    else
      f.format("%s%n", node.getKey());

    for (RadixTreeNode<T> child : node.getChildren()) {
      formatNodeTo(f, level + 1, child);
    }
  }

  /**
   * Writes a textual representation of this tree to the given formatter.
   * 
   * Currently, all options are simply ignored.
   * 
     * WARNING! Do not use this for a large Trie, it's for testing purpose only.
   */
  @Override
  public void formatTo(Formatter formatter, int flags, int width, int precision) {
    formatNodeTo(formatter, 0, root);
  }

  /**
   * Complete the a prefix to the point where ambiguity starts.
   * 
   *  Example:
   *  If a tree contain "blah1", "blah2"
   *  complete("b") -> return "blah"
   * 
   * @param prefix The prefix we want to complete
   * @return The unambiguous completion of the string.
   */
  public String complete(String prefix) {
    return complete(prefix, root, "");
  }

  private String complete(String key, RadixTreeNode<T> node, String base) {
    int i = 0;
    int keylen = key.length();
    int nodelen = node.getKey().length();

    while (i < keylen && i < nodelen) {
      if (key.charAt(i) != node.getKey().charAt(i)) {
        break;
      }
      i++;
    }

    if (i == keylen && i <= nodelen) {
      return base + node.getKey();
    } else if (nodelen == 0 || (i < keylen && i >= nodelen)) {
      String beginning = key.substring(0, i);
      String ending = key.substring(i, keylen);
      for (RadixTreeNode<T> child : node.getChildren()) {
        if (child.getKey().startsWith(ending.charAt(0) + "")) {
          return complete(ending, child, base + beginning);
        }
      }
    }

    return "";
  }

  public T lastMatchNode(String key) {
    
    RadixTreeNode<T> tmp = this.lastMatchNodeBase(key);
    return tmp == null ? null : tmp.getValue();
  }
  
  public RadixTreeNode<T> lastMatchNodeInTree(String key) {
    return this.lastMatchNodeBase(key);
  }
  
  /**
   * If path not in radix tree, return the last matched parent node.
   * @param key
   * @return
   */
  private RadixTreeNode<T> lastMatchNodeBase(String key) {
    //lastMatch = null;
    RadixTreeNode<T> tmp = this.lastMatchNode(key, root);
//    if (tmp == null && lastMatch != null) {
//      while (lastMatch != null && !lastMatch.isReal()) {
//        lastMatch = lastMatch.getParent();
//      }
//    }
    
   //System.out.println(key + ", 1)lastMatchNode is " + tmp.getKey());
    if (tmp != null) {
      while (true){
        if (tmp == null || tmp.isReal()) break;
        tmp = tmp.getParent();
      }
//      if (tmp!=null && !key.endsWith(tmp.getKey())){
//        tmp = null;
//      }
      //System.out.println(key.endsWith(tmp.getKey()) + key + ", 2)lastMatchNode is " + (tmp == null ? null : tmp.getKey()));
    }
    return tmp;
  }

  /**
   * Check if legal, avoid this case: key: /test/user/a1 match /test/user/a
   * @param p1
   * @param p2
   * @return
   */
  private boolean isLegal(String p1, String p2) {
    int l1 = NameNodeDummy.getNameNodeDummyInstance().calculateSlashCount(p1);
    int l2 = NameNodeDummy.getNameNodeDummyInstance().calculateSlashCount(p2);
    if ((l1 == l2 && p1.length() != p2.length()) || (p1.startsWith(p2) && (p1.length() > p2.length() && p1.charAt(p2.length()) != '/'))){
      return false;
    }
    return true;
    //return (l1 == l2 && p1.length() != p2.length() ? false : true);
  }
  //private RadixTreeNode<T> lastMatch = null;

  private RadixTreeNode<T> lastMatchNode(String key, RadixTreeNode<T> node) {
    RadixTreeNode<T> result = null;
    int numberOfMatchingCharacters = node.getNumberOfMatchingCharacters(key);
    //RadixTreeNode<T> lastMatch = node;
    //Exactly match!
    if (numberOfMatchingCharacters == key.length()
        && numberOfMatchingCharacters <= node.getKey().length()) {
//      System.out.println(key + " vs " + node.getKey() + " : Match " + node.getParent());
//      if (node.getKey().length() > key.length() && node.getKey().charAt(key.length()) != '/'){
//        System.err.println("Not legal natural node " + node.getKey());
//      } else
      result = node;
    } else if (node.getKey().equals("") == true
        || (numberOfMatchingCharacters < key.length() && numberOfMatchingCharacters >= node
            .getKey().length())) {
      String newText = key.substring(numberOfMatchingCharacters, key.length());
      for (RadixTreeNode<T> child : node.getChildren()) {
        char a = newText.charAt(0);
        //System.out.println(">>>" + child.getKey() + ":: " + newText);
        if (child.getKey().startsWith(a + "")) {
          //if (a == '/' && child.getKey().length() > 1 && newText.length() > 1 && newText.charAt(1) != child.getKey().charAt(1)) break;
          result = lastMatchNode(newText, child);
         
          break;
        }
      }
    }
    
    //System.out.println("[lastMatchNode] key " + key + ", node is " + node.getKey() + ", parent:" + (isLegal(key,node.getKey()) ? node : node.getParent()));
    return result == null ? node : result;
    //return result == null ? (isLegal(key,node.getKey()) ? node : this.getParent(node.getParent(),key)) : result;
  }

  private RadixTreeNode<T> getParent(RadixTreeNode<T> node, String key) {
    while(true){
      if(node == null || (node.isReal() && this.isLegal(key, node.getKey()))){
        System.out.println("Found legal node " + node.getKey());
        return node;
      }
      node = node.getParent();
    }
  }
}