package org.apache.hadoop.hdfs.server.namenode.dummy;

/**
 * 
 * @author rzhan33
 *
 * @param <T> Value of this tree.
 * @param <R> Tree node.
 */
public interface IOverflowTable<T, R> {
  /**
   * Build the tree from scratch or to an existing one.
   * @param es
   */
  public R buildOrAddBST(T[] es);

  /**
   * Find node by given key.
   * @param key
   * @param createIfNothere
   * @param alwaysReturnParent
   * @return
   */
  public T findNode(String key);
  
  /**
   * If cannot find the key, return last matched parent node.
   * @param key
   * @return
   */
  public T findLastMatchedNode(String key);

  /**
   * Insert a node.
   * @param key
   * @param value
   * @return
   */
  public void insert(String key, T value);

  /**
   * Delete a node.
   * @param key
   * @return
   */
  public boolean remove(String key);
  
  /**
   * Print out radix tree.
   */
  public void display();
  
  /**
   * Find all the values for the giving path
   * @return
   */
  public T[] findAllValues(String key);
}
