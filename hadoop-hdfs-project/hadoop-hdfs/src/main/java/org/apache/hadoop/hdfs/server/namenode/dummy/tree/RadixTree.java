
package org.apache.hadoop.hdfs.server.namenode.dummy.tree;

import java.util.ArrayList;

public interface RadixTree<T> {
    /**
     * Insert a new string key and its value to the tree.
     * 
     * @param key
     *            The string key of the object
     * @param value
     *            The value that need to be stored corresponding to the given
     *            key.
     * @throws DuplicateKeyException
     */
    public void insert(String key, T value) throws DuplicateKeyException;

    /**
     * Delete a key and its associated value from the tree.
     * @param key The key of the node that need to be deleted
     * @return
     */
    public boolean delete(String key);

    /**
     * Find a value based on its corresponding key.
     * 
     * @param key The key for which to search the tree.
     * @return The value corresponding to the key. null if it can not find the key
     */
    public T find(String key);

    /**
     * Check if the tree contains any entry corresponding to the given key.
     * 
     * @param key The key that needto be searched in the tree.
     * @return retun true if the key is present in the tree otherwise false
     */
    public boolean contains(String key);

    /**
     * Search for all the keys that start with given prefix. limiting the results based on the supplied limit.
     * 
     * @param prefix The prefix for which keys need to be search
     * @param recordLimit The limit for the results
     * @return The list of values those key start with the given prefix
     */
    public ArrayList<T> searchPrefix(String prefix, int recordLimit);
    
    /**
     * Return the size of the Radix tree
     * @return the size of the tree
     */
    public long getSize(); 
    
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
    public String complete(String prefix);
}
