
package org.apache.hadoop.hdfs.server.namenode.dummy.tree;

import java.util.ArrayList;
import java.util.List;


public class RadixTreeNode<T> {
    private String key;

    private List<RadixTreeNode<T>> children;

    private boolean real;

    private T value;
    
    private RadixTreeNode<T> parent = null;

    /**
     * Initialize the fields with default values to avoid null reference checks
     * all over the places
     */
    public RadixTreeNode(RadixTreeNode<T> parent) {
        key = "";
        children = new ArrayList<RadixTreeNode<T>>();
        real = false;
        this.parent = parent;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T data) {
        this.value = data;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String value) {
        this.key = value;
    }

    public boolean isReal() {
        return real;
    }

    public void setReal(boolean datanode) {
        this.real = datanode;
    }

    public List<RadixTreeNode<T>> getChildren() {
        return children;
    }

    public void setChildren(List<RadixTreeNode<T>> children) {
        this.children = children;
    }
    

	public int getNumberOfMatchingCharacters(String key) {
		int numberOfMatchingCharacters = 0;
        while (numberOfMatchingCharacters < key.length() && numberOfMatchingCharacters < this.getKey().length()) {
            if (key.charAt(numberOfMatchingCharacters) != this.getKey().charAt(numberOfMatchingCharacters)) {
                break;
            }
            numberOfMatchingCharacters++;
        }
		return numberOfMatchingCharacters;
	}

    
    @Override
    public String toString() {
		return key;
    	
    }

    public RadixTreeNode<T> getParent() {
      return parent;
    }

    public void setParent(RadixTreeNode<T> parent) {
      this.parent = parent;
    }
}
