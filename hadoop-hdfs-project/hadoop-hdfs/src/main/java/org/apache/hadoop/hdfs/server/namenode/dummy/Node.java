package org.apache.hadoop.hdfs.server.namenode.dummy;


public class Node<T>
{
   public T key;
   public Node<T> left, right;
   public boolean color;
   public int N;             // subtree count
   public ExternalStorage val;
   public Node(T data, Node<T> l, Node<T> r)
   {
      left = l; right = r;
      this.key = data;
   }
   
   public Node(T data, ExternalStorage val, boolean color, int N)
   {
      //left = l; right = r;
      this.val = val;
      this.key = data;
      this.color = color;
      this.N = N;
   }

   public Node(T data)
   {
      this(data, null, null);
   }

   

  public String toString()
   {
      return key.toString();
   }
} //end of Node
//end of BST
