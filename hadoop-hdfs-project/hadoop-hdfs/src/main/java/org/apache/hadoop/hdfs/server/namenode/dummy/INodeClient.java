package org.apache.hadoop.hdfs.server.namenode.dummy;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.jsp.JspWriter;

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeExternalLink;
import org.apache.hadoop.hdfs.server.namenode.NameNodeDummy;
import org.apache.hadoop.hdfs.server.namenode.Quota;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.security.AccessControlException;

import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;

/**
 * Send namespace tree to another namenode, NIO client.
 * 
 * @author Ray Zhang
 *
 */
public class INodeClient {
  private final static long SIZE_TO_SPLIT = 1l;
  private static Map<String, INodeClient> nioClients =
      new ConcurrentHashMap<String, INodeClient>();
  private Client client = null;
  private NameNodeDummy nnd = null;
  private String server;
  private int tcpPort;
  private int udpPort;
  private INode subTree;
  private static Object obj = new Object();
  private INodeExternalLink link;
  private boolean isReponsed = false;
  private boolean sendDone = false;
  private boolean existingNamespace = false;
  private JspWriter out;
  // Retry times for BufferOverflowException.
  private int MAX_RETRY = 6;
  private int retry = 0;

  public static INodeClient getInstance(String server, int tcpPort, int udpPort) {
    INodeClient client = nioClients.get(server);
    if (client != null
        && (client.client == null || !client.client.isConnected())) {
      nioClients.remove(server);
      client = null;
    }
    if (client == null) {
      synchronized (obj) {
        if (nioClients.get(server) == null) {
          nioClients.put(server, new INodeClient(server, tcpPort, udpPort));
        }
      }
    }

    return nioClients.get(server);
  }

  public INodeClient(String server, int tcpPort, int udpPort) {
    this.server = server;
    this.tcpPort = tcpPort;
    this.udpPort = udpPort;
    this.nnd = NameNodeDummy.getNameNodeDummyInstance();
  }

  static {
    com.esotericsoftware.minlog.Log.set(com.esotericsoftware.minlog.Log.LEVEL_TRACE);
    com.esotericsoftware.minlog.Log.TRACE();
  }

  /**
  public static void main(String[] args) {
  	
  	long start = System.currentTimeMillis();
  	INodeClient client = INodeClient.getInstance("localhost",
  			NameNodeDummy.TCP_PORT, NameNodeDummy.UDP_PORT);
  	
  	
  	IMocksControl control = EasyMock.createControl();
  	//INode inode = control.createMock(INode.class);
  	INodeDirectory root = Divide.simpleInodeDir();
  	INode out = Divide.simpleInode();
  	INode out2 = Divide.simpleInode();
  	INode out3 = Divide.moreInode();
  	INode out4 = Divide.moreInode();
  	root.addChild(out);
  	INode inode = Divide.simpleInode();
  	out.asDirectory()
  	.getChildrenList(Snapshot.CURRENT_STATE_ID).get(0).asDirectory().addChild(inode);
  	
  	out.asDirectory()
  	.getChildrenList(Snapshot.CURRENT_STATE_ID).get(1).asDirectory().addChild(out2);
  	out.asDirectory()
  	.getChildrenList(Snapshot.CURRENT_STATE_ID).get(2).asDirectory().addChild(out3);
  	root.addChild(out4);
  	inode.setParent(out.asDirectory()
  			.getChildrenList(Snapshot.CURRENT_STATE_ID).get(0).asDirectory());
  	inode.getFullPathName();
  	//EasyMock.expectLastCall().andReturn("/data1").times(2);
  	
  	inode.getLocalNameBytes();
  	//EasyMock.expectLastCall().andReturn("test".getBytes());
  	
  	
  	NameNode nn = control.createMock(NameNode.class);
  	InetSocketAddress byAddress1 = new InetSocketAddress("localhost", NameNodeDummy.TCP_PORT);
  	//nn.getNameNodeAddress();
  	
  	
  	NameNodeDummy.getNameNodeDummyInstance().setNameNode(nn);
  	nn.getNameNodeAddress();
  	EasyMock.expectLastCall().andReturn(byAddress1).times(1);
  	
  	
  	NameNodeDummy.getNameNodeDummyInstance().getNamenodeAddress();
  	
  	
  	EasyMock.expectLastCall().andReturn(byAddress1);
  	
  	control.replay();
  	//inode.setLocalName("test".getBytes());

  	System.out.println(client.nnd.printNSInfo(root, 0, 10).replaceAll("&nbsp;", " ").replaceAll("<br/>", "\n"));
  	client.sendINode(inode, null, false);
  	//control.verify();
  	
  	System.out.println("Totally spend " + (System.currentTimeMillis()-start));
  }
  **/

  /**
   * Communication from name node to name node.
   * 
   * @throws IOException
   */
  public void connect() throws IOException {
    this.client =
        new Client(INodeServer.WRITE_BUFFER, INodeServer.OBJECT_BUFFER);
    client.addListener(new Listener() {
      public void received(Connection connection, Object object) {
        //System.out.println("Client received " + object.getClass().getName());
        if (object instanceof MoveNSResponse) {
          MoveNSResponse response = (MoveNSResponse) object;
          INodeClient.this.nnd.setOriginalBpId(response.getPoolId());
          isReponsed = true;
        } else if (object instanceof ClientCommends) {
          ClientCommends response = (ClientCommends) object;
          //if (NameNodeDummy.DEBUG)
          System.out.println("Get ClientCommends from server "
              + response.getCommand());
          if (response.getCommand() == 0) {

          } else if (response.getCommand() == 1) {
            if (response.getListSize() == listSize)
              sendDone = true;

          }
        }
      }

      public void disconnected(Connection c) {
        if (NameNodeDummy.DEBUG)
          NameNodeDummy.debug("Disconnected be invoked!");
        super.disconnected(c);
      }
    });

    client.setKeepAliveTCP(INodeServer.KEEP_ALIVE);

    client.setTimeout(INodeServer.TIME_OUT);

    // Add it after added listener
    INodeServer.register(client.getKryo());

    client.start();
    client.connect(INodeServer.TIME_OUT, server, tcpPort, udpPort);
    //client.update(INodeServer.TIME_OUT);
  }

  public void close() {
    if (this.client != null) {
      this.client.close();
      this.client = null;
    }
  }

  /**
   * 
   * @param obj
   * @param out
   * @return Size of sent data.
   * @throws Exception 
   */
  public int sendTCP(Object obj, JspWriter out) throws Exception {

    int size = -1;
    try {
      size = client.sendTCP(obj);
      if (size == 0) throw new Exception("Send size should not be zero!");
      if (!this.client.isConnected()) throw new Exception("Lost connection, try to reconnect...");
      //size = client.sendUDP(obj);
    } catch (Exception e) {
      //e.printStackTrace();
      System.err.println(e.getMessage());
      if (obj instanceof org.apache.hadoop.hdfs.server.namenode.dummy.MapRequest) {
        System.err.println("[ERROR] obj = " + ((MapRequest) obj).getKey());
      }

      try {
        Thread.sleep(100);
      } catch (InterruptedException e1) {
        e1.printStackTrace();
      }
      if (retry < MAX_RETRY) {
        System.out.println("-----Retry now ..." + this.client.isConnected());
        retry++;
        if (this.client == null || !this.client.isConnected())
          this.connect();
        size = this.sendTCP(obj, out);
        //Clear memory.
        //obj = null;
      } else {
        retry = 0;
        System.err.println("[ERROR] Retry failed!");
        //this.close();
        //this.connect();
      }

    } finally {
      obj = null;
    }

    if (NameNodeDummy.DEBUG)
      if (obj instanceof org.apache.hadoop.hdfs.server.namenode.dummy.MapRequest) {
        System.out.println("obj = " + ((MapRequest) obj).getKey());
      }
    if (NameNodeDummy.DEBUG)
    if (out != null) {
      if (obj instanceof org.apache.hadoop.hdfs.server.namenode.dummy.MapRequest
          && ((MapRequest) obj).getKey() % 1000 == 0) {
        this.nnd.logs(out,
            "Send tcp package:" + this.nnd.humanReadableByteCount(size));
      }
    } else {
      NameNodeDummy.info(obj.getClass().getName() + " , Send tcp package:"
          + this.nnd.humanReadableByteCount(size));
    }
    retry = 0;
    return size;
  }

  private int listSize = -1;

  /**
   * Send namespace to another namenode server
   * 
   * @param subTree
   * @param out
   * @return
   */
  public boolean sendINode(INode subTree, JspWriter out, boolean isParentRoot) {
    long start = System.currentTimeMillis();
    if (this.out == null)
      this.out = out;
    boolean ifSuccess = false;
    this.subTree = subTree;
    INodeDirectory parent = subTree.getParent();
    try {
      /**
       * Before send sub-tree, divorce INodeExternalLink first.
       */
      ExternalStorageMapping es =
          new ExternalStorageMapping(this.server, NameNodeDummy
              .getNameNodeDummyInstance().getOriginalBpId(),
              this.subTree.getFullPathName(), NameNodeDummy
                  .getNameNodeDummyInstance().getNamenodeAddress()
                  .getHostName());

      String src = INodeExternalLink.PREFIX + this.subTree.getLocalName();
      INodeDirectory t = this.subTree.getParent();
      boolean isLink = false;
      if (t != null) {
        INode l = t.getChild(src.getBytes(), Snapshot.CURRENT_STATE_ID);
        if (l != null && l.isExternalLink()) {
          if (NameNodeDummy.DEBUG)
            NameNodeDummy.debug("[INodeClient]:found existing ExternalLink "
                + l.getFullPathName() + ";es=" + l.asExternalLink().getEsMap());
          this.link = l.asExternalLink();
          this.link.addToEsMap(es.getRoot());
          isLink = true;
        }
      }
      if (!isLink)
        this.link = INodeExternalLink.getInstance(this.subTree, es, src);

      /**
       * Handle overflow talbe
       */
      NameNodeDummy.getNameNodeDummyInstance().filterExternalLink(this.subTree,
          link, parent);

      /**
       * Send parent path information first
       */

      MoveNSRequest request = new MoveNSRequest();
      request.setOperation(0);
      if (subTree.getId() == 1
          && subTree.getLocalName().startsWith(INodeServer.PREFIX)) {
        existingNamespace = true;
        request.setNamespace(subTree.getLocalName());
      }
      if (!isParentRoot) {
        INode temp = subTree;
        while (temp != null && temp.getLocalNameBytes().length != 0) {
          if (out != null)
            this.nnd.logs(out, "Found parent path " + temp.getLocalName());
          else {
            NameNodeDummy.debug("Found parent path " + temp.getLocalName());
          }
          request.addId(temp.getId());
          request.addLocalName(temp.getLocalName());
          request.addMtime(temp.getModificationTime());
          request.addUser(temp.getUserName());
          request.addGroup(temp.getGroupName());
          request.addMode(temp.getFsPermissionShort());
          request.addAccessTime(temp.getAccessTime());
          final Quota.Counts q = temp.getQuotaCounts();
          final long nsQuota = q.get(Quota.NAMESPACE);
          final long dsQuota = q.get(Quota.DISKSPACE);
          request.addNsQuota(nsQuota);
          request.addDsQuota(dsQuota);
          temp = temp.getParent();
          // out.println("Parent folder "+temp.getFullPathName());
        }
      }

      /**
       * Send the namespace tree
       */
      SplitTree splitTree = new SplitTree();

      // Temporary unlink parent reference, has to been fix later.
      //subTree.setParent(null);

      splitTree.intelligentSplitToSmallTree(subTree, SIZE_TO_SPLIT, 0);

      List<MapRequest> list = splitTree.getSplittedNodes();

      int size = list.size();
      listSize = size;
      request.setListSize(size);

      if (this.client == null || !this.client.isConnected())
        this.connect();
      int response = this.sendTCP(request, out);
      if (NameNodeDummy.DEBUG)
        System.out.println("; list size is " + size);
      int remain = size;
      int count = 0;
      if (response > 0) {

        /** Prepare to send all namespace sub-tree **/
        Iterator<MapRequest> iter = list.iterator();
        MapRequest[] sendArray = this.getSendArray(size);
        int i = 0;
        int len = sendArray.length -1;
        while (iter.hasNext()) {
        //for (int i = 0; i < size; i++) {
          MapRequest mapRequest = iter.next();
          sendArray[i] = mapRequest;
          if (i == len) {
            i = 0;
            count++;
            int s = this.sendTCP(sendArray, out);
            //if (NameNodeDummy.TEST) {
            System.out.println(remain + " = remain, send tcp size " + s + ", count is " + count + ", array len is " + len);
           // }
            remain = remain - sendArray.length;
            if (remain == 0) {
              break;
            }
            sendArray = this.getSendArray(remain);
            len = sendArray.length -1; 
          } else {
            i++;
          }
          //MapRequest mapRequest = iter.next();
          //if (out != null)
          // this.nnd.logs(out, "Sending "
          //System.out.println("Sending " + mapRequest.getKey() + " to server "
            //  + this.server + "; send object list size is " + size);
          /**
          else {
            if (NameNodeDummy.DEBUG)
              System.out.println("Sending " + mapRequest.getKey()
                  + " to server " + this.server + "; send object list size is "
                  + size);
          }
          **/
            
          //mapRequest = null;
        }
      }

      ifSuccess = true;
      list.clear();
      list = null;
      System.out.println("Client successfully send all data: " + size);
      if (out != null)
      NameNodeDummy.getNameNodeDummyInstance().logs(out, "Send INode spend " + (System.currentTimeMillis() - start)
          + " milliseconds!" + count);
      System.out.println("Send INode spend " + (System.currentTimeMillis() - start)
          + " milliseconds!" + count);

    } catch (Exception e) {
      System.err.println("Failed to send namespace: " + e.getMessage());
      if (out != null)
        this.nnd.logs(out, "Cannot send sub-tree " + subTree.getFullPathName()
            + " to server " + this.server + ";Error message:" + e.getMessage());
      else {
        System.err.println("Cannot send sub-tree " + subTree.getFullPathName()
            + " to server " + this.server + ";Error message:" + e.getMessage());
      }
      //For test only!
      e.printStackTrace();
      this.subTree = null;
      this.close();
      nioClients.remove(this.server);
    } finally {
      // Reference
      //subTree.setParent(parent);
      /**
       * Reset reference for INodeExternalLink, no need now, have handled
       */
      //NameNodeDummy.getNameNodeDummyInstance().recoverExternalLink();
      ifSuccess = this.waitServerReceivedAllData();
    }

    return ifSuccess;
  }

  private MapRequest[] getSendArray(int size) {
    MapRequest[] sendArray = null;
    if (size > INodeServer.MAX_GROUP){
      sendArray = new MapRequest[INodeServer.MAX_GROUP];
    } else {
      sendArray = new MapRequest[size];
    }

    //if (NameNodeDummy.TEST)
    //System.out.println("Group size is " + sendArray.length);
    return sendArray;
  }
  private void waitResponseFromTargetNN() {
    while (!isReponsed) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Wait until server response received all the datas.
   */
  private boolean waitServerReceivedAllData() {
    boolean success = true;
    int waitLoops = 30 * 10;
    int i = 0;
    while (!sendDone) {
      i++;
      if (i > waitLoops) {
        System.err
            .println("Error! Server didn't received all the datas. Move namespace failed!");
        success = false;
        break;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    if (sendDone && i < waitLoops) {
      System.out.println("Server has successfully received all the datas!");
    }

    return success;
  }

  /**
   * notify source namenode update overflowing table.
   * @throws Exception 
   */
  private void notifySourceNNUpdate(JspWriter out) throws Exception {
    if (!subTree.isDirectory())
      throw new Exception("Unknow Error!");
    String path = this.subTree.getLocalName();
    String server = path.substring(INodeServer.PREFIX.length(), path.length());
    ReadOnlyList<INode> roList =
        this.subTree.asDirectory().getChildrenList(Snapshot.CURRENT_STATE_ID);
    String[] srcs = new String[roList.size()];
    String parent = this.subTree.getParent().getFullPathName();
    Iterator<INode> ite = roList.iterator();
    //for (int i = 0; i < roList.size(); i++) {
    int i = 0;
    while (ite.hasNext()) {
      //INode inode = roList.get(i);
      INode inode = ite.next();
      srcs[i++] = parent + inode.getLocalName();
      if (NameNodeDummy.DEBUG)
        System.out.println("[INodeClient]notifySourceNNUpdate: send path = "
            + srcs[i]);
    }
    NameNodeDummy.getNameNodeDummyInstance().sendToNN(
        server,
        out,
        this.server,
        NameNodeDummy.getNameNodeDummyInstance().getNamenodeAddress()
            .getHostName(), srcs);
  }

  private void clean() throws Exception {

    boolean success = this.waitServerReceivedAllData();
    if (success) {
      this.cleanup();
    } else {
      System.err
          .println("Server failed to receive all the datas, so don't delete namespace on source node!");
    }

  }

  public void cleanup() throws Exception {

    if (this.existingNamespace) {
      System.out.println("Found existing namespace "
          + this.subTree.getFullPathName());
      this.notifySourceNNUpdate(this.out);
      this.existingNamespace = false;
    } else if (this.subTree != null) {
      waitResponseFromTargetNN();
      System.out.println("Removing sub-tree from the source NN: "
          + this.subTree.getFullPathName());
      if (this.link == null) {
        ExternalStorageMapping es =
            new ExternalStorageMapping(this.server, NameNodeDummy
                .getNameNodeDummyInstance().getOriginalBpId(),
                this.subTree.getFullPathName(), NameNodeDummy
                    .getNameNodeDummyInstance().getNamenodeAddress()
                    .getHostName());
        String src = INodeExternalLink.PREFIX + this.subTree.getLocalName();
        this.link = INodeExternalLink.getInstance(this.subTree, es, src);
      }
      if (this.link.getEsMap().length > 0)
        this.link.getEsMap()[0].setTargetNNPId(NameNodeDummy
            .getNameNodeDummyInstance().getOriginalBpId());
      System.out.println(NameNodeDummy.getNameNodeDummyInstance()
          .getNamenodeAddress().getHostName()
          + "Before delete add INodeExternalLink:"
          + this.link
          + ";es="
          + this.link.getEsMap().length);
      NameNodeDummy.getNameNodeDummyInstance().buildOrAddBST(
          this.link.getEsMap());
      NameNodeDummy.getNameNodeDummyInstance().addExternalNode(this.link,
          this.subTree.getParent());
    }

    // Delete namespace tree in memory.
    NameNodeDummy.getNameNodeDummyInstance().deletePath(
        this.subTree.getFullPathName());
    // Clear memory
    if (this.subTree != null){
      // This subTree must be a directory
      if (this.subTree.isDirectory())
        this.subTree.asDirectory().clear();
      if (this.subTree.getParent() != null) {
        this.subTree.getParent().removeChild(this.subTree);
      }
    } 
    this.subTree = null;
    
    try {
      NameNodeDummy.getNameNodeDummyInstance().saveNamespace();
    } catch (AccessControlException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }  finally {
      // No guarantee.
      System.gc();
    }
  }

}
