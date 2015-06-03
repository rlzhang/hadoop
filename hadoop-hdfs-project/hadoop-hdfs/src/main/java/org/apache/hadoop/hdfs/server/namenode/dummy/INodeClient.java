package org.apache.hadoop.hdfs.server.namenode.dummy;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
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
  private final static int SIZE_TO_SPLIT = 10000;
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
  private boolean existingNamespace = false;
  private JspWriter out;

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
    //com.esotericsoftware.minlog.Log.set(com.esotericsoftware.minlog.Log.LEVEL_TRACE);
    //com.esotericsoftware.minlog.Log.TRACE();
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
          if (response.getCommand() == 0) {
            try {
              INodeClient.this.cleanup();
            } catch (Exception e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
              System.err.println("Cannot run clean up!");
            }
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
   */
  public int sendTCP(Object obj, JspWriter out) {

    int size = client.sendTCP(obj);

    if (out != null)
      this.nnd.logs(out,
          "Send tcp package:" + this.nnd.humanReadableByteCount(size));
    else {
      NameNodeDummy.info(obj.getClass().getName() + " , Send tcp package:"
          + this.nnd.humanReadableByteCount(size));
    }

    return size;
  }

  /**
   * Send namespace to another namenode server
   * 
   * @param subTree
   * @param out
   * @return
   */
  public boolean sendINode(INode subTree, JspWriter out, boolean isParentRoot) {
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

      if (this.client == null || !this.client.isConnected())
        this.connect();
      int response = this.sendTCP(request, out);

      /**
       * Send the namespace tree
       */
      SplitTree splitTree = new SplitTree();

      // Temporary unlink parent reference, has to been fix later.
      //subTree.setParent(null);

      splitTree.intelligentSplitToSmallTree(subTree, SIZE_TO_SPLIT, 0);

      Map<Integer, SubTree> map = splitTree.getMap();

      int size = map.size();

      if (response > 0) {

        /** Prepare to send all namespace sub-tree **/
        Iterator<Entry<Integer, SubTree>> iter = map.entrySet().iterator();
        while (iter.hasNext()) {
          Entry<Integer, SubTree> entry = iter.next();
          Integer id = entry.getKey();
          SubTree sub = entry.getValue();
          MapRequest mapRequest = new MapRequest(id, sub, size);
          if (out != null)
            this.nnd.logs(out, "Sending "
                + mapRequest.getSubtree().getInode().getLocalName()
                + " to server " + this.server);

          this.sendTCP(mapRequest, out);
        }
      }

      ifSuccess = true;

    } catch (Exception e) {
      if (out != null)
        this.nnd.logs(out, "Cannot send sub-tree " + subTree.getFullPathName()
            + " to server " + this.server + ";Error message:" + e.getMessage());
      else {
        NameNodeDummy.debug("Cannot send sub-tree " + subTree.getFullPathName()
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
       * Reset reference for INodeExternalLink
       */
      NameNodeDummy.getNameNodeDummyInstance().recoverExternalLink();
    }

    return ifSuccess;
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
    for (int i = 0; i < roList.size(); i++) {
      INode inode = roList.get(i);
      srcs[i] = parent + inode.getLocalName();
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

  public void cleanup() throws Exception {
    //Wait to clean
    try {
      Thread.sleep(10 * 1000);
    } catch (Exception e) {

    }
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

    NameNodeDummy.getNameNodeDummyInstance().deletePath(
        this.subTree.getFullPathName());
    try {
      NameNodeDummy.getNameNodeDummyInstance().saveNamespace();
    } catch (AccessControlException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
