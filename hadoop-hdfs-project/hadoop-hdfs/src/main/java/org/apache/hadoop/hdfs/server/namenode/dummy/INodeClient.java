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
	private static Map<String, INodeClient> nioClients = new ConcurrentHashMap<String, INodeClient>();
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

	public static INodeClient getInstance(String server, int tcpPort,
			int udpPort) {
		if (nioClients.get(server) == null) {
			synchronized (obj) {
				if (nioClients.get(server) == null) {
					nioClients.put(server, new INodeClient(server, tcpPort,
							udpPort));
				}
			}
		}
		return nioClients.get(server);
	}

	private INodeClient(String server, int tcpPort, int udpPort) {
		this.server = server;
		this.tcpPort = tcpPort;
		this.udpPort = udpPort;
		this.nnd = NameNodeDummy.getNameNodeDummyInstance();
	}

	static {
		// Log.set(Log.LEVEL_TRACE);
	}

	/**
	 * Communication from name node to name node.
	 * 
	 * @throws IOException
	 */
	public void connect() throws IOException {
		this.client = new Client(INodeServer.WRITE_BUFFER,
				INodeServer.OBJECT_BUFFER);

		client.addListener(new Listener() {
			public void received(Connection connection, Object object) {
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
							System.out.println("Cannot run clean up!");
						}
					}
				}
			}

			public void disconnected(Connection c) {
				System.out.println("Disconnected be invoked!");
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

		this.nnd.logs(out,
				"Send tcp package:" + this.nnd.humanReadableByteCount(size));

		/**
		 * try { client.update(INodeServer.KEEP_ALIVE); } catch (IOException e)
		 * { e.printStackTrace(); }
		 **/

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
		if(this.out == null) this.out = out;
		boolean ifSuccess = false;
		this.subTree = subTree;
		INodeDirectory parent = subTree.getParent();
		try {
			/**
			 * Before send sub-tree, divorce INodeExternalLink first.
			 */
			ExternalStorageMapping es = new ExternalStorageMapping(this.server,
					NameNodeDummy.getNameNodeDummyInstance().getOriginalBpId(),
					this.subTree.getFullPathName(), NameNodeDummy
							.getNameNodeDummyInstance().getNamenodeAddress()
							.getHostName());
			String src = INodeExternalLink.PREFIX + this.subTree.getLocalName();
			INode l = this.subTree.getParent().getChild(src.getBytes(), Snapshot.CURRENT_STATE_ID);
			if(l!=null&&l.isExternalLink()){
				System.out.println("[INodeClient]:found existing ExternalLink "+l.getFullPathName()+";es="+l.asExternalLink().getEsMap());
				this.link = l.asExternalLink();
				this.link.addToEsMap(es.getRoot());
			}
			else
			this.link = INodeExternalLink.getInstance(this.subTree, es , src);
			
			NameNodeDummy.getNameNodeDummyInstance().filterExternalLink(
					this.subTree, link, parent);

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
					this.nnd.logs(out,
							"Found parent path " + temp.getLocalName());

					request.addId(temp.getId());
					request.addLocalName(temp.getLocalName());
					request.addMtime(temp.getModificationTime());
					request.addUser(temp.getUserName());
					request.addGroup(temp.getGroupName());
					request.addMode(temp.getFsPermissionShort());
					request.addAccessTime(temp.getAccessTime());
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
			subTree.setParent(null);

			splitTree.intelligentSplitToSmallTree(subTree, 90000, 0);

			Map<Integer, SubTree> map = splitTree.getMap();

			int size = map.size();

			if (response > 0) {

				/** Prepare to send all namespace sub-tree **/
				Iterator<Entry<Integer, SubTree>> iter = map.entrySet()
						.iterator();
				while (iter.hasNext()) {
					Entry<Integer, SubTree> entry = iter.next();
					Integer id = entry.getKey();
					SubTree sub = entry.getValue();
					MapRequest mapRequest = new MapRequest(id, sub, size);
					this.nnd.logs(out, "Sending "
							+ mapRequest.getSubtree().getInode().getLocalName()
							+ " to server " + this.server);

					this.sendTCP(mapRequest, out);
				}
			}

			ifSuccess = true;

		} catch (Exception e) {
			this.nnd.logs(out,
					"Cannot send sub-tree " + subTree.getFullPathName()
							+ " to server " + this.server + ";Error message:"
							+ e.getMessage());
			this.subTree = null;
			this.close();
			nioClients.remove(this.server);
		} finally {
			// Reference
			subTree.setParent(parent);
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * notify source namenode update overflowing table.
	 * @throws Exception 
	 */
	private void notifySourceNNUpdate(JspWriter out) throws Exception{
		if(!subTree.isDirectory()) throw new Exception("Unknow Error!");
		String path = this.subTree.getLocalName();
		String server = path.substring(INodeServer.PREFIX.length(),path.length());
		ReadOnlyList<INode> roList = this.subTree.asDirectory().getChildrenList(
				Snapshot.CURRENT_STATE_ID);
		String[] srcs = new String[roList.size()];
		String parent = this.subTree.getParent().getFullPathName();
		for (int i = 0; i < roList.size(); i++) {
			INode inode = roList.get(i);
			srcs[i] = parent+inode.getLocalName();
			System.out.println("[INodeClient]notifySourceNNUpdate: send path = "+srcs[i]);
		}
		NameNodeDummy.getNameNodeDummyInstance().sendToNN(server, out, this.server, NameNodeDummy
								.getNameNodeDummyInstance()
								.getNamenodeAddress().getHostName(), srcs);
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
				ExternalStorageMapping es = new ExternalStorageMapping(
						this.server, NameNodeDummy.getNameNodeDummyInstance()
								.getOriginalBpId(),
						this.subTree.getFullPathName(), NameNodeDummy
								.getNameNodeDummyInstance()
								.getNamenodeAddress().getHostName());
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
					+ ";es=" + this.link.getEsMap().length);
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
