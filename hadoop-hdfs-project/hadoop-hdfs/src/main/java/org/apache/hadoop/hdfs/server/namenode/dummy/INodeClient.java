package org.apache.hadoop.hdfs.server.namenode.dummy;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.jsp.JspWriter;

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeDummy;

import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.minlog.Log;

/**
 * Send namespace tree to another namenode, NIO client.
 * @author Ray Zhang
 *
 */
public class INodeClient {
	private final static int TIME_OUT = 60 * 1000;
	private final static int KEEP_ALIVE = 50 * 1000;
	private Client client = null;
	private NameNodeDummy nnd = null;
	private String server;
	private int tcpPort;
	private int udpPort;

	public INodeClient(String server, int tcpPort, int udpPort) {
		this.server = server;
		this.tcpPort = tcpPort;
		this.udpPort = udpPort;
		this.nnd = NameNodeDummy.getNameNodeDummyInstance();
	}

	static {
		Log.set(Log.LEVEL_TRACE);
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
				}
			}
		});

		client.setKeepAliveTCP(KEEP_ALIVE);

		client.setTimeout(TIME_OUT);

		// Add it after added listener
		INodeServer.register(client.getKryo());

		client.start();
		client.connect(TIME_OUT, server, tcpPort, udpPort);

	}

	public void close(){
		if(this.client!=null){
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

		try {
			client.update(INodeServer.KEEP_ALIVE);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return size;
	}

	/**
	 * Send namespace to another namenode server
	 * @param subTree
	 * @param out
	 * @return
	 */
	public boolean sendINode(INode subTree, JspWriter out, boolean isParentRoot) {
		boolean ifSuccess = false;
		INode parent = subTree.getParent();
		try {

			/**
			 * Send parent path information first
			 */
			
			MoveNSRequest request = new MoveNSRequest();
			request.setOperation(0);
			if(!isParentRoot) {
			INode temp = subTree;
			while (temp != null && temp.getLocalNameBytes().length != 0) {
				this.nnd.logs(out, "Found parent path " + temp.getLocalName());
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
			
			this.connect();
			int response = this.sendTCP(request, out);
			
			/**
			 * Send the namespace tree
			 */
			SplitTree splitTree = new SplitTree();
			
			//Temporary unlink parent reference, has to been fix later.
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
							+ mapRequest.getSubtree().getInode().getLocalName() + " to server "+this.server);

					this.sendTCP(mapRequest, out);
				}
			}
			
			ifSuccess = true;
			
		} catch (Exception e) {
			this.nnd.logs(out, "Cannot send sub-tree "+subTree.getFullPathName()+" to server "+this.server+";Error message:"+e.getMessage());
		} finally {
			// Reference
			subTree.setParent(parent.asDirectory());
		}

		return ifSuccess;
	}

}
