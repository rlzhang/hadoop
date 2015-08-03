package org.apache.hadoop.hdfs.server.namenode.dummy;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.ContentSummaryComputationContext;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory.SnapshotAndINode;
import org.apache.hadoop.hdfs.server.namenode.INodeExternalLink;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeReference;
import org.apache.hadoop.hdfs.server.namenode.INodeSymlink;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeDummy;
import org.apache.hadoop.hdfs.server.namenode.Quota;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.util.Time;
import org.objenesis.strategy.StdInstantiatorStrategy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;

/**
 * NIO server
 * 
 * @author Ray Zhang
 *
 */
public class INodeServer extends Thread {
  public static String ENCODE = "utf8";
  public static boolean isTest = false;
  public static final Log LOG = LogFactory.getLog(INodeServer.class.getName());
  public final static String PREFIX = "distr_from_";
  public final static int WRITE_BUFFER_MB = 1024 * 1024;
  public final static int OBJECT_BUFFER = 1024 * 1024 * 9;
  //Factor from namenode size => memory MB.
  public final static double factor = 0.0000489;
  public final static int MAX_GROUP = 5000;
  public final static String DUMMY = "dummy";
  public final static int TIME_OUT = 63 * 1000;
  public final static int KEEP_ALIVE = 61 * 1000;
  private INodeDirectory parent = null;
  static INodeDirectory root = null;
  private static int TCP_PORT = 8019;
  private static int UDP_PORT = 18019;
  private final static PermissionStatus perm = new PermissionStatus(DUMMY,
      DUMMY, FsPermission.getDirDefault());
  private NameNodeDummy nameNodeDummy;
  private static Object lock = new Object();
  private static ThreadPoolExecutor threadPool;
  /**
   * For test only.
   */
  public static void setRoot(INodeDirectory root) {
    INodeServer.root = root;
  }

  public INodeServer(NameNode nn) {
    nameNodeDummy = NameNodeDummy.getNameNodeDummyInstance();
    nameNodeDummy.setNameNode(nn);
  }

  static {

    com.esotericsoftware.minlog.Log
        .set(com.esotericsoftware.minlog.Log.LEVEL_TRACE);
    //com.esotericsoftware.minlog.Log.TRACE = true;
    com.esotericsoftware.minlog.Log.TRACE();
    System.out.println(com.esotericsoftware.minlog.Log.TRACE);
  }

  
  private  static void threadPool(int totalThread){
    if (threadPool == null)
      synchronized(lock) {
        if (threadPool == null)
        threadPool = new ThreadPoolExecutor(1,
            totalThread, 60, TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(5000),
            new ThreadPoolExecutor.CallerRunsPolicy());
      }
    
  }
  /**
  public static void main(String[] args) {
  	com.esotericsoftware.minlog.Log.TRACE();
  	IMocksControl control = EasyMock.createControl();
  	NameNode nn = control.createMock(NameNode.class);
  	INodeDirectory root = Divide.simpleInodeDir();
  	INodeServer.setRoot(root);
  	INodeServer.isTest = true;
  	INodeServer server = new INodeServer(nn);
  	
  	InetSocketAddress byAddress1 = new InetSocketAddress("localhost", NameNodeDummy.TCP_PORT);
  	NameNodeDummy.getNameNodeDummyInstance().setNameNode(nn);
  	//nn.getNameNodeAddress();
  	FSNamesystem fs = control.createMock(FSNamesystem.class);
  	nn.getNamesystem();
  	EasyMock.expectLastCall().andReturn(fs).times(1);
  	
  	FSDirectory dir = control.createMock(FSDirectory.class);
      fs.getFSDirectory();
  	EasyMock.expectLastCall().andReturn(dir).times(1);
  	
  	NameNodeDummy.getNameNodeDummyInstance().getNamenodeAddress();
  	EasyMock.expectLastCall().andReturn(byAddress1);
  	control.replay();
  	server.start();
  }
  **/

  public void run() {
    try {
      this.kickOff(TCP_PORT, UDP_PORT);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void kickOff(int tcpPort, int udpPort) throws IOException {
    Server server = new Server(OBJECT_BUFFER, OBJECT_BUFFER );
    server.addListener(new Listener() {
      // If you use same hostname and multiple client instances to connect the same server, will cause issue.
      //private Map<Integer,MapRequest> map = new HashMap<Integer,MapRequest>();
      long listSize = 0;

      public void received(Connection connection, Object object) {
        if (object == null)
          System.err.println("Object should not be null!");
        if (object instanceof MapRequest[]) {
          this.handleMapRequestArray(connection, object);
        } else if (object instanceof INode) {
          this.handleINode(connection, object);
        } else if (object instanceof MoveNSRequest) {
          this.handleMoveNSRequest(connection, object);
          this.response(connection, object);
        } else if (object instanceof MapRequest) {
          System.err.println("[INodeServer] Should not execute here!");
          //this.handleMapRequest(connection, object);
          INodeTools.updateCountForQuotaRecursively(INodeServer.root,
              Quota.Counts.newInstance());
          if (NameNodeDummy.DEBUG)
            System.out.println(NameNodeDummy.getNameNodeDummyInstance()
                .printNSInfo(root, 0, 10).replaceAll("&nbsp;", " ")
                .replace("<br/>", "\n"));

        } else if (object instanceof UpdateRequest) {
          this.handleOverflowTableUpdate(connection, object);
        } else if (object instanceof ClientCommends) {
          ClientCommends response = (ClientCommends) object;
          //if (NameNodeDummy.DEBUG)
          System.out.println("Get ClientCommends from Client "
              + response.getCommand());
          if (response.getCommand() == 4) {
            String hostName =
                connection.getRemoteAddressTCP().getAddress().getHostAddress();
            //INodeServer.getThreadPool().shutdown();
            Map<Integer, MapRequest> map =
                UpdateNIOData.getServersMap().get(hostName);
            if (map != null) {
              map.clear();
              System.out.println("Server clear map!");
            }
          }
        }
      }

      public void connected(Connection connection) {

        int buf = connection.getTcpWriteBufferSize();
        System.out.println(buf + " = write buffer,connected "
            + connection.getRemoteAddressTCP());
        //connection.setKeepAliveTCP(KEEP_ALIVE);
      }

      private void handleOverflowTableUpdate(Connection connection,
          Object object) {
        UpdateRequest request = (UpdateRequest) object;
        String[] srcs = request.getSrcs();
        for (int i = 0; i < srcs.length; i++) {
          ExternalStorage[] es =
              NameNodeDummy.getNameNodeDummyInstance().findAllValues(srcs[i]);
              //NameNodeDummy.getNameNodeDummyInstance().findExternalNN_OLD(
                //  srcs[i], false);
          NameNodeDummy.info("[handleOverflowTableUpdate] Try to update existing overflow table " + srcs[i]);
          if (es == null || es.length == 0) {
            System.out.println("Try to insert new record for overflow table. Cannot find giving path " + srcs[i]);
            ExternalStorage e = new ExternalStorage(200, request.getNewTargetNN(), "", srcs[i], request.getSourceNNServer());
            NameNodeDummy.getNameNodeDummyInstance().buildOrAddRadixAllBST(new ExternalStorage[]{e});
            continue;
          }
          String local = NameNodeDummy.getNameNodeDummyInstance().getNamenodeAddress()
              .getHostName();
          //If metadata belong to the same NN
          if (request.getNewTargetNN().equals(local)) {
//            if (NameNodeDummy.DEBUG)
//              System.out.println(es.getPath()
//                  + "[INodeServer]handleOverflowTableUpdate: Found useless table:"
//                  + srcs[i] + "; from " + es.getTargetNNServer() + " to "
//                  + request.getNewTargetNN());
            for (int j=0;j<es.length;j++){
              if (request.getOldTargetNN().equals(es[j].getTargetNNServer()))
                NameNodeDummy.getNameNodeDummyInstance().removeFromRadixTree(es[j].getPath());
            }
            
            //.removeExternalNN(
              //  es.getPath(), false);
            continue;
          }
          for (int j=0;j<es.length;j++){
            if (request.getOldTargetNN().equals(es[j].getTargetNNServer())){
              es[j].setMoveTime(Time.now());
              System.out
              .println("[INodeServer]handleOverflowTableUpdate: update metadat:"
                  + srcs[i]
                  + "; from "
                  + es[j].getTargetNNServer()
                  + " to "
                  + request.getNewTargetNN());
             es[j].setTargetNNServer(request.getNewTargetNN());
            }
              
          }
          
         
        }
      }

      public void disconnected(Connection c) {
        System.out.println("Disconnected be invoked!");
        super.disconnected(c);
      }

      public void response(Connection connection, Object object) {
        MoveNSResponse response = new MoveNSResponse();
        if (!INodeServer.isTest) {
          response.setPoolId(nameNodeDummy.getFSNamesystem().getBlockPoolId());
        } else {
          response.setPoolId("testpoolid");
        }

        int bytesCount = connection.sendTCP(response);
        System.out.println(bytesCount + " bytes. Send poolid to client "
            + response.getPoolId());
      }

      private AtomicInteger count = new AtomicInteger(0);
      private void handleMapRequestArray(Connection connection, Object object) {
        if (NameNodeDummy.DEBUG)
          System.out.println("Received data from client(MapRequestArray) " + count.getAndIncrement());
        //INodeServer.threadPool(100);
        if (root == null) {
          root = nameNodeDummy.getRoot().asDirectory();
          parent = root;
        }
        UpdateNIOData update =
            new UpdateNIOData(object, parent, listSize, connection);
        //INodeServer.getThreadPool().execute(update);
        update.start();
      }
      
    

      //			private void handleMapRequest(Connection connection, Object object) {
      //			  String hostName = connection.getRemoteAddressTCP().getHostName();
      //			  Map<Integer,MapRequest> map = serversMap.get(hostName);
      //			  if (map == null) {
      //			    map = new HashMap<Integer,MapRequest>();
      //			    serversMap.put(hostName, map);
      //			  }
      //			  //System.out.println(connection.getID() + " connection is " + connection.getRemoteAddressTCP().getHostName() + connection.getRemoteAddressTCP().getPort() );
      //				MapRequest request = (MapRequest) object;
      //				//if(NameNodeDummy.DEBUG)
      //				if (request.getKey() % 1000 == 0)
      //				System.out.println(request.getKey() + ", total object list size is " + listSize + "; map size is " + map.size());
      //				map.put(request.getKey(), request);
      //				//list.add(request);
      //				if (parent == null) {
      //					NameNodeDummy.LOG
      //							.error("Namenode server not ready yet, do nothing!");
      //					return;
      //				}
      //				if(NameNodeDummy.DEBUG)
      //				System.out.println("Adding "
      //						+ request.getInode().getLocalName()
      //						+ " to " + parent.getFullPathName());
      //				if (listSize == map.size()) {
      //
      //          System.out.println("Server received all the data!" + map.size());
      //				  this.receivedAllData(connection,listSize);
      //					SplitTree splitTree = new SplitTree();
      //					INode inode = splitTree.mergeListToINode(map);
      //					if(NameNodeDummy.DEBUG)
      //					System.out.println("After merged: inode = "
      //							+ inode.getFullPathName());
      //					// this.addBlockMap(inode);
      //					if (parent != null) {
      //						// parent.addChild(inode);
      //						this.recursiveAddNode(inode, parent);
      //						}
      //					//Display tree.
      //					//System.out.println(Tools.display(inode, 10, true));
      //					map.clear();
      //					try {
      //						if (!INodeServer.isTest)
      //						NameNodeDummy.getNameNodeDummyInstance()
      //								.saveNamespace();
      //						System.out.println("Force saved the namespace!!");
      //					} catch (AccessControlException e) {
      //						e.printStackTrace();
      //					} catch (IOException e) {
      //						e.printStackTrace();
      //					}
      //				}
      //			}

      //			private void finalResponse(Connection connection) {
      //				ClientCommends cc = new ClientCommends();
      //				cc.setCommand(0);
      //				connection.sendTCP(cc);
      //			}

      //waitForLoadingFSImage();

      private void handleINode(Connection connection, Object object) {
        INode request = (INode) object;
        System.out.println("Size:"
            + request.asDirectory().computeQuotaUsage().get(Quota.NAMESPACE));
        if (parent != null) {
          parent.addChild(request.asDirectory()
              .getChildrenList(Snapshot.CURRENT_STATE_ID).get(0).asDirectory());
          System.out.println("Append parent " + parent.getFullPathName() + ":"
              + parent.getFsPermissionShort());
        }
      }

      private void createDummyFolder(String server, MoveNSRequest request) {
        server = INodeServer.PREFIX + server;
        INode temp =
            INodeServer.root.getChild(DFSUtil.string2Bytes(server),
                Snapshot.CURRENT_STATE_ID);
        if (temp != null) {
          parent = temp.asDirectory();
          return;
        }

        INodeDirectory root2 =
            new INodeDirectory(1, DFSUtil.string2Bytes(server), perm, Time
                .now());
        root2 = NameNodeDummy.getNameNodeDummyInstance().addQuota(root2);

        root.addChild(root2);

        if (!INodeServer.isTest)
          UpdateNIOData.addINodeToMap(root2);
        parent = root2.asDirectory();

      }

      private void handleMoveNSRequest(Connection connection, Object object) {

        MoveNSRequest request = (MoveNSRequest) object;
        //System.out.println("Server try to accept new namespace: " + request.getFullPath());
        listSize = request.getListSize();
        if (root == null)
          root = nameNodeDummy.getRoot().asDirectory();
        parent = root;
        if (request.getOperation() == 0) {
          int loop = request.getId().size() - 1;
          if (loop < 0)
            loop = 0;
          /**
           * Avoid create duplicate namespace after first move
           */
          if (request.getNamespace() == null)
            this.createDummyFolder(connection.getRemoteAddressTCP()
                .getHostName(), request);
          for (int k = loop; k > 0; k--) {

            // if(parent.isDirectory()){
            INode child =
                parent.getChild(
                    DFSUtil.string2Bytes(request.getLocalName().get(k)),
                    Snapshot.CURRENT_STATE_ID);
            if (child != null) {
              LOG.info("Found path existing , ignore "
                  + child.getFullPathName());
              parent = child.asDirectory();
              continue;
            }
            // }

            PermissionStatus perm =
                new PermissionStatus(request.getUser().get(k), request
                    .getGroup().get(k), FsPermission.createImmutable(request
                    .getMode().get(k)));
            child =
                new INodeDirectory(request.getId().get(k), DFSUtil
                    .string2Bytes(request.getLocalName().get(k)), perm, request
                    .getMtime().get(k));

            NameNodeDummy.getNameNodeDummyInstance().addQuota(
                child.asDirectory(), request.getNsQuota().get(k),
                request.getDsQuota().get(k));
            // tmp.setAccessTime(request.getAccessTime().get(k));
            child.setAccessTime(Time.now());
            LOG.info("Adding node local name=" + child.getFullPathName()
                + " to " + parent.getFullPathName());
            parent.addChild(child);
            if (!INodeServer.isTest)
              UpdateNIOData.addINodeToMap(child);
            child.setParent(parent);
            parent = child.asDirectory();
          }

          /** Logs only. **/
//          if (parent != null) {
//            System.out.println("Checking files under "
//                + parent.getFullPathName());
//            ReadOnlyList<INode> roList =
//                parent.asDirectory().getChildrenList(Snapshot.CURRENT_STATE_ID);
//            Iterator<INode> ite = roList.iterator();
//            while (ite.hasNext()) {
//              //for (int i = 0; i < roList.size(); i++) {
//              System.out.println("Getting files "
//                  + ite.next().getFullPathName());
//            }
//          }

        } else if (request.getOperation() == 1) {
          parent = request.getParent();
        }
      }
    });

    INodeServer.register(server.getKryo());
    server.start();
    server.bind(tcpPort, udpPort);
    //System.out.println("Server running on " + tcpPort);
  }

  public static void register(Kryo kryo) {
    // kryo.setRegistrationRequired(false);
    kryo.setReferences(true);
    registerClass(kryo);
    kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
  }

  /**
  private static void registerINodeFile(Kryo kryo) {
  	kryo.register(
  			INodeFile.class,
  			new Serializer<INodeFile>() {

  				@Override
  				public INodeFile read(Kryo kryo, Input input,
  						Class<INodeFile> arg2) {
  					int replication = input.readInt();
  					long bid = input.readLong();
  					long stamp = input.readLong();
  					long blockSize = input.readLong();
  					BlockInfo bi = new BlockInfo(new Block(bid, blockSize,
  							stamp), replication);
  					kryo.reference(bi);
  					return bi;
  				}

  				@Override
  				public void write(Kryo kryo, Output out, INodeFile file) {
  					out.writeShort(file.getFileReplication());
  					out.writeLong(file.getModificationTime());
  					
  					out.writeLong(file.getAccessTime());
  					out.writeLong(file.getPermissionLong());
  					out.writeLong(file.getPreferredBlockSize());
  					out.writeByte(file.getLocalStoragePolicyID());
  				}

  			});
  }
  
  **/

  /**
   * Decouple block id -> datanodes mapping, which will handle by datanode report.
   * We only care about INodeFile and INodeDirectory, INodeSymlink and INodeExternalLink will never be divided out.
   * @param kryo
   */
  private static void registerClass(Kryo kryo) {
    FieldSerializer inode = new FieldSerializer(kryo, INode.class);

    //inode.removeField("parent");
    kryo.register(INode.class, inode);

    FieldSerializer dir = new FieldSerializer(kryo, INodeDirectory.class);
    //dir.removeFieldFirstOnly("parent");
    dir.removeField("parent");
    dir.removeField("children");
    //dir.removeField("name");

    kryo.register(INodeDirectory.class, dir);

    FieldSerializer file = new FieldSerializer(kryo, INodeFile.class);
    file.removeField("parent");
    //file.removeField("next");
    //file.removeField("blocks");
    kryo.register(INodeFile.class, file);

    kryo.register(org.apache.hadoop.hdfs.server.namenode.dummy.ExternalStorage.class);
    kryo.register(ExternalStorageMapping.class);
    kryo.register(org.apache.hadoop.hdfs.server.namenode.dummy.ExternalStorage[].class);
    FieldSerializer external =
        new FieldSerializer(kryo, INodeExternalLink.class);
    external.removeField("parent");
    kryo.register(INodeExternalLink.class, external);

    FieldSerializer ref = new FieldSerializer(kryo, INodeReference.class);
    ref.removeField("parent");
    kryo.register(INodeReference.class, ref);

    FieldSerializer sym = new FieldSerializer(kryo, INodeSymlink.class);
    sym.removeField("parent");
    kryo.register(INodeSymlink.class, sym);

    /**
    kryo.register(org.apache.hadoop.hdfs.server.namenode.INodeExternalLink.class);
    kryo.register(ExternalStorageMapping.class);
    kryo.register(ExternalStorage.class);
    **/
    kryo.register(org.apache.hadoop.hdfs.server.namenode.dummy.MoveNSRequest.class);

    kryo.register(String[].class);
    kryo.register(org.apache.hadoop.hdfs.server.namenode.dummy.UpdateRequest.class);
    kryo.register(org.apache.hadoop.hdfs.server.namenode.FileUnderConstructionFeature.class);

    kryo.register(org.apache.hadoop.hdfs.server.namenode.dummy.ClientCommends.class);
    kryo.register(java.util.ArrayList.class);
    kryo.register(org.apache.hadoop.hdfs.server.namenode.INode.Feature[].class);
    kryo.register(byte[].class);

    kryo.register(org.apache.hadoop.hdfs.server.namenode.dummy.MapRequest.class);
    kryo.register(org.apache.hadoop.hdfs.server.namenode.dummy.MapRequest[].class);
    //kryo.register(org.apache.hadoop.hdfs.server.namenode.dummy.SubTree.class);
    kryo.register(
        org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo.class,
        new Serializer<BlockInfo>() {

          @Override
          public BlockInfo read(Kryo kryo, Input input, Class<BlockInfo> arg2) {
            int replication = input.readInt();
            long bid = input.readLong();
            long stamp = input.readLong();
            long blockSize = input.readLong();
            BlockInfo bi =
                new BlockInfo(new Block(bid, blockSize, stamp), replication);
            kryo.reference(bi);
            return bi;
          }

          @Override
          public void write(Kryo kryo, Output out, BlockInfo bi) {
            int capacity = bi.getCapacity();
            out.writeInt(capacity);
            out.writeLong(bi.getBlockId());
            out.writeLong(bi.getGenerationStamp());
            out.writeLong(bi.getNumBytes());
          }

        });
    kryo.register(org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo[].class);

    kryo.register(Object[].class);

    kryo.register(org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo.class);

    kryo.register(org.apache.hadoop.hdfs.server.namenode.dummy.MoveNSResponse.class);

    kryo.register(BlockCollection.class);

    FieldSerializer datanodeDescriptorSerializer =
        new FieldSerializer(kryo, DatanodeDescriptor.class);
    datanodeDescriptorSerializer.removeField("replicateBlocks");
    datanodeDescriptorSerializer.removeField("recoverBlocks");
    datanodeDescriptorSerializer.removeField("decommissioningStatus");
    datanodeDescriptorSerializer.removeField("storageMap");

    kryo.register(
        org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.class,
        datanodeDescriptorSerializer);

    kryo.register(org.apache.hadoop.hdfs.protocol.Block.class);

    kryo.register(org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates.class);
    kryo.register(org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.CachedBlocksList.class);

    kryo.register(org.apache.hadoop.hdfs.util.EnumCounters.class);
    kryo.register(long[].class);
    kryo.register(java.lang.Class.class);
    kryo.register(org.apache.hadoop.hdfs.StorageType.class);
    //kryo.register(org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.DecommissioningStatus.class);
    kryo.register(org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor[].class);
    kryo.register(java.util.Collections.class);
    kryo.register(java.util.Collections.EMPTY_LIST.getClass());
    kryo.register(org.apache.hadoop.hdfs.util.LightWeightHashSet.class);
    // kryo.register(org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.BlockQueue.class);
    kryo.register(java.util.LinkedList.class);
    kryo.register(java.util.Map.class);
    kryo.register(java.util.HashMap.class);
    kryo.register(java.util.Queue.class);
    kryo.register(java.util.Set.class);
    kryo.register(java.util.HashSet.class);
    kryo.register(java.util.Collection.class);

    //New ones
    kryo.register(org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.class);
    kryo.register(org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction.class);
    kryo.register(org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction.ReplicaUnderConstruction.class);
    kryo.register(org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState.class);
    kryo.register(org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState.class);
    kryo.register(org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole.class);
    kryo.register(org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType.class);
    kryo.register(org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption.class);
    kryo.register(org.apache.hadoop.hdfs.server.common.HdfsServerConstants.RollingUpgradeStartupOption.class);

    kryo.register(org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State.class);

    // After blockinfo, might can be deleted
    kryo.register(org.apache.hadoop.hdfs.server.namenode.snapshot.FileWithSnapshotFeature.class);
    kryo.register(org.apache.hadoop.hdfs.server.namenode.DirectoryWithQuotaFeature.class);
    kryo.register(org.apache.hadoop.hdfs.server.namenode.snapshot.DirectorySnapshottableFeature.class);
    kryo.register(org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiffList.class);
    kryo.register(org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiff.class);

    kryo.register(INodeReference.class);
    kryo.register(INodeReference.WithCount.class);
    kryo.register(INodeReference.WithName.class);
    kryo.register(Quota.Counts.class);
    kryo.register(ContentSummaryComputationContext.class);
    kryo.register(SnapshotAndINode.class);

  }

  public static ThreadPoolExecutor getThreadPool() {
    return threadPool;
  }

}
