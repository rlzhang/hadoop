package org.apache.hadoop.hdfs.server.namenode.dummy;

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.NameNodeDummy;
import org.apache.hadoop.hdfs.server.namenode.Quota;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

public class INodeTools {

  /**
   * Update quota recursively
   * Quota.Counts.newInstance()
   * @param dir
   * @param counts
   */
  static void updateCountForQuotaRecursively(INodeDirectory dir,
      Quota.Counts counts) {
    //System.out.println("[updateCountForQuotaRecursively] root dir is " + dir.getFullPathName());
    final long parentNamespace = counts.get(Quota.NAMESPACE);
    final long parentDiskspace = counts.get(Quota.DISKSPACE);

    dir.computeQuotaUsage4CurrentDirectory(counts);

    for (INode child : dir.getChildrenList(Snapshot.CURRENT_STATE_ID)) {
      if (child.isDirectory()) {
        updateCountForQuotaRecursively(child.asDirectory(), counts);
      } else {
        // file or symlink: count here to reduce recursive calls.
        child.computeQuotaUsage(counts, false);
      }
    }

    if (dir.isQuotaSet()) {
      // check if quota is violated. It indicates a software bug.
      final Quota.Counts q = dir.getQuotaCounts();

      final long namespace = counts.get(Quota.NAMESPACE) - parentNamespace;
      final long nsQuota = q.get(Quota.NAMESPACE);

      final long diskspace = counts.get(Quota.DISKSPACE) - parentDiskspace;
      final long dsQuota = q.get(Quota.DISKSPACE);
      NameNodeDummy.getNameNodeDummyInstance().setQota(dir, namespace,
          diskspace);
    }
  }

  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }

}
