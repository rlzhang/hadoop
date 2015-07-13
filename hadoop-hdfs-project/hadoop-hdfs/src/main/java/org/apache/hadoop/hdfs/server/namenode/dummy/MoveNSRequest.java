package org.apache.hadoop.hdfs.server.namenode.dummy;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;

public class MoveNSRequest {
  private String msg;
  private INodeDirectory parent;
  /** 0,Send path only;1,Send INode parent; **/
  private int operation;
  private String namespace = null;

  // INode directory info
  private List<Long> id = new ArrayList<Long>();
  private byte[] fullPath;
  private List<String> localName = new ArrayList<String>();
  private List<String> user = new ArrayList<String>();
  private List<String> group = new ArrayList<String>();
  private List<Short> mode = new ArrayList<Short>();
  private List<Long> mtime = new ArrayList<Long>();
  private List<Long> accessTime = new ArrayList<Long>();
  private List<Long> nsQuota = new ArrayList<Long>();
  private List<Long> dsQuota = new ArrayList<Long>();
  
  private long listSize;

  public List<Long> getDsQuota() {
    return dsQuota;
  }

  public void addDsQuota(Long dsQuota) {
    this.dsQuota.add(dsQuota);
  }

  public List<Long> getNsQuota() {
    return nsQuota;
  }

  public void addNsQuota(Long nsQuota) {
    this.nsQuota.add(nsQuota);
  }

  public List<Long> getId() {
    return id;
  }

  public void addId(Long id) {
    this.id.add(id);
  }

  public List<String> getUser() {
    return user;
  }

  public void addUser(String user) {
    this.user.add(user);
  }

  public List<String> getGroup() {
    return group;
  }

  public void addGroup(String group) {
    this.group.add(group);
  }

  public List<Short> getMode() {
    return mode;
  }

  public void addMode(Short mode) {
    this.mode.add(mode);
  }

  public List<Long> getMtime() {
    return mtime;
  }

  public void addMtime(Long mtime) {
    this.mtime.add(mtime);
  }

  public String getMsg() {
    return msg;
  }

  public void setMsg(String msg) {
    this.msg = msg;
  }

  public int getOperation() {
    return operation;
  }

  public void setOperation(int operation) {
    this.operation = operation;
  }

  public INodeDirectory getParent() {
    return parent;
  }

  public void setParent(INodeDirectory parent) {
    this.parent = parent;
  }

  public String getFullPath() {
    return new String(fullPath);
  }

  public void setFullPath(byte[] fullPath) {
    this.fullPath = fullPath;
  }

  public List<String> getLocalName() {
    return localName;
  }

  public void addLocalName(String localName) {
    this.localName.add(localName);
  }

  public List<Long> getAccessTime() {
    return accessTime;
  }

  public void addAccessTime(Long accessTime) {
    this.accessTime.add(accessTime);
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public long getListSize() {
    return listSize;
  }

  public void setListSize(long listSize) {
    this.listSize = listSize;
  }

}
