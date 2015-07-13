package org.apache.hadoop.hdfs.server.namenode.dummy;

import javax.servlet.jsp.JspWriter;

public interface CallBack {

  int sendTCP(Object obj, JspWriter out) throws Exception;

}
