<%
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file 
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
%>
<%@ page
  contentType="text/html; charset=UTF-8"
  import="org.apache.hadoop.hdfs.server.namenode.NameNode"
  import="org.apache.hadoop.hdfs.server.namenode.NameNodeDummy"
%>
<%!
  //for java.io.Serializable
  private static final long serialVersionUID = 1L;
%>
<!DOCTYPE html>
<html>
<head>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
</head>    
<body>
<%
  String depth = request.getParameter("depth");
  NameNode nn = NameNodeHttpServer.getNameNodeFromContext(application);
  FSNamesystem fsn = nn.getNamesystem();
  int defaultDepth = 10;
  if( depth !=null && !"".equals(depth) ){
	  try{
	      defaultDepth = Integer.parseInt(depth);
	  } catch(Exception e){
		  out.println("Parse error, use default value "+defaultDepth+"<br/>");
	  }
  }
%>
<%= NameNodeDummy.getNameNodeDummyInstance().displayNS(defaultDepth,fsn) %>
<br />
</body>
</html>
