<%--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
--%>
<%@page import="org.apache.jackrabbit.util.Text"%>
<%
String context = Text.encodeIllegalXMLCharacters(request.getContextPath());
%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"
          "http://www.w3.org/TR/html4/loose.dtd">
<html>
  <head>
    <title>JCR Remoting Server</title>
    <link rel="stylesheet"
          href="<%= context %>/css/default.css"
          type="text/css"/>
    <link rel="shortcut icon"
          href="<%= context %>/images/favicon.ico"
          type="image/vnd.microsoft.icon" />
  </head>
  <body>
    <div id="page">
      <div id="banner">
        <p id="jcr">
          <a href="<%= context %>/">
            <img src="<%= context %>/images/jlogo.gif" alt="Apache Jackrabbit"/>
          </a>
        </p>
        <p id="asf">
          <a href="http://www.apache.org/">
            <img src="<%= context %>/images/asf-logo.png" alt="Apache Software Foundation"/>
          </a>
        </p>
      </div>
      <div id="navigation">
        <ul>
          <li>JCR Remoting Server
            <ul>
              <li><a href="<%= context %>/remoting/index.jsp">Introduction</a></li>
              <li><a href="<%= context %>/remoting/read.jsp">Read</a></li>
              <li><a href="<%= context %>/remoting/write.jsp">Write</a></li>
            </ul>
          </li>
          <li>Examples
            <ul>
              <li><a href="<%= context %>/remoting/read_batch.jsp">Batch Read</a></li>
              <li><a href="<%= context %>/remoting/write_batch.jsp">Batch Write</a></li>
              <li><a href="<%= context %>/remoting/write_simple.jsp">Simple Write</a></li>
            </ul>
          </li>
        </ul>
      </div>
