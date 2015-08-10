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
String title =
    Text.encodeIllegalXMLCharacters(request.getAttribute("title").toString());
String context =
    Text.encodeIllegalXMLCharacters(request.getContextPath());
%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"
          "http://www.w3.org/TR/html4/loose.dtd">
<html>
  <head>
    <title><%= title %></title>
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
            <img src="<%= context %>/images/jlogo.gif"
                 alt="Apache Jackrabbit" height="100" width="336"/>
          </a>
        </p>
        <p id="asf">
          <a href="http://www.apache.org/">
            <img src="<%= context %>/images/asf-logo.gif"
                 alt="Apache Software Foundation" height="100" width="387"/>
          </a>
        </p>
      </div>
      <div id="navigation">
        <ul>
          <li>Jackrabbit JCR Server
            <ul>
              <li><a href="<%= context %>/">Welcome</a></li>
              <li><a href="<%= context %>/webdav-simple.jsp">Standard WebDAV</a></li>
              <li><a href="<%= context %>/webdav-jcr.jsp">JCR Remoting</a></li>
              <li><a href="<%= context %>/remote.jsp">Remote access</a></li>
              <li><a href="<%= context %>/local.jsp">Local access</a></li>
              <li><a href="<%= context %>/osgi/system/console">Web Console</a></li>
              <li><a href="<%= context %>/troubleshooting.jsp">Troubleshooting</a></li>
              <li><a href="<%= context %>/about.jsp">About Jackrabbit</a></li>
            </ul>
          </li>
          <li>Default workspace
            <ul>
              <li><a href="<%= context %>/repository/default/">Browse</a></li>
              <li><a href="<%= context %>/search.jsp">Search</a></li>
              <li><a href="<%= context %>/populate.jsp">Populate</a></li>
            </ul>
          </li>
          <li>Apache Jackrabbit
            <ul>
              <li><a href="http://jackrabbit.apache.org/">Apache Jackrabbit</a></li>
              <li><a href="http://jackrabbit.apache.org/api/2.3/">Jackrabbit API</a></li>
              <li><a href="http://wiki.apache.org/jackrabbit/FrontPage">Jackrabbit Wiki</a></li>
            </ul>
          </li>
          <li>JCR
            <ul>
              <li><a href="http://jcp.org/en/jsr/detail?id=170">JSR 170</a></li>
              <li><a href="http://jcp.org/en/jsr/detail?id=283">JSR 283</a></li>
              <li><a href="http://www.day.com/maven/javax.jcr/javadocs/jcr-2.0/">JCR 2.0 API</a></li>
            </ul>
          </li>
        </ul>
      </div>
      <div id="content">
        <h2><%= title %></h2>
