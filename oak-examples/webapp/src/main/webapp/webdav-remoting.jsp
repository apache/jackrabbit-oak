<%@ page import="org.apache.jackrabbit.j2ee.JCRWebdavServerServlet,
                 java.net.URI"
%><%--
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
--%><%@page import="org.apache.jackrabbit.util.Text"%>
<%@ page import="org.apache.jackrabbit.j2ee.JcrRemotingServlet" %>
<%
request.setAttribute("title", "JCR Remoting Server with Batch Read/Write");

URI uri = new URI(request.getRequestURL().toString());
int port = uri.getPort();
String href =
    uri.getScheme() + "://" + uri.getHost() + (port == -1 ? "" : (":" + port))
    + request.getContextPath()
    + JCRWebdavServerServlet.getPathPrefix(pageContext.getServletContext());
href = Text.encodeIllegalXMLCharacters(href);
String shref = href + "/default/jcr:root";
%><jsp:include page="header.jsp"/>
<p>
  The JCR Remoting Server provides an item-based WebDAV view to the
  JCR repository, mapping the functionality provided by JSR 170 to the
  WebDAV protocol in order to allow remote content repository access
  via WebDAV.
</p>
<p>
  This implementation variant adds batch read and write functionality to the initial
  <a href="webdav-jcr.jsp">JCR Remoting Server</a>. In addition it supports
  copy across workspaces and clone.
</p>

<h3>Access the content repository</h3>
<p>
  Use the following URLs to access the content repository in the remoting client:
</p>
<dl>
<dt><a href="<%= href %>"><%= href %></a></dt>
<dd>to access all workspaces of your JCR repository</dd>
<dt><a href="<%= shref %>"><%= shref %></a></dt>
<dd>to access a single workspace (example with workspace named 'default')</dd>
</dl>

<h3>Supported WebDAV functionality</h3>
<p>
  See <a href="webdav-jcr.jsp">JCR Remoting Server</a>.
</p>

<h3>Batch Read</h3>
<p>
Composes a JSON object for a node (and its child items) up to a explicitely
specified or configuration determined depth.
<br>
See <a href ="http://svn.apache.org/repos/asf/jackrabbit/trunk/jackrabbit-jcr-server/src/main/java/org/apache/jackrabbit/server/remoting/davex/JcrRemotingServlet.java">JavaDoc</a> for details
or try the <a href="remoting/index.jsp">Examples</a>.
</p>

<h3>Batch Write</h3>
<p>
In contrast to the default JCR remoting this extended version allows to send
a block of modifications (SPI Batch) within a single POST request containing a
custom ":diff" parameter.
<br>
See the <a href ="http://svn.apache.org/repos/asf/jackrabbit/trunk/jackrabbit-jcr-server/src/main/java/org/apache/jackrabbit/server/remoting/davex/JcrRemotingServlet.java">JavaDoc</a> for details
or try the <a href="remoting/index.jsp">Examples</a>.
</p>

<h3>JCR Remoting Client</h3>
<p>
  For the client counterpart of this WebDAV servlet please take a look at the <a href="http://svn.apache.org/repos/asf/jackrabbit/trunk/jackrabbit-spi2dav/src/main/java/org/apache/jackrabbit/spi2davex">extended SPI2DAV</a>
  project.
</p>

<h3>Configuration</h3>
<ul>
  <li>Context Path: <%= Text.encodeIllegalXMLCharacters(request.getContextPath()) %></li>
  <li>Resource Path Prefix: <%= Text.encodeIllegalXMLCharacters(JcrRemotingServlet.getPathPrefix(pageContext.getServletContext())) %></li>
  <li>Workspace Name: <i>optional</i> (available workspaces are mapped as resources)</li>
  <li>Additional servlet configuration: see <i>/WEB-INF/web.xml</i></li>
</ul>
<jsp:include page="footer.jsp"/>
