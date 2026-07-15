<%@ page import="org.apache.jackrabbit.j2ee.SimpleWebdavServlet,
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
--%><%@page import="org.apache.jackrabbit.util.Text"%><%
request.setAttribute("title", "Standard WebDAV Server");

URI uri = new URI(request.getRequestURL().toString());
int port = uri.getPort();
String href =
    uri.getScheme() + "://" + uri.getHost() + (port == -1 ? "" : (":" + port))
    + request.getContextPath()
    + SimpleWebdavServlet.getPathPrefix(pageContext.getServletContext())
    + "/default/";
href = Text.encodeIllegalXMLCharacters(href);
%><jsp:include page="header.jsp"/>

<p>
  The default WebDAV server (aka: Simple Server) is a
  <a href="http://www.ietf.org/rfc/rfc2518.txt">DAV 1,2</a> and
  <a href="http://www.ietf.org/rfc/rfc3253.txt">DeltaV</a>
  compliant WebDAV  server implementation. It offers a file-based view to
  the JCR repository, suitable  for everybody looking for standard WebDAV
  functionality. Essentially, the contents of the underlying content
  repository are exposed as a hierarchical collection of files and folders.
<p>

<h3>Access the content repository</h3>
<p>
  Use the following URL to access the content repository in your WebDAV client:
</p>
<ul>
  <li><a href="<%= href %>"><%= href %></a></li>
</ul>
<p>
  The server asks for authentication, but by default any username and password
  is accepted. You can modify this security policy in the repository
  configuration file.
</p>
<p>
  To access other workspace than the default one, replace the last part of
  the URL (<code>/default/</code>) with the name of another workspace.
</p>
<p>
  You can also <a href="search.jsp">search</a> the default workspace
  <a href="populate.jsp">populate</a> it with example content from the
  Internet.
</p>

<h3>File system access</h3>
<p>
  Many operating systems, including Windows and Mac OS X, allow you to
  "mount" a WebDAV server as a shared network disk. You can use the above
  URL to make the default workspace available as such a network disk, after
  which you can use normal file system tools to copy files and folders to
  and from the content repository.
</p>

<h3>Supported WebDAV functionality</h3>
<ul>
<li><a href="http://www.ietf.org/rfc/rfc2518.txt">RFC 2518</a> (WebDAV 1,2)</li>
<li><a href="http://www.ietf.org/rfc/rfc3253.txt">RFC 3253</a> (DeltaV)</li>
<li>Experimental: <a href="http://www.ietf.org/rfc/rfc5842.txt">RFC 5842</a> (WebDAV BIND)</li>
</ul> 

<h3>Configuration</h3>
<ul>
    <li>Context path: <%= Text.encodeIllegalXMLCharacters(request.getContextPath()) %></li>
    <li>Resource path prefix: <%= Text.encodeIllegalXMLCharacters(SimpleWebdavServlet.getPathPrefix(pageContext.getServletContext())) %></li>
    <li>Servlet configuration: see <i>/WEB-INF/web.xml</i></li>
    <li>WebDAV specific resource configuration: see <i>/WEB-INF/config.xml</i></li>
</ul>
<jsp:include page="footer.jsp"/>
