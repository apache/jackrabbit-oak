<%@ page import="org.apache.jackrabbit.j2ee.RepositoryAccessServlet,
                 javax.jcr.Repository"
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
--%><%

Repository rep;
try {
    rep = RepositoryAccessServlet.getRepository(pageContext.getServletContext());
} catch (Throwable e) {
    %><jsp:forward page="bootstrap/missing.jsp"/><%
}

request.setAttribute("title", "Apache Jackrabbit JCR Server");
%><jsp:include page="header.jsp"/>
<p>
  Welcome to the Apache Jackrabbit JCR Server. This web application
  contains a JCR content repository and makes it available to clients
  through WebDAV and other means.
</p>
<p>
  The following WebDAV view is provided for accessing the
  content in the JCR content repository.
</p>
<ul>
  <li><a href="webdav-simple.jsp">Standard WebDAV</a></li>
</ul>
<p>
  In addition the JCR Server project provides means for JCR remoting over HTTP:
</p>
<ul>
  <li><a href="webdav-jcr.jsp">JCR remoting over WebDAV</a></li>
  <li><a href="webdav-remoting.jsp">JCR remoting over WebDAV (including Batch Read/Write)</a></li>
</ul>
<p>
  Clients can also access the repository using the JCR API. Both local
  and remote access is supported.
</p>
<ul>
  <li><a href="remote.jsp">Remote repository access</a></li>
  <li><a href="local.jsp">Local repository access</a></li>
</ul>
<p>
  For more information, including copyright and licensing details, visit the
  <a href="about.jsp">About Apache Jackrabbit</a> page.
</p>
<jsp:include page="footer.jsp"/>
