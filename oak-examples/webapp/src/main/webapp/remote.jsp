<%@ page import="java.net.URI"%><%--
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
request.setAttribute("title", "Remote Repository Access");

URI uri = new URI(request.getRequestURL().toString());
String base =
    uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort()
    + request.getContextPath();
base = Text.encodeIllegalXMLCharacters(base);
%><jsp:include page="header.jsp"/>
<p>
  The content repository within this web application is made available
  to remote clients through
  <a href="http://java.sun.com/javase/technologies/core/basic/rmi/">RMI</a>
  and the <em>jackrabbit-jcr-rmi</em> component.
<p>
<p>
  The remote repository stub is available both in the RMI registry
  (one is started automatically by this web application if not already running)
  and as a direct HTTP download. The default URLs for accessing the remote
  repository are:
</p>
<ul>
  <li>RMI registry: //localhost/jackrabbit.repository</li>
  <li>HTTP download: <%= base %>/rmi</li>
</ul>
<p>
  Note that the above URLs are the defaults. You can disable or change them
  by modifying the /WEB-INF/web.xml deployment descriptor.
</p>

<h3>Accessing the remote repository</h3>
<p>
  To access the remote content repository you need to use the
  <em>jackrabbit-jcr-rmi</em> component in your application. If you use
  Maven 2, you can declare the JCR and jackrabbit-jcr-rmi dependencies
  like this:
</p>
<pre>&lt;dependency&gt;
  &lt;groupId&gt;javax.jcr&lt;/groupId&gt;
  &lt;artifactId&gt;jcr&lt;/artifactId&gt;
  &lt;version&gt;1.0&lt;/version&gt;
&lt;/dependency&gt;
&lt;dependency&gt;
  &lt;groupId&gt;org.apache.jackrabbit&lt;/groupId&gt;
  &lt;artifactId&gt;jackrabbit-jcr-rmi&lt;/artifactId&gt;
  &lt;version&gt;1.4&lt;/version&gt;
&lt;/dependency&gt;
</pre>
<p>
  With that dependency in place, you can use either the RMI registry or
  the direct HTTP download to access the repository.
</p>
<p>
  The required code for accessing the repository using the RMI registry is:
</p>
<pre>
<b>import</b> javax.jcr.Repository;
<b>import</b> org.apache.jackrabbit.rmi.repository.RMIRemoteRepository;

Repository repository =
    <b>new</b> RMIRemoteRepository("<em>//localhost/jackrabbit.repository</em>");
</pre>
<p>
  The required code for accessing the repository using the RMI registry is:
</p>
<pre>
<b>import</b> javax.jcr.Repository;
<b>import</b> org.apache.jackrabbit.rmi.repository.URLRemoteRepository;

Repository repository =
    <b>new</b> URLRemoteRepository("<em><%= base %>/rmi</em>");
</pre>
<p>
  See the <a href="http://jcp.org/en/jsr/detail?id=170">JCR specification</a>
  and the
  <a href="http://www.day.com/maven/jsr170/javadocs/jcr-1.0/javax/jcr/Repository.html">Repository</a>
  javadoc for details on what to do with the acquired Repository instance.
</p>

<h3>Remote access performance</h3>
<p>
  Note that the design goal of the current jackrabbit-jcr-rmi component
  is correct and complete functionality instead of performance, so you should
  not rely on remote access for performance-critical applications.
</p>
<p>
  You may want to look at the Jackrabbit clustering feature for best
  performance for concurrently accessing the repository on multiple separate
  servers.
</p>
<jsp:include page="footer.jsp"/>
