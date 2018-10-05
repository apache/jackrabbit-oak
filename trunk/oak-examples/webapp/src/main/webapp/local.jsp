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
--%><%@page import="org.apache.jackrabbit.util.Text"%><%
request.setAttribute("title", "Local Repository Access");
%><jsp:include page="header.jsp"/>
<p>
  The content repository within this web application can be accessed
  locally by other web applications within the same servlet container.
  Local access is much faster than <a href="remote.jsp">remote access</a>.
</p>
<p>
  The content repository is made available both through JNDI and the
  web application context.
</p>

<h3>Accessing the repository through JNDI</h3>
<p>
  By default the repository is only made available in a dummy JNDI directory
  local to this web application. However, you can make the repository globally
  available if your servlet container allows a web application to modify the
  global JNDI directory or you are using some other JNDI directory that can
  manage unserializable Java objects.
</p>
<p>
  To bind the the repository to such a JNDI directory, you need to modify
  the <code>java.naming</code> parameters in either the /WEB-INF/web.xml
  deployment descriptor or the jackrabbit/bootstrap.properties file. You need
  to redeploy this web application to activate the changes.
</p>
<p>
  Use the following code to access a repository bound in a JNDI directory:
</p>
<pre>
<b>import</b> javax.jcr.Repository;
<b>import</b> javax.naming.Context;
<b>import</b> javax.naming.InitialContext;

Context context = <b>new</b> InitialContext(...);
Repository repository = (Repository) context.lookup(...);
</pre>

<h3>Accessing the repository through servlet context</h3>
<p>
  This web application makes the repository available as the
  <code>javax.jcr.Repository</code> attribute in the application context.
  If your servlet container supports cross-context access, you can
  access the repository directly using that attribute.
</p>
<p>
  For example in <a href="http://tomcat.apache.org/">Apache Tomcat</a>
  you can enable cross-context access by setting the <code>crossContext</code>
  attribute to true in the &lt;Context/&gt; configuration.
</p>
<p>
  Use the following code to access a repository through the servlet context:
</p>
<pre>
<b>import</b> javax.jcr.Repository;
<b>import</b> javax.servlet.ServletContext;

ServletContext context = ...; // <em>context of your servlet</em>
ServletContext jackrabbit =
    context.getContext("<em><%= Text.encodeIllegalXMLCharacters(request.getContextPath()) %></em>");
Repository repository = (Repository)
    context.getAttribute(Repository.<b>class</b>.getName()).
</pre>

<h3>Using the jackrabbit-jcr-servlet component</h3>
<p>
  The <em>jackrabbit-jcr-servlet</em> component contains utility classes
  for use within JCR web applications. With that component you can hide
  both the above and the <a href="remote.jsp">remote access</a> options
  from your code, and use just the following to access a repository:
</p>
<pre>
<b>import</b> javax.jcr.Repository;
<b>import</b> org.apache.jackrabbit.servlet.ServletRepository;

<b>public class</b> MyServlet <b>extends</b> HttpServlet {

    <b>private final</b> Repository repository = <b>new</b> ServletRepository(<b>this</b>);

    // ...

}
</pre>
<p>
  See the jackrabbit-jcr-servlet documentation for more details.
</p>
<jsp:include page="footer.jsp"/>
