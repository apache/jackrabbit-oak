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
--%><%@page import="org.apache.jackrabbit.util.Text,
                    java.io.StringWriter,
                    java.io.PrintWriter"%><%
request.setAttribute("title", "Troubleshooting");
%><jsp:include page="header.jsp"/>
<p>
  If you experience problems with the Jackrabbit JCR server, please
  check the following:
</p>
<ol>
  <li>
    Did you encounter an exception? Copy the exception stack trace somewhere
    so you don't loose it. The stack trace contains valuable information
    for the Jackrabbit developers if you need to file a bug report for the
    problem you encountered.
  </li>
  <li>
    Is the repository up and running? Try browsing the
    <a href="<%= Text.encodeIllegalXMLCharacters(request.getContextPath()) %>/repository/default/">default workspace</a>
    to check if you can still see any content in the repository. You will
    see an error message if the repository is not available.
  </li>
  <li>
    What were you trying to do? Try to verify that your client code or
    other manner of repository use is correct. Did it work before or are
    you trying to do something new?
  </li>
  <li>
    Are there any notable log entries? Check the log files for any related
    warnings or errors. By default the Jackrabbit JCR Server writes log
    entries to the standard output of the servlet container. You can customize
    logging by editing the <code>/WEB-INF/log4j.xml</code> file and
    redeploying this web application.
  </li>
</ol>
<p>
  If none of the above steps help you identify or resolve the problem,
  you can contact the Jackrabbit users mailing list or report the problem
  in the Jackrabbit issue tracker to get support from the Jackrabbit community.
  When contacting the community, please include any relevant details related
  to the above questions and the environment information shown at the end
  of this page.
</p>

<h2>Jackrabbit mailing list</h2>
<p>
  The Jackrabbit user mailing list, users@jackrabbit.apache.org, is the
  place to discuss any problems or other issues regarding the use of
  Apache Jackrabbit (or JCR content repositories in general).
</p>
<p>
  Feel free to subscribe the mailing list or browse the archives listed as
  described in the
  <a href="http://jackrabbit.apache.org/mail-lists.html">Jackrabbit mailing lists</a>
  page.
</p>

<h2>Jackrabbit issue tracker</h2>
<p>
  If you think you've identified a defect in Jackrabbit, you're welcome
  to file a bug report in the
  <a href="https://issues.apache.org/jira/browse/JCR">Jackrabbit issue tracker</a>.
  You can also use the issue tracker to request new features and other
  improvements.
</p>
<p>
  You need an account in the issue tracker to report new issues or to comment
  on existing. Use the
  <a href="https://issues.apache.org/jira/secure/Signup!default.jspa">registration form</a>
  if you don't already have an account. No account is needed browsing
  and searching existing issues.
</p>

<h2>Environment information</h2>
<p>
  This instance of the Jackrabbit JCR Server is running in
  a <em><%= Text.encodeIllegalXMLCharacters(application.getServerInfo()) %></em> servlet container
  that supports the Java Servlet API version
  <%= application.getMajorVersion() %>.<%= application.getMinorVersion() %>.
</p>
<p>
  Details of the Java and operating system environment are included in
  the system properties shown below:
</p>
<%
StringWriter buffer = new StringWriter();
System.getProperties().list(new PrintWriter(buffer));
%>
<pre><%= Text.encodeIllegalXMLCharacters(buffer.toString()) %></pre>
<jsp:include page="footer.jsp"/>
