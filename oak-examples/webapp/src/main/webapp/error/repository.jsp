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
--%><%@ page isErrorPage="true"
             import="org.apache.jackrabbit.util.Text,
                     java.io.StringWriter,
                     java.io.PrintWriter"%><%
request.setAttribute("title", "Repository Error");
%><jsp:include page="../header.jsp"/>
<p>
  The content repository operation failed with the following
  <%= exception.getClass().getSimpleName() %> error:
</p>
<blockquote><%= Text.encodeIllegalXMLCharacters(exception.getMessage()) %></blockquote>
<p>
  See the
  <a href="<%= Text.encodeIllegalXMLCharacters(request.getContextPath()) %>/troubleshooting.jsp">troubleshooting page</a>
  for ideas on how to resolve this issue.
</p>

<h2>Exception stack trace</h2>
<p>
  Below is the full exception stack trace associated with this error:
</p>
<%
StringWriter buffer = new StringWriter();
exception.printStackTrace(new PrintWriter(buffer));
%>
<pre><%= Text.encodeIllegalXMLCharacters(buffer.toString()) %></pre>
<jsp:include page="../footer.jsp"/>
