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
request.setAttribute("title", "Content Repository Not Found");
%><jsp:include page="../header.jsp"/>
<p>The repository home directory or configuration do not exists.</p>
<p>
You have chosen to <b>reuse</b> an existing repository but the specified home
directory or the configuration file do not exist.
</p>
<p>
Please specify a correct location or choose to create a new repository.
</p>
<p><a href="<%= Text.encodeIllegalXMLCharacters(request.getContextPath()) %>/admin">back</a></p>
<jsp:include page="../footer.jsp"/>
