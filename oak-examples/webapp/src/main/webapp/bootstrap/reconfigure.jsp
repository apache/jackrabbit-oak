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
request.setAttribute("title", "Content Repository Already Running");
%><jsp:include page="../header.jsp"/>
<p>Your repository is already properly configured an running.</p>
<p>
Your changes were discarded. To reconfigure or reinstall the repository modify
the respective configuration files or remove them.
</p>
<p><a href="<%= Text.encodeIllegalXMLCharacters(request.getContextPath()) %>/">home</a></p>
<jsp:include page="../footer.jsp"/>
