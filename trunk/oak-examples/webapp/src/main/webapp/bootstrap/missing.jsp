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
request.setAttribute("title", "Content Repository Setup");
%><jsp:include page="../header.jsp"/>
<p>
  Your content repository is not properly configured yet. Please use
  the forms below to setup the content repository.
</p>
<p>
  Alternatively, you can directly modify the settings in the
  <code>WEB-INF/web.xml</code> deployment descriptor and redeploy this
  web application.
</p>

<h3>Create a new content repository</h3>
<form action="<%= Text.encodeIllegalXMLCharacters(request.getContextPath()) %>/admin" method="POST">
  <input type="hidden" name="mode" value="new"/>
  <p>
    Use this form to create a new content repository in the given directory.
    The directory is created by this web application and should not already
    exist. The repository is created using a default configuration file.
  </p>
  <p>
    <label>
      Repository home directory:
      <input size="40" type="text" name="repository_home" value="oak">
    </label>
  </p>
  <p><input type="submit" value="Create Content Repository"></p>
</form>

<h3>Use an existing content repository</h3>
<form action="<%= Text.encodeIllegalXMLCharacters(request.getContextPath()) %>/admin" method="POST">
  <input type="hidden" name="mode" value="existing"/>
  <p>
    Use this form to access an existing content repository in the given
    directory. The repository configuration file should be available as
    <code>repository-config.json</code> within the given directory.
  </p>
  <p>
    Note that the repository can not be concurrently accessed by multiple
    applications. You must use WebDAV or RMI through this web application
    if you want to access the repository remotely. Other web applications
    running in the same servlet container can access the repository locally
    using JNDI.
  </p>
  <p>
    <label>
      Repository home directory:
      <input size="40" type="text" name="repository_home" value="oak">
    </label>
  </p>
  <p><input type="submit" value="Access Content Repository"></p>
</form>

<jsp:include page="../footer.jsp"/>
