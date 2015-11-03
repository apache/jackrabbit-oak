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
--%><%
request.setAttribute("title", "JCR API Not Found");
%><jsp:include page="../header.jsp"/>
<p>
The <code>javax.jcr.Repository</code> interface from the JCR API could not
be loaded.
</p>
<p>
To resolve this issue, you need to make the <code>jcr-2.0.jar</code> file
available in the shared classpath of the servlet container. The file is
available for download from the
<a href="http://www.jcp.org/en/jsr/summary?id=283">JSR 283</a> web page.
</p>
<jsp:include page="../footer.jsp"/>
