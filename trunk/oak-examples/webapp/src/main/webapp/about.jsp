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
--%><%@page import="java.io.InputStream,
                    java.io.InputStreamReader,
                    java.io.Reader"%><%!

    /**
     * Escapes and outputs the contents of a given (UTF-8) text resource.
     * TODO: There should be an easier way to do this!
     *
     * @param path path of the resource to output
     * @param out the JSP output writer
     * @throws Exception if something goes wrong
     */
    private void output(String path, JspWriter out) throws Exception {
        InputStream input = getServletContext().getResourceAsStream(path);
        try {
            Reader reader = new InputStreamReader(input, "UTF-8");
            for (int ch = reader.read(); ch != -1; ch = reader.read()) {
                if (ch == '<') {
                    out.write("&lt;");
                } else if (ch == '>') {
                    out.write("&gt;");
                } else if (ch == '&') {
                    out.write("&amp;");
                } else {
                    out.write((char) ch);
                }
            }
        } finally {
            input.close();
        }
    }

%><% request.setAttribute("title", "About Apache Jackrabbit");
%><jsp:include page="header.jsp"/>
<p>
  <a href="http://jackrabbit.apache.org/">Apache Jackrabbit</a> is a fully
  conforming implementation of the Content Repository for Java Technology API
  (JCR). A content repository is a hierarchical content store with support for
  structured and unstructured content, full text search, versioning,
  transactions, observation, and more. Typical applications that use content
  repositories include content management, document management, and records
  management systems.
</p>
<p>
  Version 1.0 of the JCR API was specified by the
  <a href="http://jcp.org/en/jsr/detail?id=170">Java Specification Request 170</a>
  (JSR 170) and version 2.0 by the 
  <a href="http://jcp.org/en/jsr/detail?id=283">Java Specification Request 283</a>.
</p>
<p>
  Apache Jackrabbit is a project of the
  <a href="http://www.apache.org/">Apache Software Foundation</a>. 
</p>
<h2>Copyright Notice</h2>
<pre><% output("/META-INF/NOTICE", out); %></pre>
<h2>License Information</h2>
<pre><% output("/META-INF/LICENSE", out); %></pre>
<jsp:include page="footer.jsp"/>
