<%@ page import="java.net.URI" %>
<%@ page import="org.apache.jackrabbit.j2ee.JcrRemotingServlet" %>
<%@ page import="org.apache.jackrabbit.util.Text" %>
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

URI uri = new URI(request.getRequestURL().toString());
String href =
    uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort()
    + request.getContextPath()
    + JcrRemotingServlet.getPathPrefix(pageContext.getServletContext());
href = Text.encodeIllegalXMLCharacters(href);
href += "/default/jcr:root";
    
%><jsp:include page="header.jsp"/>
<div id="content">
    <h2>Read</h2>
    <h3><a name="default_read">Default Reading</a></h3>
    <p>Reading remotely from the repository generally follows the rules described in
    <a href="http://jackrabbit.apache.org/JCR_Webdav_Protocol.doc">JCR_Webdav_Protocol.zip</a>.
    </p>
    <h3><a name="batch_read">Batch Read</a></h3>
    <p>Batch read is triggered by adding a <b>'.json'</b> extension to the resource
        href. Optionally the client may explicitely specify the desired batch
        read depth by appending <b>'.depth.json'</b> extension. If no json extension
        is present the GET request is processed by applied the default
        remoting rules.
    </p>
    <p>The response to a batch read request contains a plain text representing
       a JSON object. Its member either represent nodes or properties.
    <ul>
    <li>The name element of the Item path is added as key</li>
    <li>The value of a Node entry is a JSON object.</li>
    <li>The value of a Property entry is either a JSON array or a simple JSON value.</li>
    </ul>
    </p>
    <p>In order to cope with property types that cannot be expressed with JSON
    a couple of special rules are defined:
    <ul>
    <li>Binary properties: The key gets a leading ":", the value represents the
        length of the property. In order to retrieve the binary value, the
        client must follow the default rules (see above).</li>
    <li>Date, Name, Path and Reference properties: The type information is passed with a separate JSON pair.</li>
    <li>The value of a Property entry is either a JSON array or a simple JSON value.</li>
    </ul>
    </p>
    See <a href="read_batch.jsp">Example: Batch Write</a> for a demostration of
    the batch read functionality.
    </p>
</div>
<jsp:include page="footer.jsp"/>