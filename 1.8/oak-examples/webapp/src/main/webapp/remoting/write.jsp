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
    <h2>Write</h2>
    <h3><a name="default_write">Default Writing</a></h3>
    <p>Writing remotely to the repository generally follows the rules described in
    <a href="http://jackrabbit.apache.org/JCR_Webdav_Protocol.doc">JCR_Webdav_Protocol.zip</a>.
    </p>
    <h3><a name="batch_write">Batch Write</a></h3>
    <p>A set of transient modifications can in addition be sent by using the
    extended batch write: A single POST request that contains a custom
    <strong>:diff</strong> parameter describing the changes to be applied.
    The expected format is described in the
    <a href ="http://svn.apache.org/repos/asf/jackrabbit/trunk/jackrabbit-jcr-server/src/main/java/org/apache/jackrabbit/server/remoting/davex/JcrRemotingServlet.java">JavaDoc</a>.
    </p>
    Some cases however can be easily demonstrated. The following examples can
    be tested with the form provided at
    <a href="write_batch.jsp">Example: Batch Write</a>.
    </p>
    <h4>Examples</h4>
    <p>The following examples illustrate the basics of the diff format. It does
    not cover the special treatment of properties with type <i>Date</i>,
    <i>Name</i>, <i>Path</i>, <i>Reference</i> and <i>Binary</i> (see below).</p>
    <p style="font-size:smaller">Set properties</p>
    <pre class="code">
^prop1  : "stringvalue"
^prop1  : "changedvalue"
^prop2  : true
^prop3  : 100.010
^prop4  : 1234567890
^prop5  : ["multi","valued","string prop"]
^.      : "change existing property at path."
^/abs/path/to/the/new/prop : "some value."</pre>
    <p style="font-size:smaller">Add new nodes (optionally including child items)</p>
    <pre class="code">
+node   : {"title" : "title property of the new node"}
+node2  : {"childN" : {}, "childN2" : {}}
+/abs/path/to/the/new/node : {"text" : "some text"}</pre>
    <p style="font-size:smaller">Move or rename nodes</p>
    <pre class="code">
>node   : rename
>rename : /moved/to/another/destination</pre>
    <p style="font-size:smaller">Reorder nodes</p>
    <pre class="code">
>childN : childN2#after
>childN : #first
>childN : #last
>childN : childN2#before</pre>
    <p style="font-size:smaller">Remove items</p>
    <pre class="code">
-prop4  :
-node2  :
-/moved/to/another/destination :</pre>
    <h4>Dealing with Special Property Types</h4>
    <p>Property types that can not be covered unambigously, need some special
    handling (see JavaDoc). This affects JCR properties being of type
    <ul>
    <li>Date,</li>
    <li>Name,</li>
    <li>Path,</li>
    <li>Reference,</li>
    <li>Binary.</li>
    </ul>
    In order to set properties of any of the types listed, the value part in the
    :diff param must  be left empty and a separate request parameter must be
    included. Its name equals the corresponding key in the :diff, its value represents
    the property value. In addition the desired property type must be specified
    using the conversion defined with
    <a href="http://svn.apache.org/repos/asf/jackrabbit/trunk/jackrabbit-jcr-server/src/main/java/org/apache/jackrabbit/webdav/jcr/JcrValueType.java">JcrValueType#contentTypeFromType(int)</a>.
    </p>
    <p style="font-size:smaller">Set a Date property</p>
    <pre>
POST /jackrabbit/server/default/jcr%3aroot/testNode HTTP/1.1
Content-Type: multipart/form-data; boundary=kTmAb2lkjCtxbMVFzHEplAJjHCUo5aQndaUu

--kTmAb2lkjCtxbMVFzHEplAJjHCUo5aQndaUu
Content-Disposition: form-data; <b>name="dateProp"</b>
Content-Type: <b>jcr-value/date</b>

<b>2009-02-12T10:19:40.778+01:00</b>         
--kTmAb2lkjCtxbMVFzHEplAJjHCUo5aQndaUu
Content-Disposition: form-data; <b>name=":diff"</b>
Content-Type: text/plain

<b>^dateProp :  </b>
--kTmAb2lkjCtxbMVFzHEplAJjHCUo5aQndaUu--
    </pre>
    <p>Setting <i>Binary</i>, <i>Name</i>, <i>Path</i> or <i>Reference</i>
        properties works accordingly.
    </p>

    <h3><a name="simple_write">Direct Content Editing</a></h3>
    <p>The functionality present with batch reading also enables very simplified
    content editing using common HTML forms.</p>
    <p>The :diff parameter is omitted altogether and each request parameter is
        treated as property
    <ul>
        <li>param name : property name</li>
        <li>param value : property value</li>
    </ul>
    whereas the form action indicates the path of the parent node.
    </p>
    <p>If no node exists at the specified path an attempt is made to create the
        missing intermediate nodes. The primary node type of the new node is
        either retrieved from the corresponding <i>jcr:primaryType</i> param or
        automatically determined by the implementation.</p>
    <p>Setting a property can be tested at
        <a href="write_simple.jsp">Example: Simplified Writing</a>
    </p>
    <h4>Examples</h4>
    <p>The following examples illustrate the simplified writing.</p>
    <p style="font-size:smaller">Set string property
    <ul>
        <li style="font-size:smaller">Existing or non-existing node at /testnode</li>
        <li style="font-size:smaller">Set property 'propName' with value "any string value"</li>
    </ul>
    </p>
    <pre class="code">
&lt;form method="POST" action="<%= href %>/testnode"&gt;
    &lt;input type="text" name="propName" value="any string value"/&gt;
&lt;/form&gt;</pre>
    <p style="font-size:smaller">Add node with a defined node type and set a property</p>
    <ul>
        <li style="font-size:smaller">Non-existing node at /testnode/nonexisting</li>
        <li style="font-size:smaller">Define its primary type to be "nt:unstructured"</li>
        <li style="font-size:smaller">Set property 'propName' with value "any string value"</li>
    </ul>
    <pre class="code">
&lt;form method="POST" action="<%= href %>/nonexisting"&gt;
    &lt;input type="text" name="jcr:primaryType" value="nt:unstructured"/&gt;
    &lt;input type="text" name="propName" value="any string value"/&gt;
&lt;/form&gt;</pre>
</div>
<jsp:include page="footer.jsp"/>