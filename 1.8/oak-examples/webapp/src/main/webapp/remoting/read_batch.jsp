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
<script src="json.js"></script>
<script language="javascript">
    function batchRead() {

        var path = document.getElementById("path").value;
        var depth = document.getElementById("depth").value;
        var resultType = document.getElementById("resultType");

        // TODO retrieve url from servlet context
        var action = "<%= href %>";
        if (path.length && path != "/") {
            action += path;
        }
        if (depth) {
            action += "." + depth;
        }

        var req = getXMLHttpRequest(action + ".json");
        var result = document.getElementById("result");

        if (req && req.status == 200) {
            if (resultType[4].selected) {
                // tree
                var obj = eval("(" + req.responseText + ")");
                var thref = "<%= href %>" + ((path.length && path != "/") ? path : "");
                result.innerHTML = JsonFormatter.tree(obj, thref);
            } else if (resultType[3].selected) {
                // cleaned from special props
                var obj = eval("(" + req.responseText + ")");
                result.innerHTML = JsonFormatter.format(obj, true);
            } else if (resultType[2].selected) {
                // indention (pretty printing)
                var obj = eval("(" + req.responseText + ")");
                result.innerHTML = JsonFormatter.format(obj, false);
            } else {
                // raw (default)
                result.innerHTML = req.responseText;
            }
        } else {
            var error = "ERROR: " + ((req) ? (req.status + " : "+ req.responseText) : "Failed to create XMLHttpRequest.");
            result.innerHTML = error;
        }
        return true;
    }
</script> 
<div id="content">
    <h2>Examples: Batch Read</h2>
    <p>
    Enter the path of an existing node and the desired depth.
    </p>
    <table>
        <tr>
            <td>Node Path</td>
            <td><input type="text" name="path" id="path" value="/"></td>
        </tr>
        <tr>
            <td>Depth</td>
            <td><input type="text" name="depth" id="depth" value="0"></td>
        </tr>
        <tr>
            <td>Result type</td>
            <td><select name="resultType" id="resultType">
                <option value="">--- select ---</option>
                <option value="raw">raw</option>
                <option value="indented">indented</option>
                <option value="indented_clean">indented (cleaned)</option>
                <option value="tree">tree</option>
            </select></td>
        </tr>
        <tr><td><input type="button" value="Submit" onclick="batchRead()"></td></tr>
    </table>
    <p>
    <pre id ="result" class="code"></pre>
</div>
<jsp:include page="footer.jsp"/>