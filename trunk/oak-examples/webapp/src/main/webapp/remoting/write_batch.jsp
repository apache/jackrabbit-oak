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
    function batchWrite() {
        var path = document.getElementById("path").value;
        var diff = document.getElementById(":diff").value;

        if (!diff) {
            alert("Please enter the Diff.");
            return false;
        }

        var headers = new Object();
        headers["Content-type"] = "application/x-www-form-urlencoded";
        headers["Authorization"] =  "Basic YWRtaW46YWRtaW4=";

        var params = encodeURIComponent(":diff") + "=" + encodeURIComponent(diff);

        var url = "<%= href %>" + path;
        var req = getXMLHttpRequest(url, "POST", headers, params);
        var result = document.getElementById("result");

        if (req && (req.status == 200 || req.status == 201)) {
            result.innerHTML = "Success<br><a href=\"" + url + "\" target=\"_blank\">View Result</a>";
        } else {
            var error = "ERROR: " + ((req) ? (req.status + " : "+ req.responseText) : "Failed to create XMLHttpRequest.");
            result.innerHTML = error;
        }
        return true;
    }

</script>
<div id="content">
    <h2>Examples: Batch Write</h2>
    <p>
    Enter the path of an existing node or property (depending on the desired
    actions) and enter the <i>:diff</i> value.
    </p>
    <p>See the introduction to batched <a href="write.jsp#batch_write">writing</a>
        for examples.
    </p>
    <table>
        <tr>
            <td>Item Path</td>
            <td><input type="text" id="path" value=""></td>
        </tr>
        <tr>
            <td valign="top">Diff</td>
            <td><textarea rows="10" cols="40" id=":diff"></textarea></td>
        </tr>
        <tr><td><input type="button" value="Submit" onclick="batchWrite()"></td></tr>
    </table>
    <p>
    <pre id ="result" class="code"></pre>
</div>
<jsp:include page="footer.jsp"/>