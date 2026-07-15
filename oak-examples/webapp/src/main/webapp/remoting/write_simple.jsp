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
    function simpleWrite() {
        var path = document.getElementById("path").value;
        var title = document.getElementById("title").value;
        var text = document.getElementById("text").value;

        var headers = new Object();
        headers["Content-type"] = "application/x-www-form-urlencoded";
        headers["Authorization"] =  "Basic YWRtaW46YWRtaW4=";

        var params = "";
        if (title) {
            params += encodeURIComponent("title") + "=" + encodeURIComponent(title);
        }
        if (text) {
            if (params) {
                params += "&";
            }
            params += encodeURIComponent("text") + "=" + encodeURIComponent(text);
        }

        var url = "<%= href %>" + path;
        var req = getXMLHttpRequest(url, "POST", headers, params);
        var result = document.getElementById("result");

        if (req && (req.status == 200 || req.status == 201)) {
            var res = "Success<br><ul>" +
                    "<li><a href=\"" + url + "\" target=\"_blank\">Node</a> at "+ path +"</li>";
            if (title) {
                res += "<li><a href=\"" + url + "/title\">Title</a></li>";
            }
            if (text) {
                res += "<li><a href=\"" + url + "/text\">Text</a></li>";
            }
            res += "</ul>";
            result.innerHTML = res;
        } else {
            var error = "ERROR: " + ((req) ? (req.status + " : "+ req.responseText) : "Failed to create XMLHttpRequest.");
            result.innerHTML = error;
        }
        return true;
    }

</script>
<div id="content">
    <h2>Examples: Simplified Writing</h2>
    <p>If the JCR node at the specified absolute path allows to set a properties
    with name <i>title</i> or <i>text</i>, submitting the form below will
    will set those properties to the given values.</p>
    <p>If no JCR node exists at the specified absolute path, the missing
    intermediate nodes will be created if an applicable node type for the
    specified node name(s) can be determined.</p>
    <table>
        <tr>
            <td>Node Path</td>
            <td><input type="text" id="path" value=""></td>
        </tr>
        <tr>
            <td valign="top">Title</td>
            <td><input type="text" id="title" value=""></td>
        </tr>
        <tr>
            <td valign="top">Text</td>
            <td><textarea rows="5" cols="40" id="text"></textarea></td>
        </tr>
        <tr><td><input type="button" value="Submit" onclick="simpleWrite()"></td></tr>
    </table>
    <p>
    <pre id ="result" class="code"></pre>
</div>
<jsp:include page="footer.jsp"/>