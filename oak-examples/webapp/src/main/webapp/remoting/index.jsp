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
%><jsp:include page="header.jsp"/>
      <div id="content">
        <h2>JCR Remoting Server - Introduction</h2>
        <p>Ths section shortly overviews the batch read/write extensions
        added to the JCR remoting feature.
        <ul>
            <li>Read</li>
            <ul>
                <li><a href="read.jsp#default_read">Default Reading</a></li>
                <li><a href="read.jsp#batch_read">Batch Read</a></li>
            </ul>
            <li>Write</li>
            <ul>
                <li><a href="write.jsp#default_write">Default Writing</a></li>
                <li><a href="write.jsp#batch_write">Batch Write</a></li>
                <li><a href="write.jsp#simple_write">Direct Content Editing</a></li>
            </ul>
        </ul>
        </p>
        <p>Some principals are demonstrated in the corresponding example section.</p>
      </div>
<jsp:include page="footer.jsp"/>