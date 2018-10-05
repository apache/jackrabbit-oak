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
--%><%@ page import="javax.jcr.Repository,
                   org.apache.jackrabbit.j2ee.RepositoryAccessServlet,
                   org.apache.jackrabbit.util.Text,
                   javax.jcr.Session,
                   javax.jcr.SimpleCredentials,
                   javax.jcr.query.Query,
                   javax.jcr.query.RowIterator,
                   java.text.NumberFormat,
                   javax.jcr.query.Row,
                   javax.jcr.Node,
                   java.net.URLEncoder,
                   java.text.SimpleDateFormat,
                   java.text.DateFormat,
                   java.util.List,
                   java.util.ArrayList,
                   java.util.Iterator,
                   javax.jcr.Value,
                   javax.jcr.GuestCredentials,
                   javax.jcr.RepositoryException"%>
<%@ page contentType="text/html;charset=UTF-8" %><%
    Repository rep;
    Session jcrSession;
    try {
        rep = RepositoryAccessServlet.getRepository(pageContext.getServletContext());
        jcrSession = rep.login(new SimpleCredentials("admin", "admin".toCharArray()));
    } catch (Throwable e) {
        %>Error while accessing the repository: <font color="red"><%= Text.encodeIllegalXMLCharacters(e.getMessage()) %></font><br><%
        %>Check the configuration or use the <a href="admin/">easy setup</a> wizard.<%
        return;
    }
    try {
        String wspName = jcrSession.getWorkspace().getName();
        String q = request.getParameter("q");
        if (q == null) {
            q = "";
        } else {
            q = new String(q.getBytes("ISO-8859-1"), "UTF-8");
        }
        if (request.getParameter("as_q") != null) {
            q += " " + new String(request.getParameter("as_q").getBytes("ISO-8859-1"), "UTF-8");
        }
        String executedIn = "";
        String queryTerms = "";
        String totalResults = "";
        long from = 0;
        long to = 10;
        long total = 0;
        long maxPage = 0;
        long minPage = 0;
        long currentPageIndex = 0;
        List indexes = new ArrayList();
        RowIterator rows = null;
        String suggestedQuery = null;
        if (q != null && q.length() > 0) {
            String stmt;
            if (q.startsWith("related:")) {
                String path = q.substring("related:".length());
                path = path.replaceAll("'", "''");
                stmt = "//element(*, nt:file)[rep:similar(jcr:content, '" + path + "/jcr:content')]/rep:excerpt(.) order by @jcr:score descending";
                queryTerms = "similar to <b>" + Text.encodeIllegalXMLCharacters(path) + "</b>";
            } else {
                queryTerms = "for <b>" + Text.encodeIllegalXMLCharacters(q) + "</b>";
                q = q.replaceAll("'", "''");
                stmt = "//element(*, nt:file)[jcr:contains(jcr:content, '" + q + "')]/rep:excerpt(.) order by @jcr:score descending";
            }
            Query query = jcrSession.getWorkspace().getQueryManager().createQuery(stmt, Query.XPATH);
            long time = System.currentTimeMillis();
            rows = query.execute().getRows();
            time = System.currentTimeMillis() - time;
            NumberFormat nf = NumberFormat.getNumberInstance();
            nf.setMaximumFractionDigits(2);
            nf.setMinimumFractionDigits(2);
            executedIn = nf.format(((double) time) / 1000d);
            nf.setMaximumFractionDigits(0);
            totalResults = nf.format(rows.getSize());
            if (request.getParameter("start") != null) {
                from = Long.parseLong(request.getParameter("start"));
                try {
                    rows.skip(from);
                } catch (Exception e) {
                    // make sure rows are consumed
                    while (rows.hasNext()) {
                        rows.nextRow();
                    }
                }
            }
            to = Math.min(from + 10, rows.getSize());

            total = rows.getSize();
            maxPage = total / 10L;
            if (total % 10L > 0) {
                maxPage++;
            }
            currentPageIndex = from / 10L;
            maxPage = Math.min(maxPage, currentPageIndex + 10);
            minPage = Math.max(0, currentPageIndex - 10);
            for (long i = minPage; i < maxPage; i++) {
                indexes.add(new Long(i));
            }

            if (total < 10 && !q.startsWith("related:")) {
                try {
                    RowIterator spellResultItr = jcrSession.getWorkspace().getQueryManager().createQuery(
                            "/jcr:root[rep:spellcheck('" + q + "')]/(rep:spellcheck())",
                            Query.XPATH).execute().getRows();
                    if (spellResultItr.hasNext()){
                        Value v = spellResultItr.nextRow().getValue("rep:spellcheck()");
                        if (v != null) {
                            suggestedQuery = v.getString();
                        }
                    }
                } catch (RepositoryException e) {
                    // ignore
                }
            }
        }
request.setAttribute("title", "Search workspace " + wspName);
%><jsp:include page="header.jsp"/>

<form name="gs" method="GET">
  <p>
    <input type="text" name="q" size="41" maxlength="2048" value="<%= Text.encodeIllegalXMLCharacters(q) %>" title="Search">
    <input type=submit value="Search"><br><br>
  </p>
</form>
<% if (rows != null && rows.getSize() == 0) { %>
<%     if (suggestedQuery != null) { %>
<p><font class="p" color="#cc0000">Did you mean:</font>
  <a href="search.jsp?q=<%= Text.encodeIllegalXMLCharacters(suggestedQuery) %>" class="p">
    <b><i><%= Text.encodeIllegalXMLCharacters(suggestedQuery) %></i></b>
  </a><br>
</p>
<%     } %>
  <p/>Your search - <b><%= Text.encodeIllegalXMLCharacters(q) %></b> - did not match any documents.
  <br/><br/>Suggestions:
  <ul>
    <li>Make sure all words are spelled correctly.</li>
    <li>Try different keywords.</li>
    <li>Try more general keywords.</li>
    <li>Try fewer keywords.</li>
  </ul>
  <%
    } else if (rows != null) {
  %>
  <table border=0 cellpadding=0 cellspacing=0 width=100% class="t bt">
    <tr>
      <td><font size=-1>Results <b><%= from + 1 %></b> - <b><%= to %></b> of about <b><%= totalResults %></b> <%= queryTerms %>. (<b><%= executedIn %></b> seconds)&nbsp;</font></td></tr>
  </table>
<% if (suggestedQuery != null) { %>
  <p>
    <font class="p" color="#cc0000">Did you mean:</font>
    <a href="search.jsp?q=<%= Text.encodeIllegalXMLCharacters(suggestedQuery) %>" class="p">
      <b><i><%= Text.encodeIllegalXMLCharacters(suggestedQuery) %></i></b>
    </a><br>
  </p>
<% } %>
  <div>
    <%
      while (rows.hasNext() && rows.getPosition() < to) {
          Row r = rows.nextRow();
          Node file = (Node) jcrSession.getItem(r.getValue("jcr:path").getString());
          Node resource = file.getNode("jcr:content");
          String size = "";
          if (resource.hasProperty("jcr:data")) {
              double length = resource.getProperty("jcr:data").getLength();
              size = String.valueOf(Math.round(Math.ceil(length / 1000d))) + "k";
          }
          DateFormat df = SimpleDateFormat.getDateInstance(SimpleDateFormat.LONG);
          String lastModified = df.format(resource.getProperty("jcr:lastModified").getDate().getTime());
    %>
    <h6><a href="<%= Text.encodeIllegalXMLCharacters(request.getContextPath() + "/repository/" + wspName + file.getPath()) %>" class=l><%= Text.encodeIllegalXMLCharacters(file.getName()) %></a></h6>
      <table border=0 cellpadding=0 cellspacing=0>
        <tr><td><font><%= r.getValue("rep:excerpt(jcr:content)").getString() %>
          <%= Text.encodeIllegalXMLCharacters(file.getPath()) %> - <%= size %> - <%= lastModified %> - <nobr><a href="<%= Text.encodeIllegalXMLCharacters(request.getContextPath()) %>/search.jsp?q=related:<%= Text.encodeIllegalXMLCharacters(URLEncoder.encode(file.getPath(), "UTF-8")) %>">Similar pages</a></nobr></font></td>
        </tr>
      </table>
    <%
      } // while
    %>
  </div>

  <br clear=all>
  <%
    if (indexes.size() > 1) {
  %>
  <div>
    <table border=0 cellpadding=0 cellspacing=0 align=center>
      <tr>
        <td><font size=-1>Result&nbsp;Page:&nbsp;
        <%
        if (currentPageIndex != ((Long) indexes.get(0)).longValue()) {
            %><td nowrap align=right><a href=search.jsp?q=<%= Text.encodeIllegalXMLCharacters(q) %>&start=<%= (currentPageIndex - 1) * 10 %>>Previous</a><%
        } else {
            %><td nowrap ><font size=-1><%
        }
        for (Iterator it = indexes.iterator(); it.hasNext(); ) {
            long pageIdx = ((Long) it.next()).longValue();
            if (pageIdx == currentPageIndex) {
                %><td nowrap><font size=-1><%= pageIdx + 1 %><%
            } else {
                %><td nowrap><font size=-1><a href=search.jsp?q=<%= Text.encodeIllegalXMLCharacters(q) %>&start=<%= pageIdx * 10 %>><%= pageIdx + 1 %></a><%
            }
        }
        if (currentPageIndex < (maxPage - 1)) {
            %><td nowrap><font size=-1><a href=search.jsp?q=<%= Text.encodeIllegalXMLCharacters(q) %>&start=<%= (currentPageIndex + 1) * 10 %>>Next</a><%
        } else {
            %><td nowrap ><%
        }
        %>
    </table>
  </div>
  <%
    }
  %>


    <br clear=all><br>
    <table>
      <tr><td><br><form method=GET action=<%= Text.encodeIllegalXMLCharacters(request.getContextPath()) %>/search.jsp>
          <font size=-1><input type=text name=q size=31 maxlength=2048 value="<%= Text.encodeIllegalXMLCharacters(q) %>" title="Search"> <input type=submit value="Search">
          </font></form>
        <br><font size=-1>
        <a href="<%= Text.encodeIllegalXMLCharacters(request.getContextPath()) %>/swr.jsp?q=<%= Text.encodeIllegalXMLCharacters(q) %>&swrnum=<%= rows.getSize() %>">Search&nbsp;within&nbsp;results</a> | <a href="http://issues.apache.org/jira/browse/JCR" target=_blank>Dissatisfied? Help us improve</a></font><br>
        <br>
      </td></tr>
    </table>

  <%
    } // if (rows != null)

    String tableClass = "";
    if (rows != null && rows.getSize() == 0) {
        tableClass = " class=\"t n bt\"";
    }
  %>
<jsp:include page="footer.jsp"/>
<%
    } finally {
        if (jcrSession != null) {
            jcrSession.logout();
        }
    }
%>