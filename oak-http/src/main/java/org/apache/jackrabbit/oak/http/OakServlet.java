/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.http;

import java.io.IOException;
import java.io.PrintWriter;

import javax.jcr.GuestCredentials;
import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;

public class OakServlet extends HttpServlet {

    private final ContentRepository repository;

    public OakServlet(ContentRepository repository) {
        this.repository = repository;
    }

    @Override
    protected void service(
            HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        try {
            ContentSession session = repository.login(new GuestCredentials(), null);
            try {
                response.setContentType("text/plain");
                PrintWriter writer = response.getWriter();
                Tree tree =
                        session.getCurrentRoot().getTree(request.getPathInfo());
                if (tree != null) {
                    for (PropertyState property : tree.getProperties()) {
                        writer.print("- " + property.getName() + "\n");
                    }
                    for (Tree child : tree.getChildren()) {
                        writer.print("+ " + child.getName() + "\n");
                    }
                } else {
                    response.sendError(HttpServletResponse.SC_NOT_FOUND);
                }
            } finally {
                session.close();
            }
        } catch (NoSuchWorkspaceException e) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
        } catch (LoginException e) {
            throw new ServletException(e);
        }
    }

}
