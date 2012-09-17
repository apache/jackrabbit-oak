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
import java.util.Iterator;
import java.util.Map.Entry;

import javax.jcr.GuestCredentials;
import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.DefaultConflictHandler;
import org.apache.jackrabbit.oak.plugins.memory.MemoryValueFactory;
import org.apache.tika.mime.MediaType;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;

public class OakServlet extends HttpServlet {

    private static final MediaType JSON =
            MediaType.parse("application/json");

    private static final MediaType SMILE =
            MediaType.parse("application/x-jackson-smile");

    private static final Representation[] REPRESENTATIONS = {
        new JsonRepresentation(JSON, new JsonFactory()),
        new JsonRepresentation(SMILE, new SmileFactory()),
        new PostRepresentation(),
        new TextRepresentation() };

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
                Root root = session.getLatestRoot();
                Tree tree = root.getTree(request.getPathInfo());
                request.setAttribute("root", root);
                request.setAttribute("tree", tree);
                super.service(request, response);
            } finally {
                session.close();
            }
        } catch (NoSuchWorkspaceException e) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
        } catch (LoginException e) {
            throw new ServletException(e);
        }
    }

    @Override
    protected void doGet(
            HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        AcceptHeader accept = new AcceptHeader(request.getHeader("Accept"));
        Representation representation = accept.resolve(REPRESENTATIONS);

        Tree tree = (Tree) request.getAttribute("tree");
        representation.render(tree, response);
    }

    @Override
    protected void doPost(
            HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        try {
            Root root = (Root) request.getAttribute("root");
            Tree tree = (Tree) request.getAttribute("tree");
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(request.getInputStream());
            if (node.isObject()) {
                post(node, tree);
                root.commit(DefaultConflictHandler.OURS);
                doGet(request, response);
            } else {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST);
            }
        } catch (CommitFailedException e) {
            throw new ServletException(e);
        }
    }

    private static void post(JsonNode node, Tree tree) {
        Iterator<Entry<String, JsonNode>> iterator = node.fields();
        while (iterator.hasNext()) {
            Entry<String, JsonNode> entry = iterator.next();
            String name = entry.getKey();
            JsonNode value = entry.getValue();
            if (value.isObject()) {
                if (tree.hasProperty(name)) {
                    tree.removeProperty(name);
                }
                Tree child = tree.getChild(name);
                if (child == null) {
                    child = tree.addChild(name);
                }
                post(value, child);
            } else {
                Tree child = tree.getChild(name);
                if (child != null) {
                    child.remove();
                }
                CoreValueFactory vf = MemoryValueFactory.INSTANCE;
                if (value.isNull()) {
                    tree.removeProperty(name);
                } else if (value.isBoolean()) {
                    tree.setProperty(name, vf.createValue(value.asBoolean()));
                } else if (value.isLong()) {
                    tree.setProperty(name, vf.createValue(value.asLong()));
                } else if (value.isDouble()) {
                    tree.setProperty(name, vf.createValue(value.asDouble()));
                } else if (value.isBigDecimal()) {
                    tree.setProperty(name, vf.createValue(value.decimalValue()));
                } else {
                    tree.setProperty(name, vf.createValue(value.asText()));
                }
            }
        }
    }

    @Override
    protected void doDelete(
            HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        try {
            Root root = (Root) request.getAttribute("root");
            Tree tree = (Tree) request.getAttribute("tree");
            Tree parent = tree.getParent();
            if (parent != null) {
                Tree child = parent.getChild(tree.getName());
                if (child != null) {
                    child.remove();
                }
                root.commit(DefaultConflictHandler.OURS);
                response.sendError(HttpServletResponse.SC_OK);
            } else {
                // Can't remove the root node
                response.sendError(HttpServletResponse.SC_FORBIDDEN);
            }
        } catch (CommitFailedException e) {
            throw new ServletException(e);
        }
    }

}
