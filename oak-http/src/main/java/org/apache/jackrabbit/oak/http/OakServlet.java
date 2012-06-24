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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.jcr.GuestCredentials;
import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.PropertyType;
import javax.security.auth.login.LoginException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;

public class OakServlet extends HttpServlet {

    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    private static final JsonFactory SMILE_FACTORY = new SmileFactory();

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
                Root root = session.getCurrentRoot();
                Tree tree = root.getTree(request.getPathInfo());
                if (tree != null) {
                    JsonGenerator generator = getRenderer(request, response);
                    int depth = getDepth(request);
                    render(tree, depth, generator);
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

    private int getDepth(HttpServletRequest request) {
        String d = request.getParameter("depth");
        if (d == null) {
            d = request.getParameter("d");
        }
        if (d != null) {
            try {
                return Integer.parseInt(d);
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        return 1;
    }

    private JsonGenerator getRenderer(
            HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        AcceptHeader accept = new AcceptHeader(request.getHeader("Accept"));
        String type = accept.resolve(
                "application/json",
                "application/x-jackson-smile");
        if ("application/x-jackson-smile".equals(type)) {
            response.setContentType("application/x-jackson-smile");
            return SMILE_FACTORY.createJsonGenerator(response.getOutputStream());
        } else {
            response.setContentType("application/json");
            return JSON_FACTORY.createJsonGenerator(response.getOutputStream());
        }
    }

    private void render(Tree tree, int depth, JsonGenerator generator)
            throws JsonGenerationException, IOException {
        generator.writeStartObject();
        if (depth > 0) {
            for (PropertyState property : tree.getProperties()) {
                render(property, generator);
            }
            for (Tree child : tree.getChildren()) {
                generator.writeFieldName(child.getName());
                render(child, depth - 1, generator);
            }
        }
        generator.writeEndObject();
        generator.close();
    }

    private void render(PropertyState property, JsonGenerator generator)
            throws JsonGenerationException, IOException {
        generator.writeFieldName(property.getName());
        if (property.isArray()) {
            generator.writeStartArray();
            for (CoreValue value : property.getValues()) {
                render(value, generator);
            }
            generator.writeEndArray();
        } else {
            render(property.getValue(), generator);
        }
    }

    private void render(CoreValue value, JsonGenerator generator)
            throws JsonGenerationException, IOException {
        // TODO: Type info?
        if (value.getType() == PropertyType.BOOLEAN) {
            generator.writeBoolean(value.getBoolean());
        } else if (value.getType() == PropertyType.DECIMAL) {
            generator.writeNumber(value.getDecimal());
        } else if (value.getType() == PropertyType.DOUBLE) {
            generator.writeNumber(value.getDouble());
        } else if (value.getType() == PropertyType.LONG) {
            generator.writeNumber(value.getLong());
        } else if (value.getType() == PropertyType.BINARY) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            InputStream stream = value.getNewStream();
            try {
                byte[] b = new byte[1024];
                int n = stream.read(b);
                while (n != -1) {
                    buffer.write(b, 0, n);
                    n = stream.read(b);
                }
            } finally {
                stream.close();
            }
            generator.writeBinary(buffer.toByteArray());
        } else {
            generator.writeString(value.getString());
        }
    }

}
