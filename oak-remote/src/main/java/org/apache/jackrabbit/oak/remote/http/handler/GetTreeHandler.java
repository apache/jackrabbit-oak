/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.remote.http.handler;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.remote.RemoteRevision;
import org.apache.jackrabbit.oak.remote.RemoteSession;
import org.apache.jackrabbit.oak.remote.RemoteTree;
import org.apache.jackrabbit.oak.remote.RemoteTreeFilters;
import org.apache.jackrabbit.oak.remote.RemoteValue;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonMap;
import static org.apache.jackrabbit.oak.remote.http.handler.RemoteValues.renderJson;
import static org.apache.jackrabbit.oak.remote.http.handler.ResponseUtils.sendGone;
import static org.apache.jackrabbit.oak.remote.http.handler.ResponseUtils.sendInternalServerError;
import static org.apache.jackrabbit.oak.remote.http.handler.ResponseUtils.sendNotFound;

abstract class GetTreeHandler implements Handler {

    protected abstract String readPath(HttpServletRequest request);

    protected abstract RemoteRevision readRevision(HttpServletRequest request, RemoteSession session);

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response) throws IOException {
        RemoteSession session = (RemoteSession) request.getAttribute("session");

        if (session == null) {
            sendInternalServerError(response, "session not found");
            return;
        }

        RemoteRevision revision = readRevision(request, session);

        if (revision == null) {
            sendGone(response, "unable to read the revision");
            return;
        }

        RemoteTree tree = session.readTree(revision, readPath(request), readFilters(request));

        if (tree == null) {
            sendNotFound(response, singletonMap("Oak-Revision", revision.asString()), "tree not found");
            return;
        }

        response.setStatus(HttpServletResponse.SC_OK);
        response.setHeader("Oak-Revision", revision.asString());
        response.setContentType("application/json");

        ServletOutputStream stream = response.getOutputStream();

        JsonGenerator generator = new JsonFactory().createJsonGenerator(stream, JsonEncoding.UTF8);
        renderResponse(generator, tree);
        generator.flush();

        stream.close();
    }

    private void renderResponse(JsonGenerator generator, RemoteTree tree) throws IOException {
        if (tree == null) {
            generator.writeNull();
        } else {
            generator.writeStartObject();
            generator.writeFieldName("properties");
            renderProperties(generator, tree.getProperties());
            generator.writeFieldName("children");
            renderChildren(generator, tree.getChildren());
            generator.writeFieldName("hasMoreChildren");
            generator.writeBoolean(tree.hasMoreChildren());
            generator.writeEndObject();
        }
    }

    private void renderChildren(JsonGenerator generator, Map<String, RemoteTree> children) throws IOException {
        generator.writeStartObject();

        for (Map.Entry<String, RemoteTree> entry : children.entrySet()) {
            generator.writeFieldName(entry.getKey());
            renderResponse(generator, entry.getValue());
        }

        generator.writeEndObject();
    }

    private void renderProperties(JsonGenerator generator, Map<String, RemoteValue> properties) throws IOException {
        generator.writeStartObject();

        for (Map.Entry<String, RemoteValue> entry : properties.entrySet()) {
            generator.writeFieldName(entry.getKey());
            renderJson(generator, entry.getValue());
        }

        generator.writeEndObject();
    }

    private RemoteTreeFilters readFilters(final HttpServletRequest request) {
        return new RemoteTreeFilters() {

            @Override
            public int getDepth() {
                Integer depth = readDepth(request);

                if (depth == null) {
                    return super.getDepth();
                }

                return depth;
            }

            @Override
            public Set<String> getPropertyFilters() {
                Set<String> propertyFilters = readPropertyFilters(request);

                if (propertyFilters == null) {
                    return super.getPropertyFilters();
                }

                return propertyFilters;
            }

            @Override
            public Set<String> getNodeFilters() {
                Set<String> nodeFilters = readNodeFilters(request);

                if (nodeFilters == null) {
                    return super.getNodeFilters();
                }

                return nodeFilters;
            }

            @Override
            public long getBinaryThreshold() {
                Long binaryThreshold = readBinaryThreshold(request);

                if (binaryThreshold == null) {
                    return super.getBinaryThreshold();
                }

                return binaryThreshold;
            }

            @Override
            public int getChildrenStart() {
                Integer childrenStart = readChildrenStart(request);

                if (childrenStart == null) {
                    return super.getChildrenStart();
                }

                return childrenStart;
            }

            @Override
            public int getChildrenCount() {
                Integer childrenCount = readChildrenCount(request);

                if (childrenCount == null) {
                    return super.getChildrenCount();
                }

                return childrenCount;
            }

        };
    }

    private Integer readDepth(HttpServletRequest request) {
        return readIntegerParameter(request, "depth");
    }

    private Set<String> readPropertyFilters(HttpServletRequest request) {
        return readSetParameter(request, "properties");
    }

    private Set<String> readNodeFilters(HttpServletRequest request) {
        return readSetParameter(request, "children");
    }

    private Long readBinaryThreshold(HttpServletRequest request) {
        return readLongParameter(request, "binaries");
    }

    private Integer readChildrenStart(HttpServletRequest request) {
        return readIntegerParameter(request, "childrenStart");
    }

    private Integer readChildrenCount(HttpServletRequest request) {
        return readIntegerParameter(request, "childrenCount");
    }

    private Integer readIntegerParameter(HttpServletRequest request, String name) {
        String value = request.getParameter(name);

        if (value == null) {
            return null;
        }

        Integer result;

        try {
            result = Integer.parseInt(value, 10);
        } catch (NumberFormatException e) {
            result = null;
        }

        return result;
    }

    private Long readLongParameter(HttpServletRequest request, String name) {
        String value = request.getParameter(name);

        if (value == null) {
            return null;
        }

        Long result;

        try {
            result = Long.parseLong(value, 10);
        } catch (NumberFormatException e) {
            result = null;
        }

        return result;
    }

    private Set<String> readSetParameter(HttpServletRequest request, String name) {
        String[] values = request.getParameterValues(name);

        if (values == null) {
            return null;
        }

        return Sets.newHashSet(values);
    }

}
