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
import org.apache.jackrabbit.oak.remote.RemoteQueryParseException;
import org.apache.jackrabbit.oak.remote.RemoteResult;
import org.apache.jackrabbit.oak.remote.RemoteResults;
import org.apache.jackrabbit.oak.remote.RemoteRevision;
import org.apache.jackrabbit.oak.remote.RemoteSession;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import static org.apache.jackrabbit.oak.remote.http.handler.RemoteValues.renderJson;
import static org.apache.jackrabbit.oak.remote.http.handler.RemoteValues.renderJsonOrNull;
import static org.apache.jackrabbit.oak.remote.http.handler.ResponseUtils.sendBadRequest;
import static org.apache.jackrabbit.oak.remote.http.handler.ResponseUtils.sendGone;
import static org.apache.jackrabbit.oak.remote.http.handler.ResponseUtils.sendInternalServerError;

abstract class SearchRevisionHandler implements Handler {

    protected abstract RemoteRevision readRevision(HttpServletRequest request, RemoteSession session);

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
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

        String query = readQuery(request);

        if (query == null) {
            sendBadRequest(response, "query not specified");
            return;
        }

        String language = readLanguage(request);

        if (language == null) {
            sendBadRequest(response, "language not specified");
            return;
        }

        Long offset = readOffset(request);

        if (offset == null) {
            sendBadRequest(response, "offset not specified");
            return;
        }

        Long limit = readLimit(request);

        if (limit == null) {
            sendBadRequest(response, "limit not specified");
            return;
        }

        RemoteResults results;

        try {
            results = session.search(revision, query, language, offset, limit);
        } catch (RemoteQueryParseException e) {
            sendBadRequest(response, "malformed query");
            return;
        }

        response.setStatus(HttpServletResponse.SC_OK);
        response.setHeader("Oak-Revision", revision.asString());
        response.setContentType("application/json");

        ServletOutputStream stream = response.getOutputStream();

        JsonGenerator generator = new JsonFactory().createJsonGenerator(stream, JsonEncoding.UTF8);
        renderResponse(generator, results);
        generator.flush();

        stream.close();
    }

    private String readQuery(HttpServletRequest request) {
        return readStringParameter(request, "query");
    }

    private String readLanguage(HttpServletRequest request) {
        return readStringParameter(request, "language");
    }

    private String readStringParameter(HttpServletRequest request, String name) {
        return request.getParameter(name);
    }

    private Long readOffset(HttpServletRequest request) {
        return readLongParameter(request, "offset");
    }

    private Long readLimit(HttpServletRequest request) {
        return readLongParameter(request, "limit");
    }

    private Long readLongParameter(HttpServletRequest request, String name) {
        String value = readStringParameter(request, name);

        if (value == null) {
            return null;
        }

        try {
            return Long.parseLong(value, 10);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private void renderResponse(JsonGenerator generator, RemoteResults results) throws IOException {
        generator.writeStartObject();
        generator.writeFieldName("total");
        generator.writeNumber(results.getTotal());
        generator.writeFieldName("columns");
        renderStrings(generator, results.getColumns());
        generator.writeFieldName("selectors");
        renderStrings(generator, results.getSelectors());
        generator.writeFieldName("results");
        renderResults(generator, results);
        generator.writeEndObject();
    }

    private void renderStrings(JsonGenerator generator, Iterable<String> elements) throws IOException {
        generator.writeStartArray();

        for (String element : elements) {
            generator.writeString(element);
        }

        generator.writeEndArray();
    }

    private void renderResults(JsonGenerator generator, RemoteResults results) throws IOException {
        generator.writeStartArray();

        for (RemoteResult result : results) {
            renderResult(generator, results, result);
        }

        generator.writeEndArray();
    }

    private void renderResult(JsonGenerator generator, RemoteResults results, RemoteResult result) throws IOException {
        generator.writeStartObject();
        generator.writeFieldName("columns");
        renderColumns(generator, results, result);
        generator.writeFieldName("selectors");
        renderSelectors(generator, results, result);
        generator.writeEndObject();
    }

    private void renderColumns(JsonGenerator generator, RemoteResults results, RemoteResult result) throws IOException {
        generator.writeStartObject();

        for (String name : results.getColumns()) {
            generator.writeFieldName(name);
            renderJsonOrNull(generator, result.getColumnValue(name));
        }

        generator.writeEndObject();
    }

    private void renderSelectors(JsonGenerator generator, RemoteResults results, RemoteResult result) throws IOException {
        generator.writeStartObject();

        for (String name : results.getSelectors()) {
            generator.writeFieldName(name);
            generator.writeString(result.getSelectorPath(name));
        }

        generator.writeEndObject();
    }

}
