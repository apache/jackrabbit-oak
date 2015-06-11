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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jackrabbit.oak.remote.RemoteCommitException;
import org.apache.jackrabbit.oak.remote.RemoteOperation;
import org.apache.jackrabbit.oak.remote.RemoteRevision;
import org.apache.jackrabbit.oak.remote.RemoteSession;
import org.apache.jackrabbit.oak.remote.RemoteValue;
import org.apache.jackrabbit.oak.remote.RemoteValue.Supplier;
import org.apache.jackrabbit.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.jackrabbit.oak.remote.http.handler.ResponseUtils.sendBadRequest;
import static org.apache.jackrabbit.oak.remote.http.handler.ResponseUtils.sendGone;
import static org.apache.jackrabbit.oak.remote.http.handler.ResponseUtils.sendInternalServerError;

abstract class PatchRevisionHandler implements Handler {

    private static final Logger logger = LoggerFactory.getLogger(PatchRevisionHandler.class);

    protected abstract RemoteRevision readRevision(HttpServletRequest request, RemoteSession session);

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response) throws IOException {
        RemoteSession session = (RemoteSession) request.getAttribute("session");

        if (session == null) {
            sendInternalServerError(response, "session not found");
            return;
        }

        RemoteRevision base = readRevision(request, session);

        if (base == null) {
            sendGone(response, "revision not found");
            return;
        }

        RemoteOperation operation;

        try {
            operation = parseOperations(session, new ObjectMapper().readTree(request.getInputStream()));
        } catch (Exception e) {
            operation = null;
        }

        if (operation == null) {
            sendBadRequest(response, "unable to parse the list of operations");
            return;
        }

        RemoteRevision revision;

        try {
            revision = session.commit(base, operation);
        } catch (RemoteCommitException e) {
            logger.warn("unable to perform the commit", e);
            sendBadRequest(response, "commit failed");
            return;
        }

        response.setStatus(HttpServletResponse.SC_CREATED);
        response.setContentType("application/json");

        ServletOutputStream stream = response.getOutputStream();

        JsonGenerator generator = new JsonFactory().createJsonGenerator(stream, JsonEncoding.UTF8);
        renderResponse(generator, revision);
        generator.flush();

        stream.close();
    }

    private void renderResponse(JsonGenerator generator, RemoteRevision revision) throws IOException {
        generator.writeStartObject();
        generator.writeStringField("revision", revision.asString());
        generator.writeEndObject();
    }

    private RemoteOperation parseOperations(RemoteSession session, JsonNode json) {
        List<RemoteOperation> operations = new ArrayList<RemoteOperation>();

        for (JsonNode child : json) {
            operations.add(parseOperation(session, child));
        }

        return session.createAggregateOperation(operations);
    }

    private RemoteOperation parseOperation(RemoteSession session, JsonNode json) {
        String type = parseStringField(json, "op");

        if (type.equals("add")) {
            return parseAddOperation(session, json);
        }

        if (type.equals("remove")) {
            return parseRemoveOperation(session, json);
        }

        if (type.equals("set")) {
            return parseSetOperation(session, json);
        }

        if (type.equals("unset")) {
            return parseUnsetOperation(session, json);
        }

        if (type.equals("copy")) {
            return parseCopyOperation(session, json);
        }

        if (type.equals("move")) {
            return parseMoveOperation(session, json);
        }

        throw new IllegalArgumentException("invalid operation type");
    }

    private RemoteOperation parseMoveOperation(RemoteSession session, JsonNode node) {
        return session.createMoveOperation(parseStringField(node, "from"), parseStringField(node, "to"));
    }

    private RemoteOperation parseCopyOperation(RemoteSession session, JsonNode node) {
        return session.createCopyOperation(parseStringField(node, "from"), parseStringField(node, "to"));
    }

    private RemoteOperation parseUnsetOperation(RemoteSession session, JsonNode node) {
        return session.createUnsetOperation(parseStringField(node, "path"), parseStringField(node, "name"));
    }

    private RemoteOperation parseSetOperation(RemoteSession session, JsonNode node) {
        return session.createSetOperation(parseStringField(node, "path"), parseStringField(node, "name"), parseValue(node));
    }

    private RemoteOperation parseRemoveOperation(RemoteSession session, JsonNode node) {
        return session.createRemoveOperation(parseStringField(node, "path"));
    }

    private RemoteOperation parseAddOperation(RemoteSession session, JsonNode json) {
        return session.createAddOperation(parseStringField(json, "path"), parsePropertiesField(json, "properties"));
    }

    private Map<String, RemoteValue> parsePropertiesField(JsonNode node, String name) {
        return parseProperties(node.get(name));
    }

    private Map<String, RemoteValue> parseProperties(JsonNode node) {
        Map<String, RemoteValue> values = new HashMap<String, RemoteValue>();

        Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();

        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = iterator.next();
            values.put(entry.getKey(), parseValue(entry.getValue()));
        }

        return values;
    }

    private RemoteValue parseValue(JsonNode node) {
        String type = parseStringField(node, "type");

        if (type.equals("string")) {
            return RemoteValue.toText(parseStringField(node, "value"));
        }

        if (type.equals("strings")) {
            return RemoteValue.toMultiText(parseStringArrayField(node, "value"));
        }

        if (type.equals("binary")) {
            return RemoteValue.toBinary(parseBinaryField(node, "value"));
        }

        if (type.equals("binaries")) {
            return RemoteValue.toMultiBinary(parseBinaryArrayField(node, "value"));
        }

        if (type.equals("binaryId")) {
            return RemoteValue.toBinaryId(parseStringField(node, "value"));
        }

        if (type.equals("binaryIds")) {
            return RemoteValue.toMultiBinaryId(parseStringArrayField(node, "value"));
        }

        if (type.equals("long")) {
            return RemoteValue.toLong(parseLongField(node, "value"));
        }

        if (type.equals("longs")) {
            return RemoteValue.toMultiLong(parseLongArrayField(node, "value"));
        }

        if (type.equals("double")) {
            return RemoteValue.toDouble(parseDoubleField(node, "value"));
        }

        if (type.equals("doubles")) {
            return RemoteValue.toMultiDouble(parseDoubleArrayField(node, "value"));
        }

        if (type.equals("date")) {
            return RemoteValue.toDate(parseLongField(node, "value"));
        }

        if (type.equals("dates")) {
            return RemoteValue.toMultiDate(parseLongArrayField(node, "value"));
        }

        if (type.equals("boolean")) {
            return RemoteValue.toBoolean(parseBooleanField(node, "value"));
        }

        if (type.equals("booleans")) {
            return RemoteValue.toMultiBoolean(parseBooleanArrayField(node, "value"));
        }

        if (type.equals("name")) {
            return RemoteValue.toName(parseStringField(node, "value"));
        }

        if (type.equals("names")) {
            return RemoteValue.toMultiName(parseStringArrayField(node, "value"));
        }

        if (type.equals("path")) {
            return RemoteValue.toPath(parseStringField(node, "value"));
        }

        if (type.equals("paths")) {
            return RemoteValue.toMultiPath(parseStringArrayField(node, "value"));
        }

        if (type.equals("reference")) {
            return RemoteValue.toReference(parseStringField(node, "value"));
        }

        if (type.equals("references")) {
            return RemoteValue.toMultiReference(parseStringArrayField(node, "value"));
        }

        if (type.equals("weakReference")) {
            return RemoteValue.toWeakReference(parseStringField(node, "value"));
        }

        if (type.equals("weakReferences")) {
            return RemoteValue.toMultiWeakReference(parseStringArrayField(node, "value"));
        }

        if (type.equals("uri")) {
            return RemoteValue.toUri(parseStringField(node, "value"));
        }

        if (type.equals("uris")) {
            return RemoteValue.toMultiUri(parseStringArrayField(node, "value"));
        }

        if (type.equals("decimal")) {
            return RemoteValue.toDecimal(parseDecimalField(node, "value"));
        }

        if (type.equals("decimals")) {
            return RemoteValue.toMultiDecimal(parseDecimalArrayField(node, "value"));
        }

        throw new IllegalArgumentException("invalid value type");
    }

    private BigDecimal parseDecimalField(JsonNode node, String field) {
        return parseDecimal(node.get(field));
    }

    private BigDecimal parseDecimal(JsonNode node) {
        return new BigDecimal(node.asText());
    }

    private Iterable<BigDecimal> parseDecimalArrayField(JsonNode node, String field) {
        return parseDecimalArray(node.get(field));
    }

    private Iterable<BigDecimal> parseDecimalArray(JsonNode node) {
        List<BigDecimal> result = new ArrayList<BigDecimal>();

        for (JsonNode element : node) {
            result.add(parseDecimal(element));
        }

        return result;
    }

    private boolean parseBooleanField(JsonNode node, String field) {
        return parseBoolean(node.get(field));
    }

    private boolean parseBoolean(JsonNode node) {
        return node.asBoolean();
    }

    private Iterable<Boolean> parseBooleanArrayField(JsonNode node, String field) {
        return parseBooleanArray(node.get(field));
    }

    private Iterable<Boolean> parseBooleanArray(JsonNode node) {
        List<Boolean> result = new ArrayList<Boolean>();

        for (JsonNode element : node) {
            result.add(parseBoolean(element));
        }

        return result;
    }

    private double parseDoubleField(JsonNode node, String field) {
        return parseDouble(node.get(field));
    }

    private double parseDouble(JsonNode node) {
        return node.asDouble();
    }

    private Iterable<Double> parseDoubleArrayField(JsonNode node, String field) {
        return parseDoubleArray(node.get(field));
    }

    private Iterable<Double> parseDoubleArray(JsonNode node) {
        List<Double> result = new ArrayList<Double>();

        for (JsonNode element : node) {
            result.add(parseDouble(element));
        }

        return result;
    }

    private long parseLongField(JsonNode node, String field) {
        return parseLong(node.get(field));
    }

    private long parseLong(JsonNode node) {
        return node.asLong();
    }

    private Iterable<Long> parseLongArrayField(JsonNode node, String field) {
        return parseLongArray(node.get(field));
    }

    private Iterable<Long> parseLongArray(JsonNode node) {
        List<Long> result = new ArrayList<Long>();

        for (JsonNode element : node) {
            result.add(parseLong(element));
        }

        return result;
    }

    private Supplier<InputStream> parseBinaryField(JsonNode node, String field) {
        return parseBinary(node.get(field));
    }

    private Supplier<InputStream> parseBinary(final JsonNode node) {
        return new Supplier<InputStream>() {

            @Override
            public InputStream get() {
                return new ByteArrayInputStream(Base64.decode(node.asText()).getBytes());
            }

        };
    }

    private Iterable<Supplier<InputStream>> parseBinaryArrayField(JsonNode node, String field) {
        return parseBinaryArray(node.get(field));
    }

    private Iterable<Supplier<InputStream>> parseBinaryArray(JsonNode node) {
        List<Supplier<InputStream>> result = new ArrayList<Supplier<InputStream>>();

        for (JsonNode element : node) {
            result.add(parseBinary(element));
        }

        return result;
    }

    private Iterable<String> parseStringArrayField(JsonNode node, String field) {
        return parseStringArray(node.get(field));
    }

    private String parseStringField(JsonNode node, String field) {
        return parseString(node.get(field));
    }

    private Iterable<String> parseStringArray(JsonNode node) {
        List<String> result = new ArrayList<String>();

        for (JsonNode element : node) {
            result.add(parseString(element));
        }

        return result;
    }

    private String parseString(JsonNode node) {
        return node.asText();
    }

}
