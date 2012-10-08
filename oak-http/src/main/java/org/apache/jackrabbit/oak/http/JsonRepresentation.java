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
import java.math.BigDecimal;

import javax.jcr.PropertyType;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.tika.mime.MediaType;

import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BOOLEANS;
import static org.apache.jackrabbit.oak.api.Type.DECIMALS;
import static org.apache.jackrabbit.oak.api.Type.DOUBLES;
import static org.apache.jackrabbit.oak.api.Type.LONGS;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

class JsonRepresentation implements Representation {

    private final MediaType type;

    private final JsonFactory factory;

    public JsonRepresentation(MediaType type, JsonFactory factory) {
        this.type = type;
        this.factory = factory;
    }

    @Override
    public MediaType getType() {
        return type;
    }

    @Override
    public void render(Tree tree, HttpServletResponse response)
            throws IOException {
        JsonGenerator generator = startResponse(response);
        render(tree, generator);
        generator.close();
    }

    @Override
    public void render(PropertyState property, HttpServletResponse response)
            throws IOException {
        JsonGenerator generator = startResponse(response);
        render(property, generator);
        generator.close();
    }

    protected JsonGenerator startResponse(HttpServletResponse response)
            throws IOException {
        response.setContentType(type.toString());
        return factory.createJsonGenerator(response.getOutputStream());
    }

    private static void render(Tree tree, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        for (PropertyState property : tree.getProperties()) {
            generator.writeFieldName(property.getName());
            render(property, generator);
        }
        for (Tree child : tree.getChildren()) {
            generator.writeFieldName(child.getName());
            generator.writeStartObject();
            generator.writeEndObject();
        }
        generator.writeEndObject();
    }

    private static void render(PropertyState property, JsonGenerator generator)
            throws IOException {
        if (property.isArray()) {
            generator.writeStartArray();
            renderValue(property, generator);
            generator.writeEndArray();
        } else {
            renderValue(property, generator);
        }
    }

    private static void renderValue(PropertyState property, JsonGenerator generator)
            throws IOException {
        // TODO: Type info?
        int type = property.getType().tag();
        if (type == PropertyType.BOOLEAN) {
            for (boolean value : property.getValue(BOOLEANS)) {
                generator.writeBoolean(value);
            }
        } else if (type == PropertyType.DECIMAL) {
            for (BigDecimal value : property.getValue(DECIMALS)) {
                generator.writeNumber(value);
            }
        } else if (type == PropertyType.DOUBLE) {
            for (double value : property.getValue(DOUBLES)) {
                generator.writeNumber(value);
            }
        } else if (type == PropertyType.LONG) {
            for (long value : property.getValue(LONGS)) {
                generator.writeNumber(value);
            }
        } else if (type == PropertyType.BINARY) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            for (Blob value : property.getValue(BINARIES)) {
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
            }
        } else {
            for (String value : property.getValue(STRINGS)) {
                generator.writeString(value);
            }
        }
    }

}
