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

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import org.apache.jackrabbit.oak.remote.RemoteValue;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;

class RemoteValues {

    private RemoteValues() {
        // Prevent instantiation
    }

    public static void renderJsonOrNull(JsonGenerator generator, RemoteValue value) throws IOException {
        if (value == null) {
            generator.writeNull();
        } else {
            renderJson(generator, value);
        }
    }

    public static void renderJson(JsonGenerator generator, RemoteValue value) throws IOException {
        if (value.isBinary()) {
            renderValue(generator, "binary", value.asBinary(), getBinaryWriter());
        }

        if (value.isMultiBinary()) {
            renderMultiValue(generator, "binaries", value.asMultiBinary(), getBinaryWriter());
        }

        if (value.isBinaryId()) {
            renderValue(generator, "binaryId", value.asBinaryId(), getStringWriter());
        }

        if (value.isMultiBinaryId()) {
            renderMultiValue(generator, "binaryIds", value.asMultiBinaryId(), getStringWriter());
        }

        if (value.isBoolean()) {
            renderValue(generator, "boolean", value.asBoolean(), getBooleanWriter());
        }

        if (value.isMultiBoolean()) {
            renderMultiValue(generator, "booleans", value.asMultiBoolean(), getBooleanWriter());
        }

        if (value.isDate()) {
            renderValue(generator, "date", value.asDate(), getLongWriter());
        }

        if (value.isMultiDate()) {
            renderMultiValue(generator, "dates", value.asMultiDate(), getLongWriter());
        }

        if (value.isDecimal()) {
            renderValue(generator, "decimal", value.asDecimal(), getDecimalWriter());
        }

        if (value.isMultiDecimal()) {
            renderMultiValue(generator, "decimals", value.asMultiDecimal(), getDecimalWriter());
        }

        if (value.isDouble()) {
            renderValue(generator, "double", value.asDouble(), getDoubleWriter());
        }

        if (value.isMultiDouble()) {
            renderMultiValue(generator, "doubles", value.asMultiDouble(), getDoubleWriter());
        }

        if (value.isLong()) {
            renderValue(generator, "long", value.asLong(), getLongWriter());
        }

        if (value.isMultiLong()) {
            renderMultiValue(generator, "longs", value.asMultiLong(), getLongWriter());
        }

        if (value.isName()) {
            renderValue(generator, "name", value.asName(), getStringWriter());
        }

        if (value.isMultiName()) {
            renderMultiValue(generator, "names", value.asMultiName(), getStringWriter());
        }

        if (value.isPath()) {
            renderValue(generator, "path", value.asPath(), getStringWriter());
        }

        if (value.isMultiPath()) {
            renderMultiValue(generator, "paths", value.asMultiPath(), getStringWriter());
        }

        if (value.isReference()) {
            renderValue(generator, "reference", value.asReference(), getStringWriter());
        }

        if (value.isMultiReference()) {
            renderMultiValue(generator, "references", value.asMultiReference(), getStringWriter());
        }

        if (value.isText()) {
            renderValue(generator, "string", value.asText(), getStringWriter());
        }

        if (value.isMultiText()) {
            renderMultiValue(generator, "strings", value.asMultiText(), getStringWriter());
        }

        if (value.isUri()) {
            renderValue(generator, "uri", value.asUri(), getStringWriter());
        }

        if (value.isMultiUri()) {
            renderMultiValue(generator, "uris", value.asMultiUri(), getStringWriter());
        }

        if (value.isWeakReference()) {
            renderValue(generator, "weakReference", value.asWeakReference(), getStringWriter());
        }

        if (value.isMultiWeakReference()) {
            renderMultiValue(generator, "weakReferences", value.asMultiWeakReference(), getStringWriter());
        }
    }

    private static GeneratorWriter<RemoteValue.Supplier<InputStream>> getBinaryWriter() {
        return new GeneratorWriter<RemoteValue.Supplier<InputStream>>() {

            @Override
            public void write(JsonGenerator generator, RemoteValue.Supplier<InputStream> value) throws IOException {
                generator.writeString(BaseEncoding.base64().encode(ByteStreams.toByteArray(value.get())));
            }

        };
    }

    private static GeneratorWriter<String> getStringWriter() {
        return new GeneratorWriter<String>() {

            @Override
            public void write(JsonGenerator generator, String value) throws IOException {
                generator.writeString(value);
            }

        };
    }

    private static GeneratorWriter<Boolean> getBooleanWriter() {
        return new GeneratorWriter<Boolean>() {

            @Override
            public void write(JsonGenerator generator, Boolean value) throws IOException {
                generator.writeBoolean(value);
            }

        };
    }

    private static GeneratorWriter<Long> getLongWriter() {
        return new GeneratorWriter<Long>() {

            @Override
            public void write(JsonGenerator generator, Long value) throws IOException {
                generator.writeNumber(value);
            }

        };
    }

    private static GeneratorWriter<BigDecimal> getDecimalWriter() {
        return new GeneratorWriter<BigDecimal>() {

            @Override
            public void write(JsonGenerator generator, BigDecimal value) throws IOException {
                generator.writeString(value.toString());
            }

        };
    }

    private static GeneratorWriter<Double> getDoubleWriter() {
        return new GeneratorWriter<Double>() {

            @Override
            public void write(JsonGenerator generator, Double value) throws IOException {
                generator.writeNumber(value);
            }

        };
    }

    private static <T> void renderValue(JsonGenerator generator, String type, T value, GeneratorWriter<T> writer) throws IOException {
        generator.writeStartObject();
        generator.writeStringField("type", type);
        generator.writeFieldName("value");

        writer.write(generator, value);

        generator.writeEndObject();
    }

    private static <T> void renderMultiValue(JsonGenerator generator, String type, Iterable<T> values, GeneratorWriter<T> writer) throws IOException {
        generator.writeStartObject();
        generator.writeStringField("type", type);
        generator.writeArrayFieldStart("value");

        for (T value : values) {
            writer.write(generator, value);
        }

        generator.writeEndArray();
        generator.writeEndObject();
    }


    private interface GeneratorWriter<T> {

        void write(JsonGenerator generator, T value) throws IOException;

    }

}
