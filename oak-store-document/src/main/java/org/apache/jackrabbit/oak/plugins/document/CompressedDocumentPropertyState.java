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
package org.apache.jackrabbit.oak.plugins.document;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.commons.properties.SystemPropertySupplier;
import org.apache.jackrabbit.oak.plugins.memory.AbstractPropertyState;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PropertyState compression implementation with lazy parsing of the JSOP encoded value.
 */
public final class CompressedDocumentPropertyState implements PropertyState {

    private static final Logger LOG = LoggerFactory.getLogger(CompressedDocumentPropertyState.class);

    private final DocumentNodeStore store;

    private final String name;

    private PropertyState parsed;
    private final byte[] compressedValue;
    private final Compression compression;

    private static int COMPRESSION_THRESHOLD = SystemPropertySupplier
            .create("oak.documentMK.stringCompressionThreshold ", -1).loggingTo(LOG).get();

    CompressedDocumentPropertyState(DocumentNodeStore store, String name, String value, Compression compression) {
        this.store = store;
        this.name = name;
        this.compression = compression;
        try {
            this.compressedValue = compress(value.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            LOG.warn("Failed to compress property {} value: ", name, e);
            throw new IllegalArgumentException("Failed to compress value", e);
        }
    }

    private byte[] compress(byte[] value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputStream compressionOutputStream = compression.getOutputStream(out);
        compressionOutputStream.write(value);
        compressionOutputStream.close();
        return out.toByteArray();
    }

    @NotNull
    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isArray() {
        return parsed().isArray();
    }

    @Override
    public Type<?> getType() {
        return parsed().getType();
    }

    @NotNull
    @Override
    public <T> T getValue(Type<T> type) {
        return parsed().getValue(type);
    }

    @NotNull
    @Override
    public <T> T getValue(Type<T> type, int index) {
        return parsed().getValue(type, index);
    }

    @Override
    public long size() {
        return parsed().size();
    }

    @Override
    public long size(int index) {
        long size;
        PropertyState parsed = parsed();
        if (parsed.getType() == Type.BINARIES) {
            size = parsed.getValue(Type.BINARY, index).length();
        } else {
            size = parsed.size(index);
        }
        return size;
    }

    @Override
    public int count() {
        return parsed().count();
    }

    /**
     * Returns the raw un-parsed value as passed to the constructor of this
     * property state.
     *
     * @return the raw un-parsed value.
     */
    @NotNull
    String getValue() {
        return decompress(this.compressedValue);
    }

    private String decompress(byte[] value) {
        try {
            return new String(compression.getInputStream(new ByteArrayInputStream(value)).readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            LOG.error("Failed to decompress property {} value: ", getName(), e);
            return "\"{}\"";
        }
    }

    byte[] getCompressedValue() {
        return compressedValue;
    }

    //------------------------------------------------------------< Object >--

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        } else if (object instanceof CompressedDocumentPropertyState) {
            CompressedDocumentPropertyState other = (CompressedDocumentPropertyState) object;
            if (!this.name.equals(other.name) || !Arrays.equals(this.compressedValue, other.compressedValue)) {
                return false;
            }
            if (this.compressedValue == null && other.compressedValue == null) {
                return getValue().equals(other.getValue());
            } else {
                // Compare length and content of compressed values
                if (this.compressedValue.length != other.compressedValue.length) {
                    return false;
                }
                return Arrays.equals(this.compressedValue, other.compressedValue);
            }
        }
        // fall back to default equality check in AbstractPropertyState
        return object instanceof PropertyState
                && AbstractPropertyState.equal(parsed(), (PropertyState) object);
    }

    @Override
    public int hashCode() {
        return AbstractPropertyState.hashCode(this);
    }

    @Override
    public String toString() {
        return AbstractPropertyState.toString(this);
    }

    static int getCompressionThreshold() {
        return COMPRESSION_THRESHOLD;
    }

    static void setCompressionThreshold(int compressionThreshold) {
        COMPRESSION_THRESHOLD = compressionThreshold;
    }

    //----------------------------< internal >----------------------------------

    private PropertyState parsed() {
        if (parsed == null) {
            JsopReader reader = new JsopTokenizer(getValue());
            if (reader.matches('[')) {
                parsed = readArrayProperty(name, reader);
            } else {
                parsed = readProperty(name, reader);
            }
        }
        return parsed;
    }

    /**
     * Read a {@code PropertyState} from a {@link JsopReader}
     * @param name  The name of the property state
     * @param reader  The reader
     * @return new property state
     */
    PropertyState readProperty(String name, JsopReader reader) {
        return DocumentPropertyState.readProperty(name, store, reader);
    }

    /**
     * Read a multi valued {@code PropertyState} from a {@link JsopReader}.
     *
     * @param name the name of the property state
     * @param reader the reader
     * @return new property state
     */
    PropertyState readArrayProperty(String name, JsopReader reader) {
        return DocumentPropertyState.readArrayProperty(name, store, reader);
    }
}