/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.exporter;

import java.io.Closeable;
import java.io.IOException;

import com.google.gson.stream.JsonWriter;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;

/**
 * A streaming JsopWriter which uses gson JsonWriter
 */
class JsopStreamWriter implements JsopWriter, Closeable {
    private final JsonWriter w;

    public JsopStreamWriter(JsonWriter w) {
        this.w = w;
    }

    @Override
    public JsopWriter array() {
        try {
            w.beginArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    @Override
    public JsopWriter object() {
        try {
            w.beginObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    @Override
    public JsopWriter key(String key) {
        try {
            w.name(key);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    @Override
    public JsopWriter value(String value) {
        try {
            w.value(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    @Override
    public JsopWriter encodedValue(String raw) {
        try {
            w.jsonValue(raw);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    @Override
    public JsopWriter endObject() {
        try {
            w.endObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    @Override
    public JsopWriter endArray() {
        try {
            w.endArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    @Override
    public JsopWriter value(long x) {
        try {
            w.value(x);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    @Override
    public JsopWriter value(boolean b) {
        try {
            w.value(b);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    //Unsupported operation. These are also not used by JsonSerializer

    @Override
    public JsopWriter tag(char tag) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsopWriter append(JsopWriter diff) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsopWriter newline() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resetWriter() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLineLength(int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        w.close();
    }
}
